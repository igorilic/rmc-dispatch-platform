//! The map console (SPEC §9): plants, sites and trucks live on a map, and actions on them.
//!
//! The console keeps one copy of the world for every open page ([`state`]). Master data comes
//! from the seed compiled into every binary. Live state comes from the real stack ([`live`]):
//! truck telematics and batch tickets from the EVENTS stream, plan versions from dispatch's
//! `plans.updated`, the edge agent's `/status` over the plant's monitoring path, and the uplink's
//! state from Toxiproxy. A page gets a snapshot, then every change, as server-sent events
//! ([`http`]). Actions go through dispatch's API, the simulators' MQTT commands, the plant's sync
//! command and the plant's uplink ([`actions`]).
//!
//! Tracing: each action is a trace of its own, and each push continues the trace of the event
//! behind it. Dispatch continues the console's traces because the console calls it from inside
//! the cluster.

pub mod actions;
pub mod http;
pub mod live;
pub mod metrics;
pub mod state;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use domain::{PlantId, Seed};
use reqwest::{Method, StatusCode};
use rumqttc::{AsyncClient, EventLoop};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{Notify, broadcast, watch};
use tracing::Span;

use crate::state::{Features, Links, Snapshot, State};

/// How many changes a page may fall behind before it is dropped. Its browser then reconnects
/// and gets a new snapshot.
const BACKLOG: usize = 1024;

/// How long a call to dispatch, a webhook or the uplink may take.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the shutdown waits for the uplink to come back.
const SHUTDOWN_RESET: Duration = Duration::from_secs(5);

/// How long a poll of the edge agent may take. Short, so a cut-off plant shows as unreachable
/// within one poll instead of when a dead TCP connection finally times out.
const EDGE_TIMEOUT: Duration = Duration::from_millis(1500);

#[derive(Debug, thiserror::Error)]
pub enum ConsoleError {
    #[error("the seed: {0}")]
    Seed(#[from] serde_json::Error),
    #[error("HTTP client: {0}")]
    Http(#[from] reqwest::Error),
}

/// Where the console finds the rest of the stack.
#[derive(Debug, Clone)]
pub struct Settings {
    pub dispatch_url: String,
    /// The plant whose edge agent and uplink the console watches.
    pub plant: PlantId,
    /// The agent's `/status` over the plant's monitoring path; `None` hides the edge card.
    pub plant_agent_url: Option<String>,
    /// The plant's uplink, a Toxiproxy API; `None` turns Make chaos off.
    pub uplink_url: Option<String>,
    /// How long Make chaos cuts the uplink.
    pub cut: Duration,
    /// How often the edge agent and the uplink are read.
    pub poll: Duration,
    /// The address the browser uses: for the links to Jaeger, Grafana and the API, and for the
    /// hosts allowed to change anything.
    pub public_url: String,
    pub bauwerk: Webhook,
    pub neckar: Webhook,
}

/// A customer integration's webhook, where Make chaos places its orders.
#[derive(Debug, Clone)]
pub struct Webhook {
    pub url: String,
    pub token: String,
}

/// One change for the pages: an SSE event name and its JSON.
#[derive(Debug, Clone)]
pub struct Update {
    pub event: &'static str,
    pub data: Arc<str>,
}

impl Update {
    pub fn new(event: &'static str, data: &impl Serialize) -> Option<Update> {
        let data = serde_json::to_string(data).ok()?;
        Some(Update {
            event,
            data: data.into(),
        })
    }
}

/// What dispatch answered: its status, and its JSON (`null` when it sent none).
pub(crate) struct Reply {
    pub status: StatusCode,
    pub body: Value,
}

pub struct Console {
    pub settings: Settings,
    pub seed: Seed,
    state: Mutex<State>,
    updates: broadcast::Sender<Update>,
    http: reqwest::Client,
    edge_http: reqwest::Client,
    mqtt: AsyncClient,
    mqtt_up: AtomicBool,
    /// Breakdowns sent and not yet seen as `out_of_service`.
    pending: Mutex<HashMap<String, Instant>>,
    /// Asks for `/world` again: after a NATS reconnect, and after the EVENTS consumer had to
    /// start over.
    resync: Notify,
    /// Asks the EVENTS consumer to start over from each subject's last message: after a NATS
    /// reconnect, when the stream may have been created anew and its sequence reset.
    restart: Notify,
    /// Ends Make chaos's cut early. Woken with `notify_waiters`, which stores nothing, so a click
    /// outside a cut can't end the next run's cut.
    reconnect: Notify,
    /// Whether the uplink's startup reset went through, so Make chaos may start.
    uplink_ready: AtomicBool,
    /// Ends every page's stream at shutdown, so a graceful shutdown can finish.
    shutdown: watch::Sender<bool>,
    /// When the console started, on the world clock: the event lag leaves out the replay of
    /// each subject's last event at startup.
    started: DateTime<Utc>,
}

impl Console {
    /// The console, from the seed alone. [`Console::start`] connects it to the stack.
    pub fn new(settings: Settings, mqtt: AsyncClient) -> Result<Arc<Console>, ConsoleError> {
        let seed = Seed::rhine_neckar()?;
        let uplink = settings.uplink_url.is_some() && settings.plant_agent_url.is_some();
        let state = State::from_seed(&seed, uplink);
        Ok(Arc::new(Console {
            settings,
            seed,
            state: Mutex::new(state),
            updates: broadcast::channel(BACKLOG).0,
            http: reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()?,
            edge_http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(1))
                .timeout(EDGE_TIMEOUT)
                .build()?,
            mqtt,
            mqtt_up: AtomicBool::new(false),
            pending: Mutex::new(HashMap::new()),
            resync: Notify::new(),
            restart: Notify::new(),
            reconnect: Notify::new(),
            uplink_ready: AtomicBool::new(false),
            shutdown: watch::channel(false).0,
            started: domain::clock::now(),
        }))
    }

    /// Connects the console to the stack: follows EVENTS and `plans.updated`, reads `/world`
    /// now and after every NATS reconnect ([`Console::resync`]), polls the edge agent and the
    /// uplink, and drives the MQTT client.
    pub fn start(self: &Arc<Self>, nats: async_nats::Client, mqtt: EventLoop) {
        let js = async_nats::jetstream::new(nats.clone());
        tokio::spawn(live::follow_events(Arc::clone(self), js));
        tokio::spawn(live::follow_plans(Arc::clone(self), nats));
        tokio::spawn(live::resync(Arc::clone(self)));
        tokio::spawn(live::heartbeat(Arc::clone(self)));
        tokio::spawn(live::drive_mqtt(Arc::clone(self), mqtt));
        if let Some(url) = self.settings.plant_agent_url.clone() {
            tokio::spawn(live::watch_edge(Arc::clone(self), url));
        }
        if let Some(url) = self.settings.uplink_url.clone() {
            tokio::spawn(live::watch_uplink(Arc::clone(self), url));
        }
    }

    /// The snapshot and the changes after it. A change is either in the snapshot or among the
    /// changes, never in both and never in neither: changes are made and sent under the same lock.
    pub fn subscribe(&self) -> (Snapshot, broadcast::Receiver<Update>) {
        let state = self.state();
        let updates = self.updates.subscribe();
        (self.snapshot_of(&state), updates)
    }

    pub fn snapshot(&self) -> Snapshot {
        self.snapshot_of(&self.state())
    }

    /// What a NATS reconnect calls for: the EVENTS consumer starts over from each subject's
    /// last message, and `/world` is read again, for anything missed while the link was down.
    pub fn resync(&self) {
        self.restart.notify_one();
        self.resync.notify_one();
    }

    /// Ends every page's stream, so the server's graceful shutdown can finish, and brings back
    /// the uplink if Make chaos has it cut: a console that is going away mustn't leave the plant
    /// cut off.
    pub async fn shutdown(&self) {
        self.shutdown.send_replace(true);
        let cut = {
            let state = self.state();
            state.uplink.cut || state.uplink.run.as_ref().is_some_and(state::Run::active)
        };
        if cut && self.settings.uplink_url.is_some() {
            let reset = tokio::time::timeout(SHUTDOWN_RESET, self.uplink_reset()).await;
            if !matches!(reset, Ok(Ok(()))) {
                tracing::warn!("couldn't bring the uplink back at shutdown");
            }
        }
    }

    fn shutting_down(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }

    pub fn mqtt_up(&self) -> bool {
        self.mqtt_up.load(Ordering::Relaxed)
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn snapshot_of(&self, state: &State) -> Snapshot {
        let public = self.settings.public_url.trim_end_matches('/');
        let links = Links {
            jaeger: format!("{public}/jaeger"),
            grafana: format!("{public}/grafana"),
            api: format!("{public}/swagger-ui/"),
        };
        let features = Features {
            edge: self.settings.plant_agent_url.is_some(),
            chaos: state.uplink.available,
        };
        state.snapshot(
            domain::clock::now(),
            &self.settings.plant.0,
            features,
            links,
        )
    }

    /// Changes the state and pushes what changed as `event`, under one lock.
    fn change<T: Serialize>(
        &self,
        event: &'static str,
        change: impl FnOnce(&mut State) -> Option<T>,
    ) -> bool {
        let mut state = self.state();
        let Some(changed) = change(&mut state) else {
            return false;
        };
        if let Some(update) = Update::new(event, &changed) {
            metrics::pushed(event);
            let _ = self.updates.send(update);
        }
        true
    }

    /// Pushes something that isn't state, such as the heartbeat.
    fn push(&self, update: Option<Update>) {
        if let Some(update) = update {
            let _ = self.updates.send(update);
        }
    }

    /// Calls dispatch, carrying `span`'s trace.
    pub(crate) async fn dispatch(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        span: Option<&Span>,
    ) -> Result<Reply, String> {
        let url = format!("{}{path}", self.settings.dispatch_url.trim_end_matches('/'));
        let mut request = self.http.request(method, url);
        if let Some(traceparent) = span.and_then(telemetry::traceparent) {
            request = request.header("traceparent", traceparent);
        }
        if let Some(body) = body {
            request = request
                .header("content-type", "application/json")
                .body(body.to_string());
        }
        let response = request.send().await.map_err(|e| e.to_string())?;
        let status = response.status();
        let bytes = response.bytes().await.map_err(|e| e.to_string())?;
        Ok(Reply {
            status,
            body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        })
    }
}
