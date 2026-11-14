//! How the console follows the real stack. Each loop retries on its own, so the console starts
//! before the rest of the stack and rides out restarts.
//!
//! - EVENTS: an ordered consumer on `events.trucks.>` and `events.plants.>` that starts at each
//!   subject's last message, so every truck and plant has its latest event at once. Starting
//!   there also sets the consumer's sequence right away, so a reconnect resumes from it instead
//!   of replaying the whole stream. After a NATS reconnect it starts over, because the stream may
//!   have been created anew with its sequence reset; `/world` fills in what it missed.
//! - `plans.updated`: each announced version is pushed once, in the trace of the re-dispatch that
//!   made it, and only once dispatch confirms it exists.
//! - `/world`: read at startup, after every NATS reconnect and after each plan update. It brings
//!   plant statuses, which no event announces, and any plan version core NATS dropped. A version
//!   caught up this way has no trace to link; its own announcement, if it comes later, adds it.
//! - The edge agent's `/status` and the uplink's proxies, polled.
//! - A heartbeat with the world time, against which pages measure ages, and by which they notice
//!   a dead stream.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::jetstream::{self, consumer::DeliverPolicy, consumer::pull::OrderedConfig};
use chrono::{DateTime, Utc};
use domain::{Event, EventEnvelope, PLANS_UPDATED, PlanUpdated, TruckStatus};
use futures::StreamExt;
use reqwest::{Method, StatusCode};
use rumqttc::{EventLoop, Packet};
use serde_json::{Value, json};
use tokio::time::{MissedTickBehavior, interval, sleep};
use tracing::field::Empty;
use tracing::{Instrument, Span, info, info_span, warn};

use crate::state::{EdgeView, PlanView, Ticket, World};
use crate::{Console, Update, metrics};

const RETRY: Duration = Duration::from_secs(2);
const HEARTBEAT: Duration = Duration::from_secs(15);
/// The stream ingest publishes canonical events on.
const EVENTS: &str = "EVENTS";

pub async fn follow_events(console: Arc<Console>, js: jetstream::Context) {
    loop {
        match events(&console, &js).await {
            // A NATS reconnect: start over at once.
            Ok(()) => {}
            Err(error) => {
                warn!(%error, "following EVENTS, trying again");
                sleep(RETRY).await;
            }
        }
        // Whatever the old consumer would have brought, /world has.
        console.resync.notify_one();
    }
}

async fn events(console: &Console, js: &jetstream::Context) -> Result<(), String> {
    let stream = js.get_stream(EVENTS).await.map_err(|e| e.to_string())?;
    let consumer = stream
        .create_consumer(OrderedConfig {
            filter_subjects: vec!["events.trucks.>".into(), "events.plants.>".into()],
            deliver_policy: DeliverPolicy::LastPerSubject,
            ..OrderedConfig::default()
        })
        .await
        .map_err(|e| e.to_string())?;
    let mut messages = consumer.messages().await.map_err(|e| e.to_string())?;
    info!("following EVENTS");
    loop {
        tokio::select! {
            message = messages.next() => {
                let Some(message) = message else {
                    return Err("the EVENTS consumer ended".into());
                };
                let message = message.map_err(|e| e.to_string())?;
                apply(console, &message.payload, message.headers.as_ref());
            }
            () = console.restart.notified() => return Ok(()),
        }
    }
}

/// Applies one canonical event, in a span that continues its trace. Ingest marks GPS and drum
/// traces as unsampled, so their spans are dropped here as well.
pub fn apply(console: &Console, payload: &[u8], headers: Option<&HeaderMap>) {
    let Ok(envelope) = serde_json::from_slice::<EventEnvelope>(payload) else {
        return;
    };
    let span = info_span!("console push", event = Empty);
    telemetry::continue_trace(&span, headers);
    let _entered = span.enter();
    let at = envelope.occurred_at;
    let kind = match &envelope.event {
        Event::TruckPositionReported {
            truck, location, ..
        } => console
            .change("truck", |s| s.truck_position(&truck.0, *location, at))
            .then_some("truck"),
        Event::TruckStatusReported { truck, status } => {
            if *status == TruckStatus::OutOfService {
                console.breakdown_seen(&truck.0);
            }
            console
                .change("truck", |s| s.truck_status(&truck.0, *status, at))
                .then_some("truck")
        }
        Event::BatchTicketIssued {
            plant,
            ticket_no,
            truck,
            event,
            volume_m3,
            load,
            ..
        } => {
            let ticket = Ticket {
                ticket_no: ticket_no.clone(),
                truck: truck.0.clone(),
                event: *event,
                volume_m3: *volume_m3,
                order: load.as_ref().map(|l| l.order.0.clone()),
                load: load.as_ref().map(|l| l.sequence),
                at,
            };
            console
                .change("plant", |s| s.ticket(&plant.0, ticket))
                .then_some("ticket")
        }
        _ => None,
    };
    let Some(kind) = kind else { return };
    span.record("event", kind);
    if envelope.received_at >= console.started
        && let Ok(lag) = (domain::clock::now() - envelope.received_at).to_std()
    {
        metrics::event_lag(kind, lag);
    }
}

pub async fn follow_plans(console: Arc<Console>, nats: async_nats::Client) {
    loop {
        match nats.subscribe(PLANS_UPDATED).await {
            Ok(mut announcements) => {
                info!("following plans.updated");
                while let Some(message) = announcements.next().await {
                    announced(&console, &message).await;
                }
            }
            Err(error) => warn!(%error, "subscribing to plans.updated, trying again"),
        }
        sleep(RETRY).await;
    }
}

async fn announced(console: &Console, message: &async_nats::Message) {
    let Ok(announced) = serde_json::from_slice::<PlanUpdated>(&message.payload) else {
        return;
    };
    let span = info_span!("console push", event = "plan", version = announced.version);
    telemetry::continue_trace(&span, message.headers.as_ref());
    async {
        let trace_id = telemetry::trace_id(&span);
        console
            .load_plan(announced.version, announced.created_at, &span, trace_id)
            .await;
        // Plant statuses change along with plans, and nothing else announces them.
        if let Err(error) = console.load_world(Some(&span)).await {
            warn!(%error, "reading /world after a plan update");
        }
    }
    .instrument(span.clone())
    .await;
}

/// Reads `/world` at startup, and again whenever it is asked to.
pub async fn resync(console: Arc<Console>) {
    loop {
        while let Err(error) = console.load_world(None).await {
            warn!(%error, "reading /world, trying again");
            sleep(RETRY).await;
        }
        console.resync.notified().await;
    }
}

pub async fn watch_edge(console: Arc<Console>, url: String) {
    let url = format!("{}/status", url.trim_end_matches('/'));
    let mut tick = interval(console.settings.poll);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let status = match console.edge_http.get(&url).send().await {
            Ok(response) if response.status().is_success() => response
                .bytes()
                .await
                .ok()
                .and_then(|body| serde_json::from_slice::<Value>(&body).ok()),
            _ => None,
        };
        let now = domain::clock::now();
        console.change("edge", |s| {
            let edge = match status {
                Some(status) => EdgeView {
                    reachable: true,
                    at: Some(now),
                    status: Some(status),
                },
                // Unreachable: the last values stay, with the time they were read.
                None => EdgeView {
                    reachable: false,
                    ..s.edge.clone().unwrap_or(EdgeView {
                        reachable: false,
                        at: None,
                        status: None,
                    })
                },
            };
            s.edge(edge)
        });
    }
}

pub async fn watch_uplink(console: Arc<Console>, url: String) {
    // A console that stopped during a cut left the cut in place: undo it before anything else.
    while let Err(error) = console.uplink_reset().await {
        warn!(%error, %url, "resetting the uplink, trying again");
        sleep(RETRY).await;
    }
    console.uplink_ready.store(true, Ordering::Relaxed);
    info!(%url, "uplink reset");
    let mut tick = interval(console.settings.poll);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        if let Ok(proxies) = console.uplink_proxies().await {
            let cut = proxies.iter().any(|(_, enabled)| !enabled);
            console.change("uplink", |s| s.uplink_cut(cut));
        }
    }
}

pub async fn heartbeat(console: Arc<Console>) {
    let mut tick = interval(HEARTBEAT);
    loop {
        tick.tick().await;
        console.push(Update::new(
            "heartbeat",
            &json!({ "now": domain::clock::now() }),
        ));
    }
}

/// Polls the MQTT client's event loop, and keeps [`Console::mqtt_up`] true while it is connected.
pub async fn drive_mqtt(console: Arc<Console>, mut eventloop: EventLoop) {
    loop {
        match eventloop.poll().await {
            Ok(rumqttc::Event::Incoming(Packet::ConnAck(_))) => {
                console.mqtt_up.store(true, Ordering::Relaxed);
                info!("MQTT link up");
            }
            Ok(_) => {}
            Err(error) => {
                if console.mqtt_up.swap(false, Ordering::Relaxed) {
                    warn!(%error, "MQTT link down, reconnecting");
                }
                sleep(RETRY).await;
            }
        }
    }
}

impl Console {
    /// Pushes plan `version` once dispatch confirms it exists, with its diff, unless the console
    /// holds it already. A version held without a trace takes `trace_id` when one comes.
    /// Dispatch's trouble (a timeout, a 5xx) stores nothing: the next announcement or `/world`
    /// read tries again.
    pub(crate) async fn load_plan(
        &self,
        version: i64,
        created_at: DateTime<Utc>,
        span: &Span,
        trace_id: Option<String>,
    ) {
        let wanted = |held: Option<&PlanView>| {
            held.is_none_or(|held| {
                held.version < version
                    || (held.version == version && held.trace_id.is_none() && trace_id.is_some())
            })
        };
        if !wanted(self.state().plan.as_ref()) {
            return;
        }
        let path = format!("/plans/{version}/diff");
        let diff = match self.dispatch(Method::GET, &path, None, Some(span)).await {
            Ok(reply) if reply.status.is_success() => reply.body,
            // The first version has no diff, but it has to exist.
            Ok(reply) if reply.status == StatusCode::NOT_FOUND => {
                let path = format!("/plans/{version}");
                match self.dispatch(Method::GET, &path, None, Some(span)).await {
                    Ok(reply) if reply.status.is_success() => Value::Null,
                    _ => return,
                }
            }
            Ok(reply) => {
                warn!(version, status = %reply.status, "fetching the plan's diff");
                return;
            }
            Err(error) => {
                warn!(%error, version, "fetching the plan's diff");
                return;
            }
        };
        let plan = PlanView {
            version,
            created_at,
            diff,
            trace_id,
        };
        self.change("plan", |s| s.plan(plan));
    }

    /// Reads dispatch's `/world` and merges it, then catches up on a plan version the console
    /// missed. When dispatch's latest version is older than the console's, its data was reset:
    /// the console drops its plan and tells the pages to start over.
    pub(crate) async fn load_world(&self, span: Option<&Span>) -> Result<(), String> {
        let reply = self.dispatch(Method::GET, "/world", None, span).await?;
        if !reply.status.is_success() {
            return Err(format!("dispatch answered {}", reply.status));
        }
        let world: World = serde_json::from_value(reply.body).map_err(|e| e.to_string())?;
        {
            let mut state = self.state();
            let (plants, trucks) = state.world(&world);
            let updates = plants
                .iter()
                .filter_map(|p| Update::new("plant", p))
                .chain(trucks.iter().filter_map(|t| Update::new("truck", t)));
            for update in updates {
                metrics::pushed(update.event);
                let _ = self.updates.send(update);
            }
            let held = state.plan_version();
            if held.is_some_and(|held| world.plan_version.is_none_or(|latest| latest < held)) {
                warn!(?held, latest = ?world.plan_version, "dispatch's plans went back: starting over");
                state.plan = None;
                let _ = self.updates.send(Update {
                    event: "reset",
                    data: "{}".into(),
                });
            }
        }
        let held = self.state().plan_version();
        if let Some(latest) = world.plan_version
            && held.is_none_or(|held| held < latest)
        {
            // Its own trace is in an announcement the console never got.
            let catch_up = info_span!("console catch-up", version = latest);
            let reply = self
                .dispatch(
                    Method::GET,
                    &format!("/plans/{latest}"),
                    None,
                    Some(&catch_up),
                )
                .await?;
            if !reply.status.is_success() {
                return Err(format!(
                    "plan v{latest}: dispatch answered {}",
                    reply.status
                ));
            }
            let created_at = serde_json::from_value(reply.body["created_at"].clone())
                .map_err(|e| format!("plan v{latest}: {e}"))?;
            self.load_plan(latest, created_at, &catch_up, None).await;
        }
        Ok(())
    }
}
