//! Actions from the map. Each one is a trace of its own: a root span here, whose context goes to
//! dispatch in a `traceparent` header and to the plant in the sync command's payload. Each ends
//! in the log, with its trace, and in the actions metric.
//!
//! A breakdown travels as the simulators' MQTT 3.1.1 command, which carries no trace context, so
//! its trace ends at the publish; the re-dispatch it causes has its own trace, from ingest.
//!
//! Make chaos repeats `make chaos`'s steps inside the cluster: orders near the plant, a wait until
//! the plant holds their plan, a cut of the plant's uplink (Toxiproxy closes both sides of every
//! connection), and a report of the tickets that waited out the cut. Whatever happens during
//! the cut, the uplink comes back.

use std::sync::{Arc, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use chrono::{SecondsFormat, TimeDelta, TimeZone};
use chrono_tz::Europe::Berlin;
use domain::{PlantStatus, TruckId, TruckStatus};
use reqwest::Method;
use rumqttc::QoS;
use serde::Serialize;
use serde_json::{Value, json};
use simulators::customers::{WebhookOrder, rush_orders};
use tokio::time::sleep;
use tracing::{Instrument, Span, info_span, warn};

use crate::state::{LogEntry, Run, Step};
use crate::{Console, metrics};

/// How long a sent breakdown counts as on its way, if the truck never reports it.
const PENDING: Duration = Duration::from_secs(30);
/// The mix of Make chaos's orders, the one `make chaos` uses.
const MIX: &str = "C30/37-XC4";
/// How long Make chaos waits for dispatch to plan its orders, and then for the plant to hold that
/// plan.
const PLANNING: Duration = Duration::from_secs(60);
/// How long it waits for the outbox to drain after the cut, as `make chaos` does.
const DRAINING: Duration = Duration::from_secs(120);
/// How often it tries to bring the uplink back before it gives up.
const RESTORE_TRIES: u32 = 5;

/// An action that went through: its trace, and the plan version it made, if it made one.
#[derive(Debug, Clone, Serialize)]
pub struct Done {
    pub trace_id: Option<String>,
    pub version: Option<i64>,
}

impl Done {
    fn new() -> Done {
        Done {
            trace_id: None,
            version: None,
        }
    }
}

/// Why an action didn't go through.
#[derive(Debug, Clone)]
pub struct Refused {
    pub status: StatusCode,
    pub message: String,
}

pub type Outcome = Result<Done, Refused>;

fn refused(status: StatusCode, message: impl Into<String>) -> Refused {
    Refused {
        status,
        message: message.into(),
    }
}

impl Console {
    /// Breaks a simulated truck down, unless it is out of service already or its breakdown is on
    /// its way: a second command would break it again right after its repair.
    pub async fn break_down(&self, truck: &str) -> Outcome {
        let span = info_span!("console action", action = "break_down", target = truck);
        let outcome = async {
            if self.seed.truck(truck).is_none() {
                return Err(refused(
                    StatusCode::BAD_REQUEST,
                    format!("{truck} isn't a truck"),
                ));
            }
            let out = self
                .state()
                .trucks
                .get(truck)
                .is_some_and(|t| t.status == TruckStatus::OutOfService);
            if out {
                return Err(refused(
                    StatusCode::CONFLICT,
                    format!("{truck} is already out of service"),
                ));
            }
            if self
                .pending()
                .get(truck)
                .is_some_and(|at| at.elapsed() < PENDING)
            {
                return Err(refused(
                    StatusCode::CONFLICT,
                    format!("{truck}'s breakdown is on its way"),
                ));
            }
            if !self.mqtt_up() {
                return Err(refused(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "the console's MQTT link is down",
                ));
            }
            let topic = domain::breakdown_topic(&TruckId(truck.to_owned()));
            self.mqtt
                .publish(topic, QoS::AtLeastOnce, false, Vec::new())
                .await
                .map_err(|e| refused(StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
            self.pending().insert(truck.to_owned(), Instant::now());
            Ok(Done::new())
        }
        .instrument(span.clone())
        .await;
        self.finish("break_down", "Break down", truck, &span, outcome)
    }

    /// Delays a site's pours today by `minutes`.
    pub async fn delay_site(&self, site: &str, minutes: u32) -> Outcome {
        let span = info_span!("console action", action = "delay_site", target = site);
        let outcome = async {
            if self.seed.site(site).is_none() {
                return Err(refused(
                    StatusCode::BAD_REQUEST,
                    format!("{site} isn't a site"),
                ));
            }
            if !(1..=1440).contains(&minutes) {
                return Err(refused(
                    StatusCode::BAD_REQUEST,
                    "a delay is 1 to 1,440 minutes",
                ));
            }
            let body = json!({ "minutes": minutes });
            let path = format!("/sites/{site}/delays");
            let reply = self.call(Method::POST, &path, Some(&body), &span).await?;
            Ok(Done {
                version: reply["version"].as_i64(),
                ..Done::new()
            })
        }
        .instrument(span.clone())
        .await;
        let target = format!("{site}, {minutes} min");
        self.finish("delay_site", "Delay", &target, &span, outcome)
    }

    /// Takes a plant offline or brings it back.
    pub async fn set_plant_status(&self, plant: &str, status: PlantStatus) -> Outcome {
        let span = info_span!("console action", action = "plant_status", target = plant);
        let outcome = async {
            if self.seed.plant(plant).is_none() {
                return Err(refused(
                    StatusCode::BAD_REQUEST,
                    format!("{plant} isn't a plant"),
                ));
            }
            let body = json!({ "status": status });
            let path = format!("/plants/{plant}/status");
            let reply = self.call(Method::PUT, &path, Some(&body), &span).await?;
            // With an empty diff no version follows, so the status is set from the answer.
            self.change("plant", |s| s.plant_status(plant, status));
            Ok(Done {
                version: reply["version"].as_i64(),
                ..Done::new()
            })
        }
        .instrument(span.clone())
        .await;
        let label = match status {
            PlantStatus::Online => "Bring online",
            PlantStatus::Offline => "Take offline",
        };
        self.finish("plant_status", label, plant, &span, outcome)
    }

    /// Plans everything that isn't frozen again.
    pub async fn replan(&self) -> Outcome {
        let span = info_span!("console action", action = "replan", target = "");
        let outcome = async {
            let reply = self.call(Method::POST, "/plans", None, &span).await?;
            Ok(Done {
                version: reply["version"].as_i64(),
                ..Done::new()
            })
        }
        .instrument(span.clone())
        .await;
        self.finish("replan", "Re-plan", "", &span, outcome)
    }

    /// Asks the plant's agent to fetch its plan now. Refused while the agent's link to the broker
    /// is down: the broker would drop the command, and the agent fetches within its poll interval
    /// of reconnecting anyway.
    pub async fn sync_plant(&self, plant: &str) -> Outcome {
        let span = info_span!("console action", action = "sync", target = plant);
        let outcome = async {
            if plant != self.settings.plant.0 || self.settings.plant_agent_url.is_none() {
                return Err(refused(
                    StatusCode::BAD_REQUEST,
                    format!("{plant} has no edge agent the console watches"),
                ));
            }
            let (connected, name) = {
                let state = self.state();
                let edge = state.edge.as_ref();
                let connected = edge.is_some_and(|e| {
                    e.reachable && e.status.as_ref().is_some_and(|s| s["mqtt"] == true)
                }) && !state.uplink.cut;
                let name = state.plants.get(plant).map(|p| p.name.clone());
                (connected, name.unwrap_or_else(|| plant.to_owned()))
            };
            if !connected {
                return Err(refused(
                    StatusCode::CONFLICT,
                    format!("{name}'s link is down: it fetches its plan soon after it reconnects"),
                ));
            }
            if !self.mqtt_up() {
                return Err(refused(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "the console's MQTT link is down",
                ));
            }
            let payload = telemetry::traceparent(&span).unwrap_or_default();
            self.mqtt
                .publish(
                    domain::sync_topic(&self.settings.plant),
                    QoS::AtLeastOnce,
                    false,
                    payload.into_bytes(),
                )
                .await
                .map_err(|e| refused(StatusCode::SERVICE_UNAVAILABLE, e.to_string()))?;
            Ok(Done::new())
        }
        .instrument(span.clone())
        .await;
        self.finish("sync", "Sync now", plant, &span, outcome)
    }

    /// Starts Make chaos and returns at once; the run reports its steps on the uplink card.
    pub fn make_chaos(self: &Arc<Self>) -> Outcome {
        let plant = self.settings.plant.0.clone();
        let span = info_span!("console action", action = "make_chaos", target = %plant);
        let trace_id = telemetry::trace_id(&span);
        let mut refusal = None;
        self.change("uplink", |s| {
            let check = if !s.uplink.available {
                Err(refused(
                    StatusCode::NOT_FOUND,
                    "Make chaos needs the plant's uplink and its monitoring path",
                ))
            } else if s.uplink.run.as_ref().is_some_and(Run::active) {
                Err(refused(
                    StatusCode::CONFLICT,
                    "Make chaos is already running",
                ))
            } else if !s.edge.as_ref().is_some_and(|e| e.reachable) {
                Err(refused(StatusCode::CONFLICT, "the plant doesn't answer"))
            } else {
                Ok(())
            };
            if let Err(r) = check {
                refusal = Some(r);
                return None;
            }
            s.uplink.run = Some(Run {
                step: Step::Ordering,
                orders: Vec::new(),
                plan_version: None,
                cut_until: None,
                held: None,
                report: None,
                trace_id: trace_id.clone(),
            });
            Some(s.uplink.clone())
        });
        if let Some(refusal) = refusal {
            return self.finish("make_chaos", "Make chaos", &plant, &span, Err(refusal));
        }
        tokio::spawn(Arc::clone(self).chaos(span.clone()));
        self.finish("make_chaos", "Make chaos", &plant, &span, Ok(Done::new()))
    }

    /// Ends Make chaos's cut early, or brings back an uplink that is cut without a run.
    pub async fn reconnect(&self) -> Outcome {
        let plant = self.settings.plant.0.clone();
        let span = info_span!("console action", action = "reconnect", target = %plant);
        let outcome = async {
            let (step, cut) = {
                let state = self.state();
                let step = state
                    .uplink
                    .run
                    .as_ref()
                    .filter(|r| r.active())
                    .map(|r| r.step);
                (step, state.uplink.cut)
            };
            match step {
                Some(Step::Cut) => {
                    self.reconnect.notify_one();
                    Ok(Done::new())
                }
                Some(_) => Err(refused(
                    StatusCode::CONFLICT,
                    "Make chaos hasn't cut the uplink yet",
                )),
                None if cut => {
                    let names = self.uplink_names().await?;
                    self.restore(&names)
                        .await
                        .map_err(|e| refused(StatusCode::BAD_GATEWAY, e))?;
                    Ok(Done::new())
                }
                None => Err(refused(StatusCode::CONFLICT, "the uplink isn't cut")),
            }
        }
        .instrument(span.clone())
        .await;
        self.finish("reconnect", "Reconnect", &plant, &span, outcome)
    }

    /// A plant's slice of the latest plan, for its popup.
    pub async fn plant_plan(&self, plant: &str) -> Result<Value, Refused> {
        if self.seed.plant(plant).is_none() {
            return Err(refused(
                StatusCode::BAD_REQUEST,
                format!("{plant} isn't a plant"),
            ));
        }
        self.read(&format!("/plants/{plant}/plan")).await
    }

    /// A site's orders that start today, Berlin time on the world clock, for its popup.
    pub async fn site_orders(&self, site: &str) -> Result<Value, Refused> {
        if self.seed.site(site).is_none() {
            return Err(refused(
                StatusCode::BAD_REQUEST,
                format!("{site} isn't a site"),
            ));
        }
        let today = domain::clock::now().with_timezone(&Berlin).date_naive();
        let midnight = |day: chrono::NaiveDate| {
            day.and_hms_opt(0, 0, 0)
                .and_then(|t| Berlin.from_local_datetime(&t).earliest())
                .map(|t| t.to_utc().to_rfc3339_opts(SecondsFormat::Secs, true))
        };
        let (Some(from), Some(to)) = (midnight(today), today.succ_opt().and_then(midnight)) else {
            return Err(refused(
                StatusCode::INTERNAL_SERVER_ERROR,
                "no midnight today",
            ));
        };
        let orders = self.read(&format!("/orders?from={from}&to={to}")).await?;
        let here: Vec<&Value> = orders
            .as_array()
            .into_iter()
            .flatten()
            .filter(|o| o["site"] == site)
            .collect();
        Ok(json!(here))
    }

    /// Forgets a pending breakdown once the truck reports it.
    pub(crate) fn breakdown_seen(&self, truck: &str) {
        self.pending().remove(truck);
    }

    fn pending(&self) -> MutexGuard<'_, std::collections::HashMap<String, Instant>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Calls dispatch for an action and returns its JSON, or why it didn't work.
    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        span: &Span,
    ) -> Result<Value, Refused> {
        let reply = self
            .dispatch(method, path, body, Some(span))
            .await
            .map_err(|e| refused(StatusCode::BAD_GATEWAY, format!("dispatch: {e}")))?;
        match reply.status {
            status if status.is_success() => Ok(reply.body),
            StatusCode::NOT_FOUND | StatusCode::UNPROCESSABLE_ENTITY => Err(refused(
                StatusCode::BAD_REQUEST,
                format!("dispatch answered {}", reply.status),
            )),
            status => Err(refused(
                StatusCode::BAD_GATEWAY,
                format!("dispatch answered {status}"),
            )),
        }
    }

    async fn read(&self, path: &str) -> Result<Value, Refused> {
        let reply = self
            .dispatch(Method::GET, path, None, None)
            .await
            .map_err(|e| refused(StatusCode::BAD_GATEWAY, format!("dispatch: {e}")))?;
        if !reply.status.is_success() {
            return Err(refused(
                StatusCode::BAD_GATEWAY,
                format!("dispatch answered {}", reply.status),
            ));
        }
        Ok(reply.body)
    }

    /// Logs the action with its trace, counts it, and gives the answer its trace id.
    fn finish(
        &self,
        name: &'static str,
        label: &str,
        target: &str,
        span: &Span,
        outcome: Outcome,
    ) -> Outcome {
        let trace_id = telemetry::trace_id(span);
        let (result, text) = match &outcome {
            Ok(done) => (
                "done",
                done.version
                    .map_or_else(|| "done".to_owned(), |v| format!("plan v{v}")),
            ),
            Err(r) if r.status.is_server_error() => ("failed", r.message.clone()),
            Err(r) => ("refused", r.message.clone()),
        };
        metrics::action(name, result);
        self.log(label, target, &text, trace_id.clone());
        outcome.map(|done| Done { trace_id, ..done })
    }

    fn log(&self, action: &str, target: &str, outcome: &str, trace_id: Option<String>) {
        let entry = LogEntry {
            at: domain::clock::now(),
            action: action.to_owned(),
            target: target.to_owned(),
            outcome: outcome.to_owned(),
            trace_id,
        };
        self.change("log", |s| Some(s.log(entry)));
    }

    async fn chaos(self: Arc<Self>, span: Span) {
        let result = self.chaos_steps().instrument(span.clone()).await;
        let (step, report) = match result {
            Ok(report) => (Step::Done, report),
            Err(error) => (Step::Failed, error),
        };
        self.change("uplink", |s| {
            s.run(|run| {
                run.step = step;
                run.report = Some(report.clone());
            })
        });
        let plant = self.settings.plant.0.clone();
        self.log("Make chaos", &plant, &report, telemetry::trace_id(&span));
    }

    async fn chaos_steps(&self) -> Result<String, String> {
        let plant = &self.settings.plant.0;
        let orders =
            rush_orders(&self.seed, plant, MIX, domain::clock::now()).map_err(|e| e.to_string())?;
        let ids: Vec<String> = orders.iter().map(|o| o.id.clone()).collect();
        self.change("uplink", |s| s.run(|run| run.orders = ids.clone()));
        for order in &orders {
            self.place(order).await?;
        }
        self.change("uplink", |s| s.run(|run| run.step = Step::Planning));
        let version = self.until_planned(&ids).await?;
        self.change("uplink", |s| {
            s.run(|run| {
                run.step = Step::Syncing;
                run.plan_version = Some(version);
            })
        });
        self.until_held(version).await?;

        let names = self.uplink_names().await.map_err(|r| r.message)?;
        let until =
            domain::clock::now() + TimeDelta::from_std(self.settings.cut).unwrap_or_default();
        self.change("uplink", |s| {
            s.run(|run| {
                run.step = Step::Cut;
                run.cut_until = Some(until);
            })
        });
        let cut = self.set_proxies(&names, false).await;
        if cut.is_ok() {
            tokio::select! {
                () = sleep(self.settings.cut) => {}
                () = self.reconnect.notified() => {}
            }
        }
        // Nothing leaves the outbox during the cut, so what waits at its end is what it held back.
        let held = self.outbox();
        self.restore(&names).await?;
        cut?;
        self.change("uplink", |s| {
            s.run(|run| {
                run.step = Step::Draining;
                run.held = held;
            })
        });
        let drained = self.until_drained().await?;
        Ok(match held {
            Some(held) if held > 0 => format!(
                "{held} tickets waited out the cut and were sent in order within {} s of reconnecting",
                drained.as_secs()
            ),
            _ => "No ticket fell due during the cut; run it again".to_owned(),
        })
    }

    /// Posts an order to its customer's webhook, as the customer's system does.
    async fn place(&self, order: &WebhookOrder) -> Result<(), String> {
        let webhook = match order.integration {
            "bauwerk-ag" => &self.settings.bauwerk,
            _ => &self.settings.neckar,
        };
        let response = self
            .http
            .post(&webhook.url)
            .bearer_auth(&webhook.token)
            .header("content-type", order.content_type)
            .body(order.body.clone())
            .send()
            .await
            .map_err(|e| format!("{}: {e}", order.integration))?;
        if !response.status().is_success() {
            return Err(format!(
                "{} refused the order: {}",
                order.integration,
                response.status()
            ));
        }
        Ok(())
    }

    /// Waits until a plan version plans `orders`, or says why it can't, and returns it.
    async fn until_planned(&self, orders: &[String]) -> Result<i64, String> {
        let deadline = Instant::now() + PLANNING;
        while Instant::now() < deadline {
            let latest = self
                .dispatch(Method::GET, "/plans/latest", None, Some(&Span::current()))
                .await;
            if let Ok(reply) = latest
                && reply.status.is_success()
            {
                let named: Vec<&Value> = ["loads", "unplanned"]
                    .iter()
                    .flat_map(|key| reply.body[key].as_array().into_iter().flatten())
                    .map(|entry| &entry["order"])
                    .collect();
                if orders.iter().all(|o| named.iter().any(|n| *n == o)) {
                    return reply.body["version"]
                        .as_i64()
                        .ok_or_else(|| "the plan has no version".to_owned());
                }
            }
            sleep(self.settings.poll).await;
        }
        Err("dispatch didn't plan the orders within a minute".into())
    }

    /// Waits until the plant's agent holds plan `version` or a later one.
    async fn until_held(&self, version: i64) -> Result<(), String> {
        let deadline = Instant::now() + PLANNING;
        while Instant::now() < deadline {
            if self.edge_status()["plan_version"]
                .as_i64()
                .is_some_and(|held| held >= version)
            {
                return Ok(());
            }
            sleep(self.settings.poll).await;
        }
        Err(format!(
            "the plant didn't get plan v{version} within a minute"
        ))
    }

    /// Waits until the outbox is empty and both links are up again, and returns how long it took.
    async fn until_drained(&self) -> Result<Duration, String> {
        let started = Instant::now();
        while started.elapsed() < DRAINING {
            let status = self.edge_status();
            if status["outbox"] == 0 && status["mqtt"] == true && status["dispatch"] == true {
                return Ok(started.elapsed());
            }
            sleep(self.settings.poll).await;
        }
        Err("the outbox didn't drain within two minutes".into())
    }

    fn edge_status(&self) -> Value {
        let state = self.state();
        state
            .edge
            .as_ref()
            .filter(|e| e.reachable)
            .and_then(|e| e.status.clone())
            .unwrap_or(Value::Null)
    }

    fn outbox(&self) -> Option<u64> {
        self.state()
            .edge
            .as_ref()
            .and_then(|e| e.status.as_ref())
            .and_then(|s| s["outbox"].as_u64())
    }

    fn uplink_url(&self) -> Result<String, String> {
        self.settings
            .uplink_url
            .as_deref()
            .map(|url| url.trim_end_matches('/').to_owned())
            .ok_or_else(|| "no uplink".to_owned())
    }

    /// Enables every proxy and removes every toxic.
    pub(crate) async fn uplink_reset(&self) -> Result<(), String> {
        let url = format!("{}/reset", self.uplink_url()?);
        let response = self
            .http
            .post(url)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !response.status().is_success() {
            return Err(format!("the uplink answered {}", response.status()));
        }
        Ok(())
    }

    /// The uplink's proxies, and whether each is enabled.
    pub(crate) async fn uplink_proxies(&self) -> Result<Vec<(String, bool)>, String> {
        let url = format!("{}/proxies", self.uplink_url()?);
        let response = self.http.get(url).send().await.map_err(|e| e.to_string())?;
        let body = response.bytes().await.map_err(|e| e.to_string())?;
        let proxies: serde_json::Map<String, Value> =
            serde_json::from_slice(&body).map_err(|e| e.to_string())?;
        Ok(proxies
            .into_iter()
            .map(|(name, proxy)| (name, proxy["enabled"] != false))
            .collect())
    }

    async fn uplink_names(&self) -> Result<Vec<String>, Refused> {
        let proxies = self
            .uplink_proxies()
            .await
            .map_err(|e| refused(StatusCode::BAD_GATEWAY, format!("the uplink: {e}")))?;
        Ok(proxies.into_iter().map(|(name, _)| name).collect())
    }

    async fn set_proxies(&self, names: &[String], enabled: bool) -> Result<(), String> {
        let url = self.uplink_url()?;
        for name in names {
            let response = self
                .http
                .patch(format!("{url}/proxies/{name}"))
                .header("content-type", "application/json")
                .body(json!({ "enabled": enabled }).to_string())
                .send()
                .await
                .map_err(|e| e.to_string())?;
            if !response.status().is_success() {
                return Err(format!(
                    "the uplink answered {} for {name}",
                    response.status()
                ));
            }
        }
        Ok(())
    }

    /// Re-enables the proxies, trying a few times: a plant must never stay cut by mistake.
    async fn restore(&self, names: &[String]) -> Result<(), String> {
        let mut tries = 0;
        loop {
            match self.set_proxies(names, true).await {
                Ok(()) => return Ok(()),
                Err(error) if tries + 1 < RESTORE_TRIES => {
                    warn!(%error, "bringing the uplink back, trying again");
                    tries += 1;
                    sleep(Duration::from_secs(1)).await;
                }
                Err(error) => {
                    return Err(format!(
                        "the uplink may still be cut ({error}); press Reconnect"
                    ));
                }
            }
        }
    }
}
