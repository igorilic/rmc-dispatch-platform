//! Plan sync: fetches the plant's slice of the latest plan from dispatch, keeps it in SQLite, and
//! hands it to the batch computer. A failed fetch changes nothing but the dispatch link's state:
//! the agent keeps working from the plan it has, and its age keeps growing.
//!
//! It fetches on an interval, and at once when the cloud sends a sync command over MQTT
//! ([`crate::forward`]). Only a requested fetch is traced: the interval's would be noise.

use std::sync::Arc;
use std::time::Duration;

use domain::{PlantId, PlantPlan};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{Instrument, info, info_span, warn};

use crate::store::StoreError;
use crate::{Agent, Synced};

/// How long one fetch may take.
// ponytail: fixed; a real cellular link may want more.
const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("HTTP client: {0}")]
    Client(#[from] reqwest::Error),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Fetches the slice from `dispatch_url` every `every` until the plan can't be stored.
pub async fn run(agent: Arc<Agent>, dispatch_url: &str, every: Duration) -> Result<(), SyncError> {
    let client = reqwest::Client::builder().timeout(TIMEOUT).build()?;
    let url = format!(
        "{}/plants/{}/plan",
        dispatch_url.trim_end_matches('/'),
        agent.plant.id.0
    );
    let mut tick = interval(every);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut logged: Option<bool> = None;
    loop {
        // On the interval, or at once when the cloud asks. A requested fetch has a span of its
        // own, which continues the request's trace and whose context the fetch carries to dispatch.
        let requested = tokio::select! {
            _ = tick.tick() => None,
            traceparent = agent.mqtt.sync_requested() => {
                tick.reset();
                Some(traceparent)
            }
        };
        let fetched = match requested {
            None => fetch(&client, &url, &agent.plant.id, None).await,
            Some(traceparent) => {
                let span = info_span!("plan sync", plant = %agent.plant.id.0);
                if let Some(traceparent) = &traceparent {
                    telemetry::continue_from(&span, traceparent);
                }
                let header = telemetry::traceparent(&span);
                fetch(&client, &url, &agent.plant.id, header.as_deref())
                    .instrument(span)
                    .await
            }
        };
        let up = match fetched {
            Ok(plan) => {
                let synced = Synced {
                    plan,
                    at: agent.clock.now(),
                };
                agent.store.save_plan(&synced)?;
                agent.plan.send_replace(Some(synced));
                if logged != Some(true) {
                    info!(%url, "dispatch link up");
                }
                true
            }
            Err(error) => {
                if logged != Some(false) {
                    warn!(%url, %error, "dispatch unreachable, working from the last plan");
                }
                false
            }
        };
        agent.set_dispatch_up(up);
        logged = Some(up);
    }
}

async fn fetch(
    client: &reqwest::Client,
    url: &str,
    plant: &PlantId,
    traceparent: Option<&str>,
) -> Result<PlantPlan, String> {
    let mut request = client.get(url);
    if let Some(traceparent) = traceparent {
        request = request.header("traceparent", traceparent);
    }
    let response = request.send().await.map_err(|e| e.to_string())?;
    if !response.status().is_success() {
        return Err(format!("dispatch answered {}", response.status()));
    }
    let body = response.bytes().await.map_err(|e| e.to_string())?;
    let plan: PlantPlan = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
    if plan.plant != *plant {
        return Err(format!("got the plan of {}", plan.plant.0));
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Clock;
    use crate::store::{Store, TempFile};
    use axum::Router;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use chrono::Utc;
    use domain::Seed;
    use std::sync::Mutex;
    use tokio::time::{Instant, sleep};
    use tracing::info_span;

    type Serving = Arc<Mutex<Option<PlantPlan>>>;
    type Seen = Arc<Mutex<Vec<Option<String>>>>;

    fn slice(plant: &str, version: i64) -> PlantPlan {
        PlantPlan {
            plant: PlantId(plant.into()),
            version: Some(version),
            created_at: Some(Utc::now()),
            loads: Vec::new(),
        }
    }

    async fn until(what: &str, check: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !check() {
            assert!(Instant::now() < deadline, "gave up waiting for {what}");
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// A stand-in for dispatch. It answers with whatever `Serving` holds, or 503, and keeps each
    /// request's `traceparent` header in `Seen`.
    async fn dispatch() -> (String, Serving, Seen) {
        let (serving, seen) = (Serving::default(), Seen::default());
        let (plan, headers_seen) = (Arc::clone(&serving), Arc::clone(&seen));
        let app = Router::new().route(
            "/plants/plant-walldorf/plan",
            get(move |headers: HeaderMap| {
                let traceparent = headers.get("traceparent").and_then(|v| v.to_str().ok());
                headers_seen
                    .lock()
                    .unwrap()
                    .push(traceparent.map(str::to_owned));
                let plan = plan.lock().unwrap().clone();
                async move {
                    match plan {
                        Some(plan) => axum::Json(plan).into_response(),
                        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        (url, serving, seen)
    }

    /// Walldorf's agent with an empty store in a temp file of its own.
    fn agent(name: &str) -> (Arc<Agent>, TempFile) {
        let file = TempFile::new(name);
        let plant = Seed::rhine_neckar()
            .unwrap()
            .plant("plant-walldorf")
            .unwrap()
            .clone();
        let agent = Agent::new(
            plant,
            Store::open(&file.0).unwrap(),
            Clock::start(Utc::now()),
        )
        .unwrap();
        (Arc::new(agent), file)
    }

    fn syncing(agent: &Arc<Agent>, url: String, every: Duration) -> tokio::task::JoinHandle<()> {
        let agent = Arc::clone(agent);
        tokio::spawn(async move {
            let _ = run(agent, &url, every).await;
        })
    }

    #[tokio::test]
    async fn it_keeps_each_slice_and_works_on_when_dispatch_is_down() {
        let (url, serving, _) = dispatch().await;
        let (agent, _file) = agent("sync");
        let served = slice("plant-walldorf", 3);
        *serving.lock().unwrap() = Some(served.clone());
        let syncing = syncing(&agent, url, Duration::from_millis(50));

        until("the first slice", || agent.status().unwrap().dispatch).await;
        assert_eq!(agent.status().unwrap().plan_version, Some(3));
        assert_eq!(
            agent.store.plan().unwrap().unwrap().plan,
            served,
            "kept in SQLite"
        );

        *serving.lock().unwrap() = None;
        until("the link to go down", || !agent.status().unwrap().dispatch).await;
        *serving.lock().unwrap() = Some(slice("plant-mannheim", 4));
        sleep(Duration::from_millis(300)).await;
        let status = agent.status().unwrap();
        assert!(!status.dispatch, "another plant's slice is refused");
        assert_eq!(status.plan_version, Some(3), "the last good plan stays");
        syncing.abort();
    }

    #[tokio::test]
    async fn a_sync_request_fetches_the_plan_at_once() {
        let (url, serving, _) = dispatch().await;
        let (agent, _file) = agent("sync-request");
        *serving.lock().unwrap() = Some(slice("plant-walldorf", 3));
        // An hour between polls: only the request can fetch again within the test.
        let syncing = syncing(&agent, url, Duration::from_secs(3600));
        until("the first slice", || {
            agent.status().unwrap().plan_version == Some(3)
        })
        .await;

        *serving.lock().unwrap() = Some(slice("plant-walldorf", 4));
        agent.mqtt.request_sync(None);
        until("the requested slice", || {
            agent.status().unwrap().plan_version == Some(4)
        })
        .await;
        syncing.abort();
    }

    #[tokio::test]
    async fn a_sync_request_continues_its_trace_into_the_fetch() {
        let (_guard, exporter) = telemetry::testing::capture();
        let (url, serving, seen) = dispatch().await;
        let (agent, _file) = agent("sync-trace");
        *serving.lock().unwrap() = Some(slice("plant-walldorf", 3));
        let syncing = syncing(&agent, url, Duration::from_secs(3600));
        until("the first fetch", || seen.lock().unwrap().len() == 1).await;
        assert_eq!(seen.lock().unwrap()[0], None, "a poll carries no trace");

        let click = info_span!("console action");
        agent.mqtt.request_sync(telemetry::traceparent(&click));
        until("the requested fetch", || seen.lock().unwrap().len() == 2).await;
        agent.mqtt.request_sync(Some("not a traceparent".into()));
        until("the second requested fetch", || {
            seen.lock().unwrap().len() == 3
        })
        .await;
        let finished_syncs = || {
            let spans = exporter.get_finished_spans().unwrap();
            spans.iter().filter(|s| s.name == "plan sync").count()
        };
        until("both sync spans to end", || finished_syncs() == 2).await;
        syncing.abort();
        drop(click);

        let spans = exporter.get_finished_spans().unwrap();
        let click = spans.iter().find(|s| s.name == "console action").unwrap();
        let syncs: Vec<_> = spans.iter().filter(|s| s.name == "plan sync").collect();
        assert_eq!(
            syncs.len(),
            2,
            "one span per requested fetch, none per poll"
        );
        assert_eq!(
            syncs[0].span_context.trace_id(),
            click.span_context.trace_id()
        );
        let sent = seen.lock().unwrap()[1].clone().unwrap();
        assert!(
            sent.contains(&syncs[0].span_context.span_id().to_string()),
            "the fetch carries the sync span's own context: {sent}"
        );
        assert_ne!(
            syncs[1].span_context.trace_id(),
            click.span_context.trace_id(),
            "an invalid traceparent starts a trace of its own"
        );
        assert!(seen.lock().unwrap()[2].is_some());
    }
}
