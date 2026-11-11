//! Plan sync: fetches the plant's slice of the latest plan from dispatch, keeps it in SQLite, and
//! hands it to the batch computer. A failed fetch changes nothing but the dispatch link's state:
//! the agent keeps working from the plan it has, and its age keeps growing.

use std::sync::Arc;
use std::time::Duration;

use domain::{PlantId, PlantPlan};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{info, warn};

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
        tick.tick().await;
        let up = match fetch(&client, &url, &agent.plant.id).await {
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

async fn fetch(client: &reqwest::Client, url: &str, plant: &PlantId) -> Result<PlantPlan, String> {
    let response = client.get(url).send().await.map_err(|e| e.to_string())?;
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
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use chrono::Utc;
    use domain::Seed;
    use std::sync::Mutex;
    use tokio::time::{Instant, sleep};

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

    #[tokio::test]
    async fn it_keeps_each_slice_and_works_on_when_dispatch_is_down() {
        // A stand-in for dispatch that answers with whatever `serving` holds, or 503.
        let serving: Arc<Mutex<Option<PlantPlan>>> = Arc::default();
        let shared = Arc::clone(&serving);
        let app = Router::new().route(
            "/plants/plant-walldorf/plan",
            get(move || {
                let plan = shared.lock().unwrap().clone();
                async move {
                    match plan {
                        Some(plan) => axum::Json(plan).into_response(),
                        None => StatusCode::SERVICE_UNAVAILABLE.into_response(),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });

        let file = TempFile::new("sync");
        let plant = Seed::rhine_neckar()
            .unwrap()
            .plant("plant-walldorf")
            .unwrap()
            .clone();
        let agent = Arc::new(
            Agent::new(
                plant,
                Store::open(&file.0).unwrap(),
                Clock::start(Utc::now()),
            )
            .unwrap(),
        );
        let served = slice("plant-walldorf", 3);
        *serving.lock().unwrap() = Some(served.clone());
        let url = format!("{url}/");
        let syncing = tokio::spawn({
            let agent = Arc::clone(&agent);
            async move { run(agent, &url, Duration::from_millis(50)).await }
        });

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
}
