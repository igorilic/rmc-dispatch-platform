//! The agent's HTTP endpoints (SPEC §6): `/healthz` once the process is up, `/readyz` once it has a
//! plan to work from, and `/status`, one JSON line for people and for `make chaos`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use tracing::error;

use crate::{Agent, Status};

pub fn router(agent: Arc<Agent>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/readyz", get(readyz))
        .route("/status", get(status))
        .with_state(agent)
}

/// Ready with a plan, fetched or stored. The cloud link doesn't matter: an offline plant works.
async fn readyz(State(agent): State<Arc<Agent>>) -> StatusCode {
    if agent.ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn status(State(agent): State<Arc<Agent>>) -> Result<Json<Status>, StatusCode> {
    agent.status().map(Json).map_err(|e| {
        error!(error = %e, "can't read the status");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Store, TempFile};
    use crate::{Clock, Synced};
    use axum::body::{Body, to_bytes};
    use axum::http::Request;
    use chrono::Utc;
    use domain::{PlantPlan, Seed};
    use serde_json::{Value, json};
    use tower::ServiceExt;

    async fn get(router: &Router, uri: &str) -> (StatusCode, Vec<u8>) {
        let request = Request::get(uri).body(Body::empty()).unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, body.to_vec())
    }

    #[tokio::test]
    async fn ready_once_it_has_a_plan_whatever_the_link() {
        let file = TempFile::new("http");
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
        let router = router(Arc::clone(&agent));

        assert_eq!(get(&router, "/healthz").await.0, StatusCode::OK);
        assert_eq!(
            get(&router, "/readyz").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );

        agent.plan.send_replace(Some(Synced {
            plan: PlantPlan {
                plant: agent.plant.id.clone(),
                version: Some(4),
                created_at: None,
                loads: Vec::new(),
            },
            at: agent.clock.now(),
        }));
        assert_eq!(
            get(&router, "/readyz").await.0,
            StatusCode::OK,
            "with both links down"
        );

        let (code, body) = get(&router, "/status").await;
        assert_eq!(code, StatusCode::OK);
        let status: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            status,
            json!({
                "plant": "plant-walldorf", "mqtt": false, "dispatch": false, "outbox": 0,
                "sync_lag_seconds": 0, "plan_version": 4, "plan_age_seconds": 0,
                "queued": 0, "sent": 0
            })
        );
    }
}
