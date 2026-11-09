//! The webhook server against a real NATS container: auth, duplicates, dead letters, backpressure.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use common::{customers, nats, sample};
use domain::Seed;
use ingest::mapping::Mapping;
use ingest::nats::{Bus, DLQ, EVENTS};
use ingest::sources::{Webhooks, webhook_router};
use serde_json::{Value, json};
use tower::ServiceExt;

struct App {
    _nats: testcontainers::ContainerAsync<testcontainers::GenericImage>,
    router: Router,
    bus: Bus,
}

async fn app(max_in_flight: usize) -> App {
    let (container, url) = nats().await;
    let bus = Bus::connect(&url, Duration::from_secs(3600)).await.unwrap();
    let mappings = Mapping::load_all(&customers()).unwrap();
    let token = |integration: &str| format!("dev-token-{integration}");
    let webhooks = Webhooks::new(
        bus.clone(),
        Arc::new(Seed::rhine_neckar().unwrap()),
        mappings,
        token,
        max_in_flight,
    );
    App {
        _nats: container,
        router: webhook_router(webhooks),
        bus,
    }
}

async fn post(
    app: &App,
    integration: &str,
    token: Option<&str>,
    body: Vec<u8>,
) -> (StatusCode, Option<String>, Value) {
    let mut request = Request::post(format!("/webhooks/{integration}"));
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = app
        .router
        .clone()
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let retry_after = response
        .headers()
        .get(header::RETRY_AFTER)
        .map(|v| v.to_str().unwrap().to_owned());
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    (
        status,
        retry_after,
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned())),
    )
}

#[tokio::test]
async fn an_order_is_accepted_after_it_is_published_and_a_retry_is_a_duplicate() {
    let app = app(8).await;
    let bauwerk = Some("dev-token-bauwerk-ag");
    let (status, _, body) = post(
        &app,
        "bauwerk-ag",
        bauwerk,
        sample("bauwerk-ag", "order.json"),
    )
    .await;
    assert_eq!(
        (status, body),
        (
            StatusCode::ACCEPTED,
            json!({"published": 1, "duplicates": 0, "dead_lettered": 0})
        )
    );
    let (status, _, body) = post(
        &app,
        "bauwerk-ag",
        bauwerk,
        sample("bauwerk-ag", "order.json"),
    )
    .await;
    assert_eq!(
        (status, body),
        (
            StatusCode::ACCEPTED,
            json!({"published": 0, "duplicates": 1, "dead_lettered": 0})
        )
    );

    let neckar = Some("dev-token-neckar-infra");
    let (status, _, _) = post(
        &app,
        "neckar-infra",
        neckar,
        sample("neckar-infra", "order-v2.xml"),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(app.bus.count(EVENTS).await.unwrap(), 2);
}

#[tokio::test]
async fn a_broken_order_is_accepted_and_dead_lettered() {
    let app = app(8).await;
    let (status, _, body) = post(
        &app,
        "bauwerk-ag",
        Some("dev-token-bauwerk-ag"),
        sample("bauwerk-ag", "broken.json"),
    )
    .await;
    assert_eq!(
        (status, body),
        (
            StatusCode::ACCEPTED,
            json!({"published": 0, "duplicates": 0, "dead_lettered": 1})
        )
    );
    assert_eq!(app.bus.count(DLQ).await.unwrap(), 1);
    assert_eq!(app.bus.count(EVENTS).await.unwrap(), 0);
}

#[tokio::test]
async fn only_known_webhook_integrations_with_their_own_token_get_in() {
    let app = app(8).await;
    let order = || sample("bauwerk-ag", "order.json");
    assert_eq!(
        post(&app, "bauwerk-ag", None, order()).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(&app, "bauwerk-ag", Some("wrong"), order()).await.0,
        StatusCode::UNAUTHORIZED
    );
    let other = Some("dev-token-neckar-infra");
    assert_eq!(
        post(&app, "bauwerk-ag", other, order()).await.0,
        StatusCode::UNAUTHORIZED
    );
    // Unknown and SFTP-only integrations look exactly like a wrong token, so a caller without a
    // token can't find out which integration names exist.
    let wrong_token = post(&app, "bauwerk-ag", Some("wrong"), order()).await;
    let unknown = post(&app, "nobody", Some("dev-token-nobody"), order()).await;
    let sftp_only = post(
        &app,
        "rheinbau-gmbh",
        Some("dev-token-rheinbau-gmbh"),
        order(),
    )
    .await;
    assert_eq!(unknown, wrong_token);
    assert_eq!(sftp_only, wrong_token);

    let huge = vec![b' '; 2 << 20];
    let token = Some("dev-token-bauwerk-ag");
    assert_eq!(
        post(&app, "bauwerk-ag", token, huge).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(app.bus.count(EVENTS).await.unwrap(), 0);
}

#[tokio::test]
async fn when_full_it_answers_503_with_retry_after_instead_of_queueing() {
    let app = app(0).await;
    let (status, retry_after, _) = post(
        &app,
        "bauwerk-ag",
        Some("dev-token-bauwerk-ag"),
        sample("bauwerk-ag", "order.json"),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(retry_after.as_deref(), Some("2"));
    assert_eq!(app.bus.count(EVENTS).await.unwrap(), 0);
}

#[tokio::test]
async fn healthz_answers() {
    let app = app(8).await;
    let response = app
        .router
        .clone()
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
