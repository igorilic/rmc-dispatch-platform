//! The REST API, driven through the router against a real Postgres container.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

mod common;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use common::{at, order, postgres};
use dispatch::api::{self, VersionedPlan};
use dispatch::redispatch::Settings;
use dispatch::search::Budget;
use dispatch::store::Store;
use dispatch::travel::HaversineTravelTime;
use dispatch::verify::violations;
use dispatch::worker;
use domain::{Order, OrderStatus, Seed};
use serde_json::Value;
use tower::ServiceExt;

/// The planning clock, with nanoseconds that Postgres can't keep.
fn clock() -> DateTime<Utc> {
    at(6, 0) + TimeDelta::nanoseconds(123_456_789)
}

struct Setup {
    _pg: testcontainers::ContainerAsync<testcontainers::GenericImage>,
    store: Store,
    router: Router,
}

async fn setup(orders: &[Order]) -> Setup {
    let (pg, url) = postgres().await;
    let store = Store::connect(&url).await.unwrap();
    store
        .import_master_data(&Seed::rhine_neckar().unwrap())
        .await
        .unwrap();
    for o in orders {
        store.upsert_order(o, at(5, 0), None).await.unwrap();
    }
    let (jobs, queue) = worker::channel();
    let settings = Settings {
        budget: Budget::moves(50),
        ..Settings::default()
    };
    tokio::spawn(worker::run(store.clone(), queue, settings, clock));
    let router = api::router(store.clone(), jobs);
    Setup {
        _pg: pg,
        store,
        router,
    }
}

struct Reply {
    status: StatusCode,
    location: Option<String>,
    body: Vec<u8>,
}

impl Reply {
    fn json<T: serde::de::DeserializeOwned>(&self) -> T {
        serde_json::from_slice(&self.body).unwrap()
    }
}

async fn call(router: &Router, method: &str, uri: &str) -> Reply {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    Reply {
        status: response.status(),
        location: response
            .headers()
            .get(header::LOCATION)
            .map(|v| v.to_str().unwrap().to_owned()),
        body: to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    }
}

#[tokio::test]
async fn a_plan_from_stored_orders_keeps_every_hard_constraint() {
    let orders = [
        order("BW-1", 42.5, at(9, 0)),
        order("BW-2", 18.0, at(10, 30)),
        Order {
            status: OrderStatus::Cancelled,
            ..order("BW-cancelled", 9.0, at(9, 0))
        },
        order("BW-next-week", 9.0, at(9, 0) + TimeDelta::days(7)),
    ];
    let s = setup(&orders).await;
    assert_eq!(
        call(&s.router, "GET", "/plans/latest").await.status,
        StatusCode::NOT_FOUND
    );

    let created = call(&s.router, "POST", "/plans").await;
    assert_eq!(created.status, StatusCode::CREATED);
    assert_eq!(created.location.as_deref(), Some("/plans/1"));
    let first: VersionedPlan = created.json();
    assert_eq!(first.version, 1);
    assert_eq!(first.plan.created_at, at(6, 0), "planned at whole seconds");
    assert_eq!(first.plan.metrics.planned_orders, 2);
    assert!(
        first.plan.unplanned.is_empty(),
        "{:?}",
        first.plan.unplanned
    );
    let world = s.store.world().await.unwrap();
    let stored = s.store.orders(None, None, None).await.unwrap();
    let travel = HaversineTravelTime::default();
    assert_eq!(
        violations(&first.plan, &world, &stored, &[], &travel),
        Vec::<String>::new()
    );

    let latest = call(&s.router, "GET", "/plans/latest").await;
    assert_eq!(latest.status, StatusCode::OK);
    assert_eq!(latest.json::<VersionedPlan>(), first, "stored as returned");
    assert_eq!(
        call(&s.router, "GET", "/plans/1")
            .await
            .json::<VersionedPlan>(),
        first
    );
    assert_eq!(
        call(&s.router, "GET", "/plans/2").await.status,
        StatusCode::NOT_FOUND
    );
    let second: VersionedPlan = call(&s.router, "POST", "/plans").await.json();
    assert_eq!(second.version, 2);
}

#[tokio::test]
async fn orders_can_be_listed_filtered_and_fetched() {
    let early = order("BW-1", 42.5, at(9, 0));
    let late = Order {
        status: OrderStatus::Confirmed,
        ..order("BW-2", 18.0, at(14, 0))
    };
    let s = setup(&[late.clone(), early.clone()]).await;
    let get = |uri: String| {
        let router = s.router.clone();
        async move { call(&router, "GET", &uri).await }
    };

    let all = get("/orders".into()).await;
    assert_eq!(all.status, StatusCode::OK);
    assert_eq!(
        all.json::<Vec<Order>>(),
        [early.clone(), late.clone()],
        "by start"
    );
    assert_eq!(
        get("/orders?status=confirmed".into())
            .await
            .json::<Vec<Order>>(),
        std::slice::from_ref(&late)
    );
    let noon = at(12, 0).to_rfc3339_opts(SecondsFormat::Secs, true);
    assert_eq!(
        get(format!("/orders?from={noon}"))
            .await
            .json::<Vec<Order>>(),
        std::slice::from_ref(&late)
    );
    assert_eq!(
        get(format!("/orders?to={noon}")).await.json::<Vec<Order>>(),
        std::slice::from_ref(&early)
    );
    assert_eq!(
        get("/orders?status=bogus".into()).await.status,
        StatusCode::BAD_REQUEST
    );

    let one = get("/orders/10001001/BW-1".into()).await;
    assert_eq!(one.status, StatusCode::OK);
    assert_eq!(one.json::<Order>(), early);
    assert_eq!(
        get("/orders/10001001/nope".into()).await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn the_openapi_document_lists_every_route() {
    let s = setup(&[]).await;
    let doc = call(&s.router, "GET", "/api-docs/openapi.json").await;
    assert_eq!(doc.status, StatusCode::OK);
    let doc: Value = doc.json();
    let mut paths: Vec<&str> = doc["paths"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    paths.sort();
    assert_eq!(
        paths,
        [
            "/healthz",
            "/orders",
            "/orders/{customer}/{order_ref}",
            "/plans",
            "/plans/latest",
            "/plans/{version}",
        ]
    );
    for schema in ["Order", "Load", "VersionedPlan", "OrderStatus"] {
        assert!(
            doc["components"]["schemas"][schema].is_object(),
            "{schema} is documented"
        );
    }
    assert_eq!(
        call(&s.router, "GET", "/swagger-ui/").await.status,
        StatusCode::OK
    );
    assert_eq!(
        call(&s.router, "GET", "/healthz").await.status,
        StatusCode::OK
    );
}
