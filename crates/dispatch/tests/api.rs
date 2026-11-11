//! The REST API, driven through the router against a real Postgres container.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

mod common;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use common::{at, order, postgres};
use dispatch::api::{self, Redispatched, VersionedPlan};
use dispatch::redispatch::{ChangeKind, Diff, Settings};
use dispatch::search::Budget;
use dispatch::store::{Store, Ticket};
use dispatch::travel::HaversineTravelTime;
use dispatch::verify::violations;
use dispatch::worker;
use domain::{
    DeliveryStatus, Load, LoadRef, Order, OrderStatus, PlantId, PlantPlan, PlantStatus, Seed,
};
use serde_json::{Value, json};
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
    let router = api::router(store.clone(), jobs, clock);
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
    send(router, method, uri, None).await
}

async fn send(router: &Router, method: &str, uri: &str, body: Option<Value>) -> Reply {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
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
            "/plans/{version}/diff",
            "/plants/{id}/plan",
            "/plants/{id}/status",
            "/sites/{id}/delays",
        ]
    );
    for schema in [
        "Order",
        "Load",
        "VersionedPlan",
        "OrderStatus",
        "Diff",
        "Trigger",
        "Redispatched",
        "PlantPlan",
    ] {
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

/// Checks a stored version against the hard constraints, with the store's current world and orders.
async fn assert_valid(s: &Setup, plan: &VersionedPlan) {
    let world = s.store.world().await.unwrap();
    let stored = s.store.orders(None, None, None).await.unwrap();
    let found = violations(
        &plan.plan,
        &world,
        &stored,
        &[],
        &HaversineTravelTime::default(),
    );
    assert_eq!(found, Vec::<String>::new(), "version {}", plan.version);
}

#[tokio::test]
async fn a_plant_going_offline_re_dispatches_and_says_why() {
    let s = setup(&[order("BW-1", 42.5, at(9, 0))]).await;
    let v1: VersionedPlan = call(&s.router, "POST", "/plans").await.json();
    let plant = v1.plan.loads[0].planned_plant.clone();
    let uri = format!("/plants/{}/status", plant.0);

    let offline = send(&s.router, "PUT", &uri, Some(json!({"status": "offline"}))).await;
    assert_eq!(offline.status, StatusCode::OK);
    let done: Redispatched = offline.json();
    assert_eq!(done.version, Some(v1.version + 1));
    let diff = done.diff.unwrap();
    assert_eq!(diff.triggers, [format!("{} offline", plant.0)]);
    assert!(
        diff.changes
            .iter()
            .any(|c| c.reason == format!("{} is offline", plant.0))
    );

    let v2: VersionedPlan = call(&s.router, "GET", "/plans/latest").await.json();
    assert!(v2.plan.loads.iter().all(|l| l.planned_plant != plant));
    assert_valid(&s, &v2).await;
    let stored: Diff = call(&s.router, "GET", &format!("/plans/{}/diff", v2.version))
        .await
        .json();
    assert_eq!(stored, diff);
    let first: Diff = call(&s.router, "GET", "/plans/1/diff").await.json();
    assert!(first.changes.iter().all(|c| c.kind == ChangeKind::Added));
    assert_eq!(
        call(&s.router, "GET", "/plans/99/diff").await.status,
        StatusCode::NOT_FOUND
    );

    let again = send(&s.router, "PUT", &uri, Some(json!({"status": "offline"}))).await;
    assert_eq!(
        again.json::<Redispatched>(),
        Redispatched {
            version: None,
            diff: None
        },
        "no change, no re-dispatch"
    );
    let unknown = send(
        &s.router,
        "PUT",
        "/plants/plant-nowhere/status",
        Some(json!({"status": "offline"})),
    )
    .await;
    assert_eq!(unknown.status, StatusCode::NOT_FOUND);
    let world = s.store.world().await.unwrap();
    assert_eq!(
        world.plants.iter().find(|p| p.id == plant).unwrap().status,
        PlantStatus::Offline
    );
}

#[tokio::test]
async fn a_site_delay_moves_todays_pours_there_and_leaves_tomorrow_alone() {
    let today = order("BW-1", 18.0, at(9, 0));
    let tomorrow = order("BW-2", 18.0, at(9, 0) + TimeDelta::days(1));
    let s = setup(&[today.clone(), tomorrow.clone()]).await;
    let v1: VersionedPlan = call(&s.router, "POST", "/plans").await.json();
    let loads_of = |p: &VersionedPlan, o: &Order| {
        p.plan
            .loads
            .iter()
            .filter(|l| l.order == o.id)
            .cloned()
            .collect::<Vec<_>>()
    };

    let uri = format!("/sites/{}/delays", today.site.0);
    let delayed = send(&s.router, "POST", &uri, Some(json!({"minutes": 30}))).await;
    assert_eq!(delayed.status, StatusCode::OK);
    let diff = delayed.json::<Redispatched>().diff.unwrap();
    assert_eq!(
        diff.triggers,
        [format!("site {} delayed 30 min", today.site.0)]
    );

    let v2: VersionedPlan = call(&s.router, "GET", "/plans/latest").await.json();
    assert!(loads_of(&v2, &today)[0].eta_on_site >= at(9, 30));
    // Tomorrow's pour isn't delayed. Local search may still improve it, and says so.
    assert_eq!(
        loads_of(&v2, &tomorrow)[0].eta_on_site,
        loads_of(&v1, &tomorrow)[0].eta_on_site
    );
    let delay_reason = format!("site {} is delayed 30 min", today.site.0);
    for c in diff.changes.iter().filter(|c| c.order == tomorrow.id) {
        assert_ne!(c.reason, delay_reason);
    }
    assert_valid(&s, &v2).await;
    let stored: Order = call(&s.router, "GET", "/orders/10001001/BW-1").await.json();
    assert_eq!(
        stored.requested_start,
        at(9, 0),
        "the customer's start stays as sent"
    );

    // Delays add up.
    send(&s.router, "POST", &uri, Some(json!({"minutes": 15}))).await;
    let v3: VersionedPlan = call(&s.router, "GET", "/plans/latest").await.json();
    assert!(loads_of(&v3, &today)[0].eta_on_site >= at(9, 45));

    let bad = send(&s.router, "POST", &uri, Some(json!({"minutes": 0}))).await;
    assert_eq!(bad.status, StatusCode::UNPROCESSABLE_ENTITY);
    let unknown = send(
        &s.router,
        "POST",
        "/sites/59999999/delays",
        Some(json!({"minutes": 30})),
    )
    .await;
    assert_eq!(unknown.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_plant_gets_its_slice_of_the_latest_plan() {
    let s = setup(&[
        order("BW-1", 42.5, at(9, 0)),
        order("BW-2", 18.0, at(10, 30)),
    ])
    .await;
    let before: PlantPlan = call(&s.router, "GET", "/plants/plant-walldorf/plan")
        .await
        .json();
    assert_eq!(
        before,
        PlantPlan {
            plant: PlantId("plant-walldorf".into()),
            version: None,
            created_at: None,
            loads: Vec::new(),
        },
        "no plan yet"
    );

    let v1: VersionedPlan = call(&s.router, "POST", "/plans").await.json();
    let world = s.store.world().await.unwrap();
    for plant in &world.plants {
        let reply = call(&s.router, "GET", &format!("/plants/{}/plan", plant.id.0)).await;
        assert_eq!(reply.status, StatusCode::OK);
        let slice: PlantPlan = reply.json();
        assert_eq!(slice.plant, plant.id);
        assert_eq!(slice.version, Some(v1.version));
        assert_eq!(slice.created_at, Some(v1.plan.created_at));

        let mut expected: Vec<Load> = v1
            .plan
            .loads
            .iter()
            .filter(|l| l.planned_plant == plant.id)
            .cloned()
            .collect();
        expected.sort_by_key(|l| (l.planned_batch_at, l.order.clone(), l.sequence));
        let loads: Vec<Load> = slice.loads.iter().map(|l| l.load.clone()).collect();
        assert_eq!(loads, expected, "{}'s loads, by batch time", plant.id.0);
        assert!(slice.loads.iter().all(|l| l.mix.0 == "C30/37-XC4"));
    }
    assert!(!v1.plan.loads.is_empty());

    assert_eq!(
        call(&s.router, "GET", "/plants/plant-nowhere/plan")
            .await
            .status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn an_order_with_a_delivery_is_in_progress() {
    let o = order("BW-1", 18.0, at(9, 0));
    let s = setup(std::slice::from_ref(&o)).await;
    let load = LoadRef {
        order: o.id.clone(),
        sequence: 1,
    };
    let ticket = Ticket {
        load: &load,
        plant: &domain::PlantId("plant-walldorf".into()),
        truck: &domain::TruckId("truck-07".into()),
        volume_m3: 9.0,
        ticket_no: "WAL-000001",
        event: DeliveryStatus::Batching,
        at: at(8, 30),
    };
    s.store.record_ticket(&ticket).await.unwrap();

    let one: Order = call(&s.router, "GET", "/orders/10001001/BW-1").await.json();
    assert_eq!(one.status, OrderStatus::InProgress);
    let in_progress: Vec<Order> = call(&s.router, "GET", "/orders?status=in_progress")
        .await
        .json();
    assert_eq!(in_progress.len(), 1);
    let pending: Vec<Order> = call(&s.router, "GET", "/orders?status=pending")
        .await
        .json();
    assert!(pending.is_empty());
}
