//! The Postgres store against a real Postgres container.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

mod common;

use chrono::TimeDelta;
use common::{at, order, postgres};
use dispatch::planner::{World, plan};
use dispatch::store::Store;
use dispatch::travel::HaversineTravelTime;
use domain::{OrderStatus, Seed};

#[tokio::test]
async fn master_data_imports_idempotently_and_every_site_is_located() {
    let (_pg, url) = postgres().await;
    let store = Store::connect(&url).await.unwrap();
    let seed = Seed::rhine_neckar().unwrap();
    store.import_master_data(&seed).await.unwrap();
    store.import_master_data(&seed).await.unwrap();

    let world = store.world().await.unwrap();
    assert_eq!(
        (
            world.plants.len(),
            world.trucks.len(),
            world.sites.len(),
            world.mixes.len()
        ),
        (3, 25, 40, 4)
    );
    let mut expected = World::from_seed(&seed);
    assert_eq!(
        world.sites, expected.sites,
        "address-only ship-tos are placed by postal code"
    );
    // The store returns master data sorted by id; the seed file has its own order.
    expected.plants.sort_by(|a, b| a.id.cmp(&b.id));
    expected.trucks.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(world.plants, expected.plants);
    assert_eq!(world.trucks, expected.trucks);
    assert_eq!(world.mixes, expected.mixes);
}

#[tokio::test]
async fn an_older_event_never_overwrites_a_newer_one() {
    let (_pg, url) = postgres().await;
    let store = Store::connect(&url).await.unwrap();
    store
        .import_master_data(&Seed::rhine_neckar().unwrap())
        .await
        .unwrap();
    let original = order("BW-1", 42.5, at(9, 0));
    let id = original.id.0.clone();

    assert!(store.upsert_order(&original, at(6, 0), None).await.unwrap());
    let bigger = domain::Order {
        total_volume_m3: 50.0,
        ..original.clone()
    };
    assert!(
        store
            .upsert_order(&bigger, at(6, 5), Some("2"))
            .await
            .unwrap()
    );
    assert!(
        !store.upsert_order(&original, at(6, 1), None).await.unwrap(),
        "stale: ignored"
    );
    assert_eq!(store.order(&id).await.unwrap().unwrap(), bigger);

    assert!(
        !store.cancel_order(&id, at(6, 2)).await.unwrap(),
        "stale cancellation: ignored"
    );
    assert!(store.cancel_order(&id, at(6, 10)).await.unwrap());
    assert_eq!(
        store.order(&id).await.unwrap().unwrap().status,
        OrderStatus::Cancelled
    );
    assert!(
        !store
            .cancel_order("10001001/never-seen", at(6, 10))
            .await
            .unwrap()
    );
    assert_eq!(store.order("10001001/never-seen").await.unwrap(), None);
}

#[tokio::test]
async fn orders_are_listed_by_status_and_start() {
    let (_pg, url) = postgres().await;
    let store = Store::connect(&url).await.unwrap();
    store
        .import_master_data(&Seed::rhine_neckar().unwrap())
        .await
        .unwrap();
    for (n, start) in [("A", at(9, 0)), ("B", at(7, 0)), ("C", at(15, 0))] {
        store
            .upsert_order(&order(n, 9.0, start), at(6, 0), None)
            .await
            .unwrap();
    }
    store.cancel_order("10001001/C", at(6, 1)).await.unwrap();

    let ids = |orders: Vec<domain::Order>| orders.into_iter().map(|o| o.id.0).collect::<Vec<_>>();
    assert_eq!(
        ids(store.orders(None, None, None).await.unwrap()),
        ["10001001/B", "10001001/A", "10001001/C"]
    );
    let cancelled = store
        .orders(Some(OrderStatus::Cancelled), None, None)
        .await
        .unwrap();
    assert_eq!(ids(cancelled), ["10001001/C"]);
    let morning = store
        .orders(None, Some(at(8, 0)), Some(at(12, 0)))
        .await
        .unwrap();
    assert_eq!(ids(morning), ["10001001/A"]);
    assert_eq!(
        ids(store.open_orders(at(0, 0), at(23, 0)).await.unwrap()),
        ["10001001/B", "10001001/A"]
    );
}

#[tokio::test]
async fn every_plan_is_stored_as_a_new_version() {
    let (_pg, url) = postgres().await;
    let store = Store::connect(&url).await.unwrap();
    store
        .import_master_data(&Seed::rhine_neckar().unwrap())
        .await
        .unwrap();
    for (n, volume) in [("A", 42.5), ("B", 18.0)] {
        store
            .upsert_order(&order(n, volume, at(9, 0)), at(6, 0), None)
            .await
            .unwrap();
    }
    assert_eq!(store.plan(None).await.unwrap(), None, "no plan yet");

    let now = at(5, 0);
    let world = store.world().await.unwrap();
    let open = store
        .open_orders(now, now + TimeDelta::hours(48))
        .await
        .unwrap();
    let mut made = plan(
        &world,
        &open,
        now,
        TimeDelta::hours(48),
        &HaversineTravelTime::default(),
    );
    assert!(!made.loads.is_empty());
    let first = store.save_plan(&made).await.unwrap();
    let second = store.save_plan(&made).await.unwrap();
    assert_eq!(second, first + 1);

    made.loads
        .sort_by(|a, b| (&a.order.0, a.sequence).cmp(&(&b.order.0, b.sequence)));
    assert_eq!(
        store.plan(None).await.unwrap(),
        Some((second, made.clone())),
        "latest"
    );
    assert_eq!(store.plan(Some(first)).await.unwrap(), Some((first, made)));
    assert_eq!(store.plan(Some(first + 99)).await.unwrap(), None);
}
