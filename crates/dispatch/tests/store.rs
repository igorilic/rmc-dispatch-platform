//! The Postgres store against a real Postgres container.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

mod common;

use std::collections::HashMap;

use chrono::TimeDelta;
use common::{at, order, postgres};
use dispatch::planner::{World, plan};
use dispatch::redispatch::{Trigger, diff};
use dispatch::store::{Store, Ticket, Upsert, VersionedPlan};
use dispatch::travel::HaversineTravelTime;
use domain::{
    DeliveryStatus, LoadRef, Location, OrderId, OrderStatus, PlantId, Seed, TruckId, TruckStatus,
};

async fn seeded() -> (
    testcontainers::ContainerAsync<testcontainers::GenericImage>,
    Store,
) {
    let (pg, url) = postgres().await;
    let store = Store::connect(&url).await.unwrap();
    store
        .import_master_data(&Seed::rhine_neckar().unwrap())
        .await
        .unwrap();
    (pg, store)
}

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

    assert_eq!(
        store.upsert_order(&original, at(6, 0), None).await.unwrap(),
        Upsert::New
    );
    let bigger = domain::Order {
        total_volume_m3: 50.0,
        ..original.clone()
    };
    assert_eq!(
        store
            .upsert_order(&bigger, at(6, 5), Some("2"))
            .await
            .unwrap(),
        Upsert::Changed(original.clone()),
        "says what it replaced"
    );
    assert_eq!(
        store.upsert_order(&original, at(6, 1), None).await.unwrap(),
        Upsert::Stale,
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
        ids(store
            .planning_orders(at(0, 0), at(23, 0), &[])
            .await
            .unwrap()),
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
        .planning_orders(now, now + TimeDelta::hours(48), &[])
        .await
        .unwrap();
    let mut made = plan(
        &world,
        &open,
        &[],
        now,
        TimeDelta::hours(48),
        &HaversineTravelTime::default(),
    );
    assert!(!made.loads.is_empty());
    made.loads[0].frozen = true;
    let changes = diff(None, &made, &open, &[], &HashMap::new());
    let first = store.save_plan(&made, None, &[], &changes).await.unwrap();
    let triggers = [Trigger::FullReplan];
    let second = store
        .save_plan(&made, Some(first), &triggers, &changes)
        .await
        .unwrap();
    assert_eq!(second, first + 1);

    made.loads
        .sort_by(|a, b| (&a.order.0, a.sequence).cmp(&(&b.order.0, b.sequence)));
    let stored = |version, previous_version, triggers: &[Trigger]| VersionedPlan {
        version,
        previous_version,
        triggers: triggers.to_vec(),
        plan: made.clone(),
    };
    assert_eq!(
        store.plan(None).await.unwrap(),
        Some(stored(second, Some(first), &triggers)),
        "latest, with what it replaced and why"
    );
    assert_eq!(
        store.plan(Some(first)).await.unwrap(),
        Some(stored(first, None, &[]))
    );
    assert_eq!(store.plan(Some(first + 99)).await.unwrap(), None);
}

#[tokio::test]
async fn truck_reports_keep_the_newest_and_reach_the_planner() {
    let (_pg, store) = seeded().await;
    let truck = TruckId("truck-07".into());
    assert_eq!(
        store
            .report_truck_status(&truck, TruckStatus::OutOfService, at(6, 5))
            .await
            .unwrap(),
        Some(TruckStatus::Available),
        "says what it replaced"
    );
    assert_eq!(
        store
            .report_truck_status(&truck, TruckStatus::Available, at(6, 1))
            .await
            .unwrap(),
        None,
        "stale: ignored"
    );
    let here = Location {
        lat: 49.41,
        lon: 8.69,
    };
    assert!(
        store
            .report_truck_position(&truck, here, at(6, 5))
            .await
            .unwrap()
    );
    let elsewhere = Location {
        lat: 49.0,
        lon: 8.0,
    };
    assert!(
        !store
            .report_truck_position(&truck, elsewhere, at(6, 4))
            .await
            .unwrap()
    );

    let world = store.world().await.unwrap();
    let t = world.trucks.iter().find(|t| t.id == truck).unwrap();
    assert_eq!(
        (t.status, t.last_position),
        (TruckStatus::OutOfService, Some(here))
    );

    // Back in service once a newer report says so; the import keeps operational status.
    store
        .report_truck_status(&truck, TruckStatus::Available, at(6, 30))
        .await
        .unwrap();
    store
        .import_master_data(&Seed::rhine_neckar().unwrap())
        .await
        .unwrap();
    let world = store.world().await.unwrap();
    let t = world.trucks.iter().find(|t| t.id == truck).unwrap();
    assert_eq!(t.status, TruckStatus::Available);
}

#[tokio::test]
async fn tickets_make_deliveries_whose_orders_are_always_planned() {
    let (_pg, store) = seeded().await;
    let o = order("BW-1", 18.0, at(9, 0));
    store.upsert_order(&o, at(5, 0), None).await.unwrap();
    let load = LoadRef {
        order: o.id.clone(),
        sequence: 1,
    };
    let (plant, truck) = (PlantId("plant-walldorf".into()), TruckId("truck-07".into()));
    let ticket = |event, at| Ticket {
        load: &load,
        plant: &plant,
        truck: &truck,
        volume_m3: 9.0,
        ticket_no: "WAL-000001",
        event,
        at,
    };
    assert!(
        store
            .record_ticket(&ticket(DeliveryStatus::Batching, at(8, 20)))
            .await
            .unwrap(),
        "new delivery"
    );
    assert!(
        !store
            .record_ticket(&ticket(DeliveryStatus::Loading, at(8, 25)))
            .await
            .unwrap()
    );
    // A duplicate with another time: the first report of a status wins.
    store
        .record_ticket(&ticket(DeliveryStatus::Batching, at(8, 21)))
        .await
        .unwrap();

    let deliveries = store.deliveries(at(8, 0)).await.unwrap();
    assert_eq!(deliveries.len(), 1);
    let d = &deliveries[0];
    assert_eq!((&d.order, d.sequence, &d.truck), (&o.id, 1, &truck));
    assert_eq!(
        d.events,
        [
            (DeliveryStatus::Batching, at(8, 20)),
            (DeliveryStatus::Loading, at(8, 25))
        ]
        .into()
    );
    assert!(store.deliveries(at(9, 0)).await.unwrap().is_empty());

    // Outside the planning window, an order with a delivery is still listed when asked for.
    let late = at(12, 0);
    let ids = |orders: Vec<domain::Order>| orders.into_iter().map(|o| o.id).collect::<Vec<_>>();
    let window = (late, late + TimeDelta::hours(48));
    assert!(
        ids(store
            .planning_orders(window.0, window.1, &[])
            .await
            .unwrap())
        .is_empty()
    );
    assert_eq!(
        ids(store
            .planning_orders(window.0, window.1, std::slice::from_ref(&o.id.0))
            .await
            .unwrap()),
        [OrderId(o.id.0.clone())]
    );

    let unknown = LoadRef {
        order: OrderId("10001001/never-seen".into()),
        sequence: 1,
    };
    let orphan = Ticket {
        load: &unknown,
        ..ticket(DeliveryStatus::Batching, at(8, 20))
    };
    assert!(
        store.record_ticket(&orphan).await.is_err(),
        "a ticket for an unknown order is refused"
    );
}
