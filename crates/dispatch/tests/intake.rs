//! Order intake from EVENTS, against real NATS and Postgres containers. Events are published with
//! ingest's own `Bus`, exactly as in production.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use chrono::{DateTime, Utc};
use common::{at, nats, order, postgres};
use dispatch::store::Store;
use domain::{Channel, Event, EventEnvelope, EventSource, Order, OrderStatus, Seed, SiteId};
use ingest::nats::Bus;

fn upserted(order: &Order, occurred_at: DateTime<Utc>) -> EventEnvelope {
    envelope(
        occurred_at,
        Event::OrderUpserted {
            order: order.clone(),
            source_revision: None,
        },
    )
}

fn cancelled(order: &Order, occurred_at: DateTime<Utc>) -> EventEnvelope {
    let event = Event::OrderCancelled {
        order: order.id.clone(),
        customer: order.customer.clone(),
        source_revision: None,
    };
    envelope(occurred_at, event)
}

fn envelope(occurred_at: DateTime<Utc>, event: Event) -> EventEnvelope {
    EventEnvelope {
        id: format!("test:{}", occurred_at.timestamp_nanos_opt().unwrap()),
        occurred_at,
        received_at: occurred_at,
        source: EventSource {
            integration: "bauwerk-ag".into(),
            channel: Channel::Webhook,
            schema_version: None,
            reference: "test".into(),
        },
        event,
    }
}

/// Polls until `check` holds for the stored order, or fails after 10 s.
async fn eventually(store: &Store, id: &str, check: impl Fn(Option<Order>) -> bool) {
    for _ in 0..100 {
        if check(store.order(id).await.unwrap()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "{id} never reached the expected state: {:?}",
        store.order(id).await.unwrap()
    );
}

struct Setup {
    _nats: testcontainers::ContainerAsync<testcontainers::GenericImage>,
    _pg: testcontainers::ContainerAsync<testcontainers::GenericImage>,
    bus: Bus,
    store: Store,
    js: async_nats::jetstream::Context,
}

async fn setup() -> Setup {
    let ((nats_container, nats_url), (pg_container, pg_url)) = tokio::join!(nats(), postgres());
    let bus = Bus::connect(&nats_url, Duration::from_secs(3600))
        .await
        .unwrap();
    let store = Store::connect(&pg_url).await.unwrap();
    store
        .import_master_data(&Seed::rhine_neckar().unwrap())
        .await
        .unwrap();
    let js = async_nats::jetstream::new(async_nats::connect(&nats_url).await.unwrap());
    Setup {
        _nats: nats_container,
        _pg: pg_container,
        bus,
        store,
        js,
    }
}

#[tokio::test]
async fn orders_follow_events_and_stale_ones_are_ignored() {
    let s = setup().await;
    let intake = tokio::spawn(dispatch::intake::run(s.js.clone(), s.store.clone()));
    let o = order("BW-1", 42.5, at(9, 0));
    let id = o.id.0.clone();

    s.bus.publish(&upserted(&o, at(6, 0))).await.unwrap();
    eventually(&s.store, &id, |stored| {
        stored.is_some_and(|x| x.total_volume_m3 == 42.5)
    })
    .await;

    let bigger = Order {
        total_volume_m3: 50.0,
        ..o.clone()
    };
    s.bus.publish(&upserted(&bigger, at(6, 5))).await.unwrap();
    eventually(&s.store, &id, |stored| {
        stored.is_some_and(|x| x.total_volume_m3 == 50.0)
    })
    .await;

    // A late, stale revision, then a marker: once the marker is in, the stale one has been seen.
    let stale = Order {
        total_volume_m3: 30.0,
        ..o.clone()
    };
    s.bus.publish(&upserted(&stale, at(6, 1))).await.unwrap();
    let marker = order("BW-marker", 9.0, at(9, 0));
    s.bus.publish(&upserted(&marker, at(6, 6))).await.unwrap();
    eventually(&s.store, &marker.id.0, |stored| stored.is_some()).await;
    assert_eq!(
        s.store.order(&id).await.unwrap().unwrap().total_volume_m3,
        50.0,
        "stale revision ignored"
    );

    s.bus.publish(&cancelled(&o, at(6, 10))).await.unwrap();
    eventually(&s.store, &id, |stored| {
        stored.is_some_and(|x| x.status == OrderStatus::Cancelled)
    })
    .await;
    intake.abort();
}

#[tokio::test]
async fn an_event_that_can_never_apply_does_not_block_the_rest() {
    let s = setup().await;
    let intake = tokio::spawn(dispatch::intake::run(s.js.clone(), s.store.clone()));
    let unknown_site = Order {
        site: SiteId("59999999".into()),
        ..order("BW-poison", 9.0, at(9, 0))
    };
    s.bus
        .publish(&upserted(&unknown_site, at(6, 0)))
        .await
        .unwrap();
    let fine = order("BW-fine", 9.0, at(9, 0));
    s.bus.publish(&upserted(&fine, at(6, 1))).await.unwrap();

    eventually(&s.store, &fine.id.0, |stored| stored.is_some()).await;
    assert_eq!(s.store.order(&unknown_site.id.0).await.unwrap(), None);
    intake.abort();
}

#[tokio::test]
async fn events_published_while_dispatch_is_down_are_picked_up_on_restart() {
    let s = setup().await;
    let first = tokio::spawn(dispatch::intake::run(s.js.clone(), s.store.clone()));
    let before = order("BW-before", 9.0, at(9, 0));
    s.bus.publish(&upserted(&before, at(6, 0))).await.unwrap();
    eventually(&s.store, &before.id.0, |stored| stored.is_some()).await;
    first.abort();

    let during = order("BW-during", 9.0, at(9, 0));
    s.bus.publish(&upserted(&during, at(6, 1))).await.unwrap();
    let second = tokio::spawn(dispatch::intake::run(s.js.clone(), s.store.clone()));
    eventually(&s.store, &during.id.0, |stored| stored.is_some()).await;
    second.abort();
}
