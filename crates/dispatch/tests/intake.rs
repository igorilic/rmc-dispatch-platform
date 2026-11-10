//! Event intake and re-dispatch from EVENTS, against real NATS and Postgres containers. Events
//! are published with ingest's own `Bus`, exactly as in production.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use common::{at, nats, order, postgres};
use dispatch::redispatch::{Settings, Trigger};
use dispatch::search::Budget;
use dispatch::store::{Store, VersionedPlan};
use dispatch::worker::{self, Jobs};
use domain::{
    Channel, DeliveryStatus, Event, EventEnvelope, EventSource, LoadRef, MixCode, Order,
    OrderStatus, Seed, SiteId, TruckId, TruckStatus,
};
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
    /// Triggers pile up here unless a test runs the worker.
    jobs: Jobs,
    queue: Option<tokio::sync::mpsc::Receiver<worker::Job>>,
}

impl Setup {
    fn intake(&self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(dispatch::intake::run(
            self.js.clone(),
            self.store.clone(),
            self.jobs.clone(),
        ))
    }

    /// Both intakes and the re-dispatch worker, planning at 06:00.
    fn dispatch(&mut self) -> Vec<tokio::task::JoinHandle<()>> {
        let settings = Settings {
            budget: Budget::moves(50),
            ..Settings::default()
        };
        let queue = self.queue.take().unwrap();
        vec![
            self.intake(),
            tokio::spawn(dispatch::intake::run_fleet(
                self.js.clone(),
                self.store.clone(),
                self.jobs.clone(),
            )),
            tokio::spawn(worker::run(self.store.clone(), queue, settings, || {
                at(6, 0)
            })),
        ]
    }

    /// Polls until the latest plan satisfies `check`, or fails after 10 s.
    async fn plan_where(&self, check: impl Fn(&VersionedPlan) -> bool) -> VersionedPlan {
        for _ in 0..100 {
            if let Some(plan) = self.store.plan(None).await.unwrap()
                && check(&plan)
            {
                return plan;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!(
            "no such plan; latest: {:#?}",
            self.store.plan(None).await.unwrap()
        );
    }
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
    let (jobs, queue) = worker::channel();
    Setup {
        _nats: nats_container,
        _pg: pg_container,
        bus,
        store,
        js,
        jobs,
        queue: Some(queue),
    }
}

#[tokio::test]
async fn orders_follow_events_and_stale_ones_are_ignored() {
    let s = setup().await;
    let intake = s.intake();
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
    let intake = s.intake();
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
    let first = s.intake();
    let before = order("BW-before", 9.0, at(9, 0));
    s.bus.publish(&upserted(&before, at(6, 0))).await.unwrap();
    eventually(&s.store, &before.id.0, |stored| stored.is_some()).await;
    first.abort();

    let during = order("BW-during", 9.0, at(9, 0));
    s.bus.publish(&upserted(&during, at(6, 1))).await.unwrap();
    let second = s.intake();
    eventually(&s.store, &during.id.0, |stored| stored.is_some()).await;
    second.abort();
}

fn telematics(occurred_at: DateTime<Utc>, event: Event) -> EventEnvelope {
    EventEnvelope {
        source: EventSource {
            integration: "telematics".into(),
            channel: Channel::Mqtt,
            schema_version: None,
            reference: "test".into(),
        },
        ..envelope(occurred_at, event)
    }
}

#[tokio::test]
async fn a_truck_breakdown_reported_by_telematics_moves_its_loads() {
    let mut s = setup().await;
    let tasks = s.dispatch();
    let o = order("BW-1", 42.5, at(9, 0));
    s.bus.publish(&upserted(&o, at(5, 0))).await.unwrap();
    let v1 = s.plan_where(|p| p.plan.loads.len() >= 3).await;
    assert_eq!(
        v1.triggers,
        [Trigger::NewOrder {
            order: o.id.clone()
        }]
    );

    let broken = v1.plan.loads[1].planned_truck.clone();
    let status = Event::TruckStatusReported {
        truck: broken.clone(),
        status: TruckStatus::OutOfService,
    };
    s.bus.publish(&telematics(at(5, 50), status)).await.unwrap();
    let v2 = s.plan_where(|p| p.version > v1.version).await;
    assert_eq!(v2.previous_version, Some(v1.version));
    assert_eq!(
        v2.triggers,
        [Trigger::TruckOutOfService {
            truck: broken.clone()
        }]
    );
    assert!(v2.plan.loads.iter().all(|l| l.planned_truck != broken));

    // Back in service, and a stale breakdown arriving late: neither triggers anything.
    let back = Event::TruckStatusReported {
        truck: broken.clone(),
        status: TruckStatus::Available,
    };
    s.bus.publish(&telematics(at(5, 55), back)).await.unwrap();
    let stale = Event::TruckStatusReported {
        truck: broken.clone(),
        status: TruckStatus::OutOfService,
    };
    s.bus.publish(&telematics(at(5, 52), stale)).await.unwrap();
    let marker = order("BW-marker", 9.0, at(11, 0));
    s.bus.publish(&upserted(&marker, at(5, 56))).await.unwrap();
    let v3 = s
        .plan_where(|p| p.plan.loads.iter().any(|l| l.order == marker.id))
        .await;
    assert_eq!(
        v3.triggers,
        [Trigger::NewOrder {
            order: marker.id.clone()
        }]
    );
    let world = s.store.world().await.unwrap();
    let truck = world.trucks.iter().find(|t| t.id == broken).unwrap();
    assert_eq!(
        truck.status,
        TruckStatus::Available,
        "the stale report was ignored"
    );
    tasks.iter().for_each(tokio::task::JoinHandle::abort);
}

#[tokio::test]
async fn a_batch_ticket_freezes_its_load() {
    let mut s = setup().await;
    let tasks = s.dispatch();
    let o = order("BW-1", 42.5, at(9, 0));
    s.bus.publish(&upserted(&o, at(5, 0))).await.unwrap();
    let v1 = s.plan_where(|p| p.plan.loads.len() >= 3).await;
    let first = &v1.plan.loads[0];

    // The plant starts batching load 1 five minutes late.
    let started = first.planned_batch_at + TimeDelta::minutes(5);
    let ticket = Event::BatchTicketIssued {
        plant: first.planned_plant.clone(),
        ticket_no: "WAL-000001".into(),
        truck: first.planned_truck.clone(),
        event: DeliveryStatus::Batching,
        mix: MixCode("C30/37-XC4".into()),
        volume_m3: first.volume_m3,
        load: Some(LoadRef {
            order: o.id.clone(),
            sequence: 1,
        }),
    };
    s.bus.publish(&telematics(started, ticket)).await.unwrap();
    // A ticket for a trip outside the plan changes nothing.
    let unplanned_trip = Event::BatchTicketIssued {
        plant: first.planned_plant.clone(),
        ticket_no: "WAL-000002".into(),
        truck: TruckId("truck-01".into()),
        event: DeliveryStatus::Batching,
        mix: MixCode("C30/37-XC4".into()),
        volume_m3: 9.0,
        load: None,
    };
    s.bus
        .publish(&telematics(started, unplanned_trip))
        .await
        .unwrap();
    for _ in 0..100 {
        if !s.store.deliveries(at(0, 0)).await.unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(s.store.deliveries(at(0, 0)).await.unwrap().len(), 1);

    s.bus.publish(&cancelled(&o, at(5, 30))).await.unwrap();
    let v2 = s.plan_where(|p| p.version > v1.version).await;
    assert_eq!(
        v2.triggers,
        [Trigger::OrderCancelled {
            order: o.id.clone()
        }]
    );
    assert_eq!(v2.plan.loads.len(), 1, "only the batching load stays");
    let frozen = &v2.plan.loads[0];
    assert!(frozen.frozen);
    assert_eq!((frozen.sequence, frozen.planned_batch_at), (1, started));
    tasks.iter().for_each(tokio::task::JoinHandle::abort);
}
