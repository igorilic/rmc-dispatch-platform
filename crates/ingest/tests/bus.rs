//! Against a real NATS JetStream container (SPEC §12: integration tests with testcontainers).

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use chrono::{TimeZone, Utc};
use common::{customers, nats, sample};
use domain::{Channel, Event, EventEnvelope, EventSource, Location, Seed, TruckId};
use ingest::mapping::{Delivery, Mapping};
use ingest::nats::{Bus, DLQ, EVENTS, Published};
use tracing::Instrument;

fn delivery<'a>(mapping: &Mapping, body: &'a [u8]) -> Delivery<'a> {
    let received_at = Utc.with_ymd_and_hms(2026, 11, 9, 9, 20, 0).unwrap();
    Delivery {
        body,
        channel: mapping.channel,
        reference: "test",
        received_at,
    }
}

const TTL: Duration = Duration::from_secs(3600);

#[tokio::test]
async fn an_order_delivered_twice_is_published_once_and_a_change_again() {
    let (_nats, url) = nats().await;
    let bus = Bus::connect(&url, TTL).await.unwrap();
    let (mapping, seed) = (
        Mapping::load(&customers(), "bauwerk-ag").unwrap(),
        Seed::rhine_neckar().unwrap(),
    );

    let body = sample("bauwerk-ag", "order.json");
    let first = bus
        .deliver(&mapping, &seed, &delivery(&mapping, &body))
        .await
        .unwrap();
    let retry = bus
        .deliver(&mapping, &seed, &delivery(&mapping, &body))
        .await
        .unwrap();
    assert_eq!((first.published, first.duplicates), (1, 0));
    assert_eq!((retry.published, retry.duplicates), (0, 1));
    assert_eq!(bus.count(EVENTS).await.unwrap(), 1);

    let updated = String::from_utf8(body)
        .unwrap()
        .replace("10:15:02", "10:22:40")
        .replace("42.5", "50.0");
    let change = bus
        .deliver(&mapping, &seed, &delivery(&mapping, updated.as_bytes()))
        .await
        .unwrap();
    assert_eq!(change.published, 1);
    assert_eq!(bus.count(EVENTS).await.unwrap(), 2);

    // The KV remembers keys beyond JetStream's 2-minute window, and across reconnects.
    let later = Bus::connect(&url, TTL).await.unwrap();
    let again = later
        .deliver(&mapping, &seed, &delivery(&mapping, updated.as_bytes()))
        .await
        .unwrap();
    assert_eq!(again.duplicates, 1);
    assert_eq!(later.count(EVENTS).await.unwrap(), 2);
}

#[tokio::test]
async fn a_repeated_snapshot_publishes_only_new_rows() {
    let (_nats, url) = nats().await;
    let bus = Bus::connect(&url, TTL).await.unwrap();
    let (mapping, seed) = (
        Mapping::load(&customers(), "rheinbau-gmbh").unwrap(),
        Seed::rhine_neckar().unwrap(),
    );

    let csv = sample("rheinbau-gmbh", "orders.csv");
    assert_eq!(
        bus.deliver(&mapping, &seed, &delivery(&mapping, &csv))
            .await
            .unwrap()
            .published,
        3
    );
    let next = bus
        .deliver(&mapping, &seed, &delivery(&mapping, &csv))
        .await
        .unwrap();
    assert_eq!((next.published, next.duplicates), (0, 3));
    assert_eq!(bus.count(EVENTS).await.unwrap(), 3);
}

#[tokio::test]
async fn telematics_duplicates_are_dropped_by_message_id() {
    let (_nats, url) = nats().await;
    let bus = Bus::connect(&url, TTL).await.unwrap();
    let at = Utc.with_ymd_and_hms(2026, 11, 9, 9, 20, 0).unwrap();
    let event = EventEnvelope {
        id: "truck-07:620:1794236511059".into(),
        occurred_at: at,
        received_at: at,
        source: EventSource {
            integration: "telematics".into(),
            channel: Channel::Mqtt,
            schema_version: None,
            reference: "trucks/truck-07/gps".into(),
        },
        event: Event::TruckPositionReported {
            truck: TruckId("truck-07".into()),
            location: Location {
                lat: 49.43,
                lon: 8.53,
            },
            speed_kmh: 37.5,
        },
    };
    assert_eq!(bus.publish(&event).await.unwrap(), Published::New);
    assert_eq!(bus.publish(&event).await.unwrap(), Published::Duplicate);
    assert_eq!(bus.count(EVENTS).await.unwrap(), 1);
}

#[tokio::test]
async fn poison_messages_land_in_the_dlq_with_every_error() {
    let (_nats, url) = nats().await;
    let bus = Bus::connect(&url, TTL).await.unwrap();
    let (mapping, seed) = (
        Mapping::load(&customers(), "rheinbau-gmbh").unwrap(),
        Seed::rhine_neckar().unwrap(),
    );

    let summary = bus
        .deliver(
            &mapping,
            &seed,
            &delivery(&mapping, &sample("rheinbau-gmbh", "broken.csv")),
        )
        .await
        .unwrap();
    assert_eq!((summary.published, summary.dead_lettered), (2, 1));

    let letters = bus.dead_letters().await.unwrap();
    assert_eq!(letters.len(), 1);
    let letter = &letters[0].letter;
    assert_eq!(letter.integration, "rheinbau-gmbh");
    assert_eq!(letter.record, "line 4 (RB-7790)");
    assert_eq!(letter.errors.len(), 3);
    assert!(
        letter.raw.starts_with("Auftragsnr;") && letter.raw.contains("RB-7790"),
        "{}",
        letter.raw
    );

    let counts = bus.dead_letter_counts().await.unwrap();
    assert_eq!(counts, [("rheinbau-gmbh".to_owned(), 1)].into());

    bus.remove_dead_letter(letters[0].sequence).await.unwrap();
    assert!(bus.dead_letters().await.unwrap().is_empty());
    assert_eq!(bus.count(DLQ).await.unwrap(), 0);
    assert!(bus.dead_letter_counts().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_published_event_carries_the_trace_of_its_publish_span() {
    let (_guard, exporter) = telemetry::testing::capture();
    let (_nats, url) = nats().await;
    let bus = Bus::connect(&url, TTL).await.unwrap();
    let (mapping, seed) = (
        Mapping::load(&customers(), "bauwerk-ag").unwrap(),
        Seed::rhine_neckar().unwrap(),
    );
    let body = sample("bauwerk-ag", "order.json");
    let delivery_span = tracing::info_span!("delivery");
    bus.deliver(&mapping, &seed, &delivery(&mapping, &body))
        .instrument(delivery_span)
        .await
        .unwrap();

    let js = async_nats::jetstream::new(async_nats::connect(&url).await.unwrap());
    let stored = js
        .get_stream(EVENTS)
        .await
        .unwrap()
        .get_raw_message(1)
        .await
        .unwrap();
    let traceparent = stored.headers.get("traceparent").unwrap().as_str();

    let spans = exporter.get_finished_spans().unwrap();
    let named = |name: &str| spans.iter().find(|s| s.name == name).unwrap();
    let (root, publish) = (named("delivery"), named("publish"));
    assert_eq!(publish.parent_span_id, root.span_context.span_id());
    let expected = format!(
        "00-{}-{}-01",
        publish.span_context.trace_id(),
        publish.span_context.span_id()
    );
    assert_eq!(traceparent, expected, "the consumer continues from publish");
}
