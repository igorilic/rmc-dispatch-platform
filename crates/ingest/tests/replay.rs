//! Dead letters are replayed with the current mapping after a fix (SPEC §5).

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use chrono::Utc;
use common::{customers, nats, sample};
use domain::Seed;
use ingest::mapping::{Delivery, Mapping};
use ingest::nats::{Bus, DLQ, EVENTS};
use ingest::replay;

#[tokio::test]
async fn a_dead_letter_is_replayed_once_the_mapping_is_fixed() {
    let (_nats, url) = nats().await;
    let bus = Bus::connect(&url, Duration::from_secs(3600)).await.unwrap();
    let seed = Seed::rhine_neckar().unwrap();

    // Someone typo'd the v2 ship-to attribute, so v2 orders can't be mapped.
    let fixed_yaml =
        std::fs::read_to_string(customers().join("neckar-infra/mapping.yaml")).unwrap();
    let broken = Mapping::parse(
        "neckar-infra",
        &fixed_yaml.replace("/Order/Delivery/@shipTo", "/Order/Delivery/@shipto"),
    )
    .unwrap();
    let body = sample("neckar-infra", "order-v2.xml");
    let delivery = Delivery {
        body: &body,
        channel: broken.channel,
        reference: "webhook",
        received_at: Utc::now(),
    };
    assert_eq!(
        bus.deliver(&broken, &seed, &delivery)
            .await
            .unwrap()
            .dead_lettered,
        1
    );

    // Replaying with the same broken mapping keeps the letter and says why.
    let report = replay(&bus, &[broken], &seed, None).await.unwrap();
    assert_eq!(report.replayed, 0);
    assert_eq!(report.still_failing.len(), 1);
    assert_eq!(
        report.still_failing[0].errors,
        ["/Order/Delivery/@shipto: missing"]
    );
    assert_eq!(bus.count(DLQ).await.unwrap(), 1);

    // After the fix, the letter becomes an event and leaves the DLQ.
    let fixed = Mapping::parse("neckar-infra", &fixed_yaml).unwrap();
    let report = replay(&bus, &[fixed], &seed, Some("neckar-infra"))
        .await
        .unwrap();
    assert_eq!((report.replayed, report.still_failing.len()), (1, 0));
    assert_eq!(bus.count(EVENTS).await.unwrap(), 1);
    assert!(bus.dead_letters().await.unwrap().is_empty());
}

#[tokio::test]
async fn replay_can_be_limited_to_one_integration() {
    let (_nats, url) = nats().await;
    let bus = Bus::connect(&url, Duration::from_secs(3600)).await.unwrap();
    let seed = Seed::rhine_neckar().unwrap();
    let mappings = Mapping::load_all(&customers()).unwrap();
    let bauwerk = mappings
        .iter()
        .find(|m| m.integration == "bauwerk-ag")
        .unwrap();
    let body = sample("bauwerk-ag", "broken.json");
    let delivery = Delivery {
        body: &body,
        channel: bauwerk.channel,
        reference: "webhook",
        received_at: Utc::now(),
    };
    bus.deliver(bauwerk, &seed, &delivery).await.unwrap();

    let report = replay(&bus, &mappings, &seed, Some("neckar-infra"))
        .await
        .unwrap();
    assert_eq!(
        (report.replayed, report.still_failing.len()),
        (0, 0),
        "bauwerk's letter is not touched"
    );
    assert_eq!(bus.count(DLQ).await.unwrap(), 1);
}
