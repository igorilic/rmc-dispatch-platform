//! Prometheus metrics of the ingest service (SPEC §8). The names are constants, so a test can check
//! that the Grafana dashboard only queries metrics that exist.

use std::collections::HashSet;
use std::time::Duration;

use ::metrics::{counter, describe_counter, describe_gauge, gauge};
use tracing::warn;

use crate::nats::Bus;

/// Records per integration and outcome: `published`, `duplicate` or `dead_lettered`.
pub const RECORDS: &str = "pourline_ingest_records_total";
/// Dead letters waiting for a replay, per integration.
pub const DLQ_MESSAGES: &str = "pourline_dlq_messages";
pub const NAMES: &[&str] = &[RECORDS, DLQ_MESSAGES];

/// Gives every metric its help text.
pub fn describe() {
    describe_counter!(
        RECORDS,
        "Records received, by integration and outcome (published, duplicate, dead_lettered)"
    );
    describe_gauge!(
        DLQ_MESSAGES,
        "Dead letters waiting for a replay, by integration"
    );
}

/// Counts one record. The integrations are the customers' plus `telematics` and `plants`.
pub(crate) fn record(integration: &str, outcome: &'static str) {
    counter!(RECORDS, "integration" => integration.to_owned(), "outcome" => outcome).increment(1);
}

/// Reads the DLQ's size per integration every `every`, forever, so replays by `ingest-cli`
/// show up too. An integration whose letters were all replayed drops to 0.
pub async fn watch_dlq(bus: Bus, every: Duration) {
    let mut seen = HashSet::new();
    let mut ticks = tokio::time::interval(every);
    loop {
        ticks.tick().await;
        match bus.dead_letter_counts().await {
            Ok(counts) => {
                for integration in seen.iter().filter(|i| !counts.contains_key(*i)) {
                    gauge!(DLQ_MESSAGES, "integration" => String::clone(integration)).set(0.0);
                }
                for (integration, count) in &counts {
                    gauge!(DLQ_MESSAGES, "integration" => integration.clone()).set(*count as f64);
                }
                seen.extend(counts.into_keys());
            }
            Err(error) => warn!(%error, "reading the DLQ size failed, retrying at the next tick"),
        }
    }
}
