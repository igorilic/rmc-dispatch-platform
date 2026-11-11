//! Prometheus metrics of the ingest service (SPEC §8). The names are constants, so a test can check
//! that the Grafana dashboard only queries metrics that exist.

use std::collections::{HashMap, HashSet};
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
/// show up too. Each of `integrations` reports 0 until it has letters, so the dashboard shows an
/// empty DLQ as 0 rather than as no data.
pub async fn watch_dlq(bus: Bus, every: Duration, integrations: impl IntoIterator<Item = String>) {
    let mut known: HashSet<String> = integrations.into_iter().collect();
    let mut ticks = tokio::time::interval(every);
    loop {
        ticks.tick().await;
        match bus.dead_letter_counts().await {
            Ok(counts) => report_dlq(&mut known, counts),
            Err(error) => warn!(%error, "reading the DLQ size failed, retrying at the next tick"),
        }
    }
}

/// Sets the DLQ gauges: `counts` for the integrations that have letters, 0 for every other one
/// seen so far, such as one whose letters were all replayed.
fn report_dlq(known: &mut HashSet<String>, counts: HashMap<String, u64>) {
    for integration in known.iter().filter(|i| !counts.contains_key(*i)) {
        gauge!(DLQ_MESSAGES, "integration" => integration.clone()).set(0.0);
    }
    for (integration, count) in &counts {
        gauge!(DLQ_MESSAGES, "integration" => integration.clone()).set(*count as f64);
    }
    known.extend(counts.into_keys());
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use metrics_exporter_prometheus::PrometheusBuilder;

    use super::*;

    #[test]
    fn every_known_integration_reports_its_dead_letters_even_without_any() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let mut known = HashSet::from(["bauwerk-ag".to_owned()]);
        ::metrics::with_local_recorder(&recorder, || {
            report_dlq(&mut known, HashMap::from([("rheinbau-gmbh".to_owned(), 2)]));
        });
        let text = handle.render();
        assert!(
            text.contains(r#"pourline_dlq_messages{integration="bauwerk-ag"} 0"#),
            "{text}"
        );
        assert!(text.contains(r#"pourline_dlq_messages{integration="rheinbau-gmbh"} 2"#));

        // rheinbau's letters were replayed.
        ::metrics::with_local_recorder(&recorder, || report_dlq(&mut known, HashMap::new()));
        let text = handle.render();
        assert!(
            text.contains(r#"pourline_dlq_messages{integration="rheinbau-gmbh"} 0"#),
            "{text}"
        );
    }
}
