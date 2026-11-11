//! The provisioned Grafana dashboard only queries metrics that ingest or dispatch define, so a
//! renamed metric can't leave a panel silently empty.

// Integration tests are test code; SPEC §15 allows unwrap there.
#![allow(clippy::unwrap_used)]

use std::path::Path;

#[test]
fn the_dashboard_only_queries_metrics_the_services_define() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/grafana/dashboards/dispatch-overview.json");
    let dashboard = std::fs::read_to_string(&path).unwrap();
    let defined: Vec<&str> = ingest::metrics::NAMES
        .iter()
        .chain(dispatch::metrics::NAMES)
        .copied()
        .collect();

    let mut queried = 0;
    for (at, _) in dashboard.match_indices("pourline_") {
        let name: String = dashboard[at..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        // Prometheus serves a histogram as these three series.
        let base = ["_bucket", "_sum", "_count"]
            .iter()
            .find_map(|suffix| name.strip_suffix(suffix))
            .filter(|base| defined.contains(base))
            .unwrap_or(&name);
        assert!(
            defined.contains(&base),
            "the dashboard queries {name}, which no service defines"
        );
        queried += 1;
    }
    assert!(queried > 0, "no queries found in {}", path.display());
}
