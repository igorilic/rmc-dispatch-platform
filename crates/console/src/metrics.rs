//! The console's Prometheus metrics (SPEC §8). Every name starts with `pourline_console_`.

use std::time::Duration;

/// Pages connected to the live stream.
pub const CLIENTS: &str = "pourline_console_clients";
/// Changes pushed to the pages, by `kind` (truck, plant, plan, edge, uplink, log).
pub const PUSHED: &str = "pourline_console_pushed_total";
/// Actions by `action` and `outcome` (done, refused, failed).
pub const ACTIONS: &str = "pourline_console_actions_total";
/// From ingest receiving a truck or ticket event to the console pushing it, by `kind`.
pub const EVENT_LAG: &str = "pourline_console_event_lag_seconds";

/// Every metric the console exports. The dashboard test reads these names.
pub const NAMES: &[&str] = &[CLIENTS, PUSHED, ACTIONS, EVENT_LAG];

pub fn describe() {
    metrics::describe_gauge!(CLIENTS, "Pages connected to the live stream");
    metrics::describe_counter!(PUSHED, "Changes pushed to the pages");
    metrics::describe_counter!(ACTIONS, "Actions from the map, by outcome");
    metrics::describe_histogram!(
        EVENT_LAG,
        metrics::Unit::Seconds,
        "From ingest receiving an event to the console pushing it"
    );
}

pub fn client_connected() {
    metrics::gauge!(CLIENTS).increment(1.0);
}

pub fn client_gone() {
    metrics::gauge!(CLIENTS).decrement(1.0);
}

pub fn pushed(kind: &'static str) {
    metrics::counter!(PUSHED, "kind" => kind).increment(1);
}

pub fn action(action: &'static str, outcome: &'static str) {
    metrics::counter!(ACTIONS, "action" => action, "outcome" => outcome).increment(1);
}

pub fn event_lag(kind: &'static str, lag: Duration) {
    metrics::histogram!(EVENT_LAG, "kind" => kind).record(lag.as_secs_f64());
}
