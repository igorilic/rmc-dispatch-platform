//! The agent's Prometheus metrics (SPEC §6, §8). Every name starts with `pourline_edge_`. The
//! gauges are set from [`Status`] every few seconds; the counters as tickets come and go.

use crate::Status;

/// SPEC §6's `connectivity` metric: 1 while a link is up, per `link` (mqtt, dispatch).
pub const CONNECTIVITY: &str = "pourline_edge_connectivity";
pub const OUTBOX: &str = "pourline_edge_outbox_messages";
/// SPEC §8's edge sync lag: how long the oldest waiting ticket has waited.
pub const SYNC_LAG: &str = "pourline_edge_sync_lag_seconds";
/// How stale the plan is: the time since dispatch last confirmed it.
pub const PLAN_AGE: &str = "pourline_edge_plan_age_seconds";
pub const PLAN_VERSION: &str = "pourline_edge_plan_version";
/// Tickets by `outcome`: queued into the outbox, or sent (acknowledged by the broker).
pub const TICKETS: &str = "pourline_edge_tickets_total";

/// Every metric the agent exports. The dashboard test reads these names from this file.
pub const NAMES: &[&str] = &[
    CONNECTIVITY,
    OUTBOX,
    SYNC_LAG,
    PLAN_AGE,
    PLAN_VERSION,
    TICKETS,
];

pub fn describe() {
    metrics::describe_gauge!(
        CONNECTIVITY,
        "1 while the link to the broker or dispatch is up"
    );
    metrics::describe_gauge!(OUTBOX, "Tickets waiting in the outbox");
    metrics::describe_gauge!(SYNC_LAG, "How long the oldest waiting ticket has waited");
    metrics::describe_gauge!(PLAN_AGE, "Seconds since dispatch last confirmed the plan");
    metrics::describe_gauge!(PLAN_VERSION, "The plan version the agent works from");
    metrics::describe_counter!(TICKETS, "Tickets queued into the outbox, and sent from it");
}

pub fn connectivity(link: &'static str, up: bool) {
    metrics::gauge!(CONNECTIVITY, "link" => link).set(if up { 1.0 } else { 0.0 });
}

pub fn ticket_queued() {
    metrics::counter!(TICKETS, "outcome" => "queued").increment(1);
}

pub fn ticket_sent() {
    metrics::counter!(TICKETS, "outcome" => "sent").increment(1);
}

/// Sets the gauges from a fresh status.
pub fn report(status: &Status) {
    metrics::gauge!(OUTBOX).set(status.outbox as f64);
    metrics::gauge!(SYNC_LAG).set(status.sync_lag_seconds as f64);
    if let Some(age) = status.plan_age_seconds {
        metrics::gauge!(PLAN_AGE).set(age as f64);
    }
    if let Some(version) = status.plan_version {
        metrics::gauge!(PLAN_VERSION).set(version as f64);
    }
}
