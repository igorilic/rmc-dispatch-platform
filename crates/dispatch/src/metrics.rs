//! Prometheus metrics of the dispatch service (SPEC §8). The names are constants, so a test can
//! check that the Grafana dashboard only queries metrics that exist.
//!
//! The plan gauges describe the current plan only: every new version replaces them, and dispatch
//! sets them from the latest stored version at startup. A cumulative histogram over every
//! version would count an unchanged pour again with each one.

use std::time::Duration;

use ::metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

use crate::planner::Plan;
use crate::redispatch::Trigger;

/// One re-dispatch run, by `outcome`: `new_version`, `unchanged` or `failed`.
pub const REDISPATCH_SECONDS: &str = "pourline_redispatch_duration_seconds";
/// The triggers re-dispatches ran for, by `kind`.
pub const TRIGGERS: &str = "pourline_redispatch_triggers_total";
/// The current plan's waits between loads, per `bucket` of minutes.
pub const POUR_GAPS: &str = "pourline_plan_pour_gaps";
pub const PLAN_VERSION: &str = "pourline_plan_version";
pub const PLANNED_ORDERS: &str = "pourline_plan_planned_orders";
pub const UNPLANNED_ORDERS: &str = "pourline_plan_unplanned_orders";
pub const LOADS: &str = "pourline_plan_loads";
pub const MAX_GAP: &str = "pourline_plan_max_gap_minutes";
pub const GAPS_OVER_LIMIT: &str = "pourline_plan_gaps_over_limit";
pub const LATENESS: &str = "pourline_plan_lateness_minutes";
pub const EMPTY_KM: &str = "pourline_plan_empty_km";

pub const NAMES: &[&str] = &[
    REDISPATCH_SECONDS,
    TRIGGERS,
    POUR_GAPS,
    PLAN_VERSION,
    PLANNED_ORDERS,
    UNPLANNED_ORDERS,
    LOADS,
    MAX_GAP,
    GAPS_OVER_LIMIT,
    LATENESS,
    EMPTY_KM,
];

/// Gives every metric its help text.
pub fn describe() {
    describe_histogram!(
        REDISPATCH_SECONDS,
        ::metrics::Unit::Seconds,
        "One re-dispatch, from loading the world to storing the version, by outcome"
    );
    describe_counter!(TRIGGERS, "Triggers re-dispatches ran for, by kind");
    describe_gauge!(
        POUR_GAPS,
        "The current plan's waits between consecutive loads, per bucket of minutes"
    );
    describe_gauge!(PLAN_VERSION, "The current plan's version");
    describe_gauge!(PLANNED_ORDERS, "Orders in the current plan");
    describe_gauge!(UNPLANNED_ORDERS, "Orders the current plan couldn't place");
    describe_gauge!(LOADS, "Loads in the current plan");
    describe_gauge!(
        MAX_GAP,
        "The current plan's longest wait at a site, in minutes"
    );
    describe_gauge!(
        GAPS_OVER_LIMIT,
        "Waits in the current plan longer than their order's maximum gap"
    );
    describe_gauge!(
        LATENESS,
        "Minutes the current plan's first loads arrive late, summed"
    );
    describe_gauge!(EMPTY_KM, "Kilometres the current plan drives empty");
}

/// Records one re-dispatch run and the triggers it ran for.
pub fn redispatched(outcome: &'static str, took: Duration, triggers: &[Trigger]) {
    histogram!(REDISPATCH_SECONDS, "outcome" => outcome).record(took);
    for trigger in triggers {
        counter!(TRIGGERS, "kind" => trigger.kind()).increment(1);
    }
}

/// Sets the plan gauges to `plan`, the current version.
pub fn current_plan(version: i64, plan: &Plan) {
    let m = &plan.metrics;
    gauge!(PLAN_VERSION).set(version as f64);
    gauge!(PLANNED_ORDERS).set(m.planned_orders as f64);
    gauge!(UNPLANNED_ORDERS).set(m.unplanned_orders as f64);
    gauge!(LOADS).set(m.loads as f64);
    gauge!(MAX_GAP).set(m.max_gap_minutes);
    gauge!(GAPS_OVER_LIMIT).set(m.gaps_over_limit as f64);
    gauge!(LATENESS).set(m.lateness_minutes);
    gauge!(EMPTY_KM).set(m.empty_km);
    let g = &m.pour_gaps;
    for (bucket, count) in [
        ("0-5", g.up_to_5),
        ("5-10", g.up_to_10),
        ("10-20", g.up_to_20),
        ("20-30", g.up_to_30),
        ("30+", g.longer),
    ] {
        gauge!(POUR_GAPS, "bucket" => bucket).set(count as f64);
    }
}
