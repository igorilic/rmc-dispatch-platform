//! The plant-agent: the edge service at a batching plant (SPEC §6).
//!
//! One process runs three loops that share one SQLite file ([`store`]):
//! - [`sync`] fetches the plant's slice of the plan from dispatch and keeps it;
//! - [`batching`], the batch computer, batches each load when its time comes and issues its
//!   tickets into the outbox;
//! - [`forward`] sends the outbox to the broker, oldest first.
//!
//! Only the forwarder waits for the cloud. When the link is down, the agent keeps batching from
//! its last plan, and the outbox fills up until the link comes back. [`http`] serves health and
//! status, and [`metrics`] exports them for Prometheus.

pub mod batching;
pub mod forward;
pub mod http;
pub mod metrics;
pub mod store;
pub mod sync;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use chrono::{DateTime, TimeDelta, Utc};
use domain::{Plant, PlantId, PlantPlan};
use serde::Serialize;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;

use crate::forward::Link;
use crate::store::{Store, StoreError};

/// Wall-clock time that follows tokio's clock, so ticket times stay right in paused-time tests.
// ponytail: the same eight lines as simulators::Clock; the agent builds in its own workspace.
#[derive(Debug, Clone, Copy)]
pub struct Clock {
    wall0: DateTime<Utc>,
    t0: Instant,
}

impl Clock {
    pub fn start(wall0: DateTime<Utc>) -> Self {
        Clock {
            wall0,
            t0: Instant::now(),
        }
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.wall0 + TimeDelta::from_std(self.t0.elapsed()).unwrap_or_default()
    }
}

/// The plan the agent works from, and when dispatch last confirmed it.
#[derive(Debug, Clone, PartialEq)]
pub struct Synced {
    pub plan: PlantPlan,
    pub at: DateTime<Utc>,
}

/// Everything the loops share.
pub struct Agent {
    pub plant: Plant,
    pub store: Arc<Store>,
    pub clock: Clock,
    /// The current plan: the stored one at startup, then each slice dispatch sends.
    pub plan: watch::Sender<Option<Synced>>,
    /// Wakes the forwarder when a ticket is queued.
    pub wake: Arc<Notify>,
    pub mqtt: Arc<Link>,
    dispatch_up: AtomicBool,
    queued: AtomicU64,
}

/// What `/status` returns and the metrics report.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Status {
    pub plant: PlantId,
    /// Whether the broker link is up.
    pub mqtt: bool,
    /// Whether the last plan fetch succeeded.
    pub dispatch: bool,
    /// Tickets waiting in the outbox.
    pub outbox: u64,
    /// How long the oldest waiting ticket has waited: SPEC §8's edge sync lag.
    pub sync_lag_seconds: i64,
    pub plan_version: Option<i64>,
    /// Since dispatch last confirmed the plan; `None` without a plan.
    pub plan_age_seconds: Option<i64>,
    /// Tickets issued and acknowledged since the agent started.
    pub queued: u64,
    pub sent: u64,
}

impl Agent {
    /// The agent for `plant`, working from the plan kept in `store`, if any.
    pub fn new(plant: Plant, store: Store, clock: Clock) -> Result<Agent, StoreError> {
        let stored = store.plan()?;
        Ok(Agent {
            plant,
            store: Arc::new(store),
            clock,
            plan: watch::Sender::new(stored),
            wake: Arc::new(Notify::new()),
            mqtt: Arc::new(Link::default()),
            dispatch_up: AtomicBool::new(false),
            queued: AtomicU64::new(0),
        })
    }

    /// Whether the agent can work: it has a plan, fetched or stored. The cloud link doesn't
    /// matter, because an offline plant keeps working.
    pub fn ready(&self) -> bool {
        self.plan.borrow().is_some()
    }

    pub fn status(&self) -> Result<Status, StoreError> {
        let now = self.clock.now();
        let seconds = |since: DateTime<Utc>| (now - since).num_seconds().max(0);
        let synced = self.plan.borrow().clone();
        Ok(Status {
            plant: self.plant.id.clone(),
            mqtt: self.mqtt.up(),
            dispatch: self.dispatch_up.load(Ordering::Relaxed),
            outbox: self.store.waiting()?,
            sync_lag_seconds: self.store.oldest()?.map_or(0, |q| seconds(q.queued_at)),
            plan_version: synced.as_ref().and_then(|s| s.plan.version),
            plan_age_seconds: synced.as_ref().map(|s| seconds(s.at)),
            queued: self.queued.load(Ordering::Relaxed),
            sent: self.mqtt.sent(),
        })
    }

    fn set_dispatch_up(&self, up: bool) {
        self.dispatch_up.store(up, Ordering::Relaxed);
        metrics::connectivity("dispatch", up);
    }

    fn ticket_queued(&self) {
        self.queued.fetch_add(1, Ordering::Relaxed);
        metrics::ticket_queued();
        self.wake.notify_one();
    }
}
