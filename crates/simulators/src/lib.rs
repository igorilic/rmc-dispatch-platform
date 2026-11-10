//! Fake trucks, plants and customers, so Pourline runs locally with no real data (SPEC §2, M2).
//!
//! The `simulators` binary runs all of them; the `demo` binary reuses the customers' formats to
//! play `make demo`'s scenario.

pub mod customers;
pub mod fleet;
pub mod link;
pub mod orders;
pub mod plant;

use chrono::{DateTime, TimeDelta, Utc};
use tokio::time::Instant;

/// Wall-clock time that follows tokio's clock, so payload timestamps stay right in paused-time tests.
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
