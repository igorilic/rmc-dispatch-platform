//! Dispatch: plans which plant batches each load, when, and on which truck (SPEC §4).

pub mod api;
pub mod intake;
pub mod planner;
pub mod redispatch;
pub mod search;
pub mod store;
#[cfg(test)]
mod testkit;
pub mod travel;
pub mod verify;
pub mod worker;
