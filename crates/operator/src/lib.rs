//! The Pourline operator (SPEC §7): runs each `CustomerIntegration` as its own ingest workload.
//! `crd` is the resource, `workload` what it becomes, and `reconcile` the controller that keeps
//! the two in step.

pub mod crd;
pub mod reconcile;
pub mod workload;

pub use reconcile::run;
