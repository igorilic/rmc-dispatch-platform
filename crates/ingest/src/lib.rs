//! Ingest: customer orders, telematics and batch tickets in; canonical events out (SPEC §5).

pub mod mapping;
pub mod nats;
mod validate;

pub use validate::report;
