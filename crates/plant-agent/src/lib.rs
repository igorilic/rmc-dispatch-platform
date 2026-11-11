//! The plant-agent: the edge service at a batching plant (SPEC §6). It batches its plant's loads
//! from the dispatch plan and sends every ticket through a SQLite outbox, so a cut cloud link
//! loses nothing.

pub mod store;
