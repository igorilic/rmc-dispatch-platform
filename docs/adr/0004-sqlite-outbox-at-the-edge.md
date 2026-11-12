# ADR 0004: SQLite outbox for edge store-and-forward

- **Status:** Accepted
- **Date:** 2026-11-11 (milestone 7)

## Context

Every batching plant runs a `plant-agent` on a small edge node, in its own k3s cluster. It batches
the plant's loads from its slice of the plan, and it reports each step as a batch ticket
(`batching`, `loading`, `loaded`). Those tickets freeze loads in the cloud, so losing one, or
counting one twice, corrupts the plan.

The plant's link to the cloud drops, sometimes for minutes, and the plant can't stop pouring
concrete while it's down. So the agent has to:

- keep batching from the last plan it has while offline, and report how stale that plan is;
- never lose a ticket, even if the agent or the node restarts while offline;
- deliver tickets in the order they were issued once the link is back, with no duplicates.

## Decision

The agent keeps a **SQLite file** with an outbox
([`crates/plant-agent/src/store.rs`](../../crates/plant-agent/src/store.rs)):

- **`outbox`** holds tickets waiting to be sent, oldest first. Each ticket is serialised once,
  when it is queued, so a resend is byte-for-byte the same message.
- **`batched`** records every load the plant has started, so a load is never batched twice,
  whatever a newer plan says.
- **`plan`** holds the last plan slice and when dispatch last confirmed it, so a restarted agent
  that can't reach the cloud still has a plan.

A ticket and its load's new stage are written in one transaction, with `synchronous = FULL`, so a
crash leaves both or neither. The forwarder
([`forward.rs`](../../crates/plant-agent/src/forward.rs)) sends one ticket at a time, oldest first,
over MQTT QoS 1. It deletes a ticket only after the broker's `PUBACK`, and Mosquitto persists what it
acknowledged. A lost acknowledgement means a resend. Ingest recognises the resend by its event id,
which it keeps in its deduplication KV for 7 days, not just JetStream's 2-minute window, because a
cut can last much longer than that ([ADR 0001](0001-nats-jetstream-over-kafka.md)).

Tickets go through ingest like every other source, so they get the same validation, DLQ, traces
and metrics. The plan slice comes from dispatch over HTTP (`GET /plants/{id}/plan`).

SQLite comes from `rusqlite` with its `bundled` feature, which compiles a patched SQLite into the
binary, so the node needs no system library. The root workspace's `sqlx` pins an older
`libsqlite3-sys`, and one lockfile can link only one SQLite, so the agent is its own Cargo
workspace for now ([ADR 0005](0005-monorepo-cargo-workspace.md)).

## Consequences

- `make chaos` shows it: it cuts the plant off for 2 minutes, the agent keeps batching, the outbox
  grows, and it drains once the link is back. The script fails if the outbox doesn't drain within
  2 minutes. Ingest drops any resend by its event id.
- SQLite is one file on a PVC. It's easy to inspect with `sqlite3` on site, and backing it up is
  copying a file.
- One ticket in flight caps throughput at one broker round trip per ticket. A plant issues a few
  tickets per load, so that's far more than needed, and it keeps ordering trivial.
- **Cost:** a second Cargo workspace and lockfile until `sqlx` allows a newer `libsqlite3-sys`.
- **Risk:** the outbox ends at the broker. If ingest is down for longer than the broker's queue
  holds (`max_queued_messages 100000`), messages are lost at the broker, not at the plant. An
  end-to-end acknowledgement from dispatch back to the plant would close that gap, at the cost of
  a return channel.

## Alternatives considered

- **A NATS leaf node with JetStream at the plant.** It gives store-and-forward without code. It lost
  because tickets would bypass ingest's checks and DLQ, every plant would need NATS credentials and
  a route to the cloud's NATS, and dedup would still be limited by the 2-minute window.
- **A pure-Rust embedded store** (`redb`, `sled`). That would avoid the SQLite clash with `sqlx`
  and the second workspace, but it departs from SPEC §6 and §14, and it's less familiar to
  whoever has to inspect a plant's state on site.
- **An in-memory queue with retries.** It's simple, but it loses every queued ticket when the agent
  or the node restarts, which happens exactly when things are already going wrong.
- **Wait for dispatch to confirm each ticket before deleting it.** This is a true end-to-end
  guarantee, but it needs a channel back to every plant and an outbox of acknowledgements in the
  cloud. Deferred until the broker's queue is shown to be a real limit.
