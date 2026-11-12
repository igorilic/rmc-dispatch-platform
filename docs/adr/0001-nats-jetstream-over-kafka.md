# ADR 0001: NATS JetStream over Kafka

- **Status:** Accepted
- **Date:** 2026-11-09 (milestone 1)

## Context

Pourline turns orders, telematics and batch tickets from many channels into canonical events, and
dispatch re-plans from those events. The event backbone has to:

- keep events durably, so dispatch can restart and resume where it stopped;
- drop duplicates, because customers resend, SFTP snapshots repeat the same orders for days,
  telematics arrive twice, and a plant's outbox resends after a lost acknowledgement;
- keep dead letters apart per customer integration, so one integration can be replayed after its
  mapping is fixed;
- be cheap to run for a small team: in Docker Compose on a laptop, in a k3s cluster, and in CI.

The team is small and runs plant sites with flaky links, so the operational cost of the broker
matters more than its peak throughput. The load is modest: about 25 trucks report every few
seconds, and orders arrive by the hundred per day.

## Decision

NATS with JetStream is the event backbone ([`crates/ingest/src/nats.rs`](../../crates/ingest/src/nats.rs)):

- **`EVENTS`** holds every canonical event under `events.>` (`events.orders.<customer>`,
  `events.trucks.*`, `events.plants.*`) for 7 days.
- **`DLQ`** holds dead letters under `dlq.<integration>`, so their count per integration is a
  subject count, and `ingest-cli dlq replay --customer <name>` replays one integration.
- **Deduplication** has two layers. Every event is published with its id as `Nats-Msg-Id`, so
  JetStream drops a repeat within its 2-minute window. Order and batch-ticket keys also go into the
  `ingest-dedup` KV bucket for `DEDUP_TTL_DAYS` (7), because their repeats can be days apart.
- **`ingest-status`**, a second KV bucket, holds each integration's last successful ingest, which
  the operator reports.
- **Dispatch** reads with durable consumers, one event at a time in stream order
  (`max_ack_pending = 1`), so a cancellation never overtakes the order it cancels
  ([`crates/dispatch/src/intake.rs`](../../crates/dispatch/src/intake.rs)).

One NATS server runs everywhere: in compose, in the cloud chart, and in the tests through
testcontainers.

## Consequences

- One small binary gives us streams, deduplication and key-value storage. There is no separate
  Redis for idempotency keys and no compacted topics to manage.
- `Nats-Msg-Id` makes publishing idempotent across retries and across processes. Kafka's idempotent
  producer only covers retries within one producer session.
- A DLQ per integration is just a subject filter. Replaying one customer's dead letters needs no
  topic per customer.
- The whole broker is one small container, which keeps the integration tests and the e2e job fast.
- The trace context travels in a `traceparent` message header, so one trace runs from a webhook
  through NATS into the re-dispatch.
- **Not decided here:** the plant edge doesn't use NATS. Plant-agents send batch tickets over MQTT
  through ingest, like the trucks ([ADR 0004](0004-sqlite-outbox-at-the-edge.md)). NATS leaf nodes
  would have given store-and-forward for free, but they would bypass ingest's validation and DLQ,
  and need NATS credentials at every plant.
- **Risks:** a single NATS server is a single point of failure, and its stream lives on an
  `emptyDir` in the chart. A production setup needs a three-node JetStream cluster with persistent
  volumes, which the chart doesn't attempt. The KV buckets grow with the order volume. At
  hundreds of orders a day that is negligible, but it is the first thing to watch at larger scale.

## Alternatives considered

- **Kafka (or Redpanda).** It's the industry default for event streams, and its ecosystem (Connect,
  Schema Registry, ksqlDB) is far bigger. It lost for this team and load: it means a broker plus
  KRaft controllers, partitions to plan, and an extra store for idempotency keys, and it has no
  answer for the edge. Redpanda removes the JVM but not the rest.
- **RabbitMQ.** It has good dead-lettering, but replaying history needs Streams. It also has no
  built-in deduplication by message id, and no key-value store.
- **Postgres as a queue** (`LISTEN/NOTIFY` or an outbox table). It's one less moving part, since
  dispatch already has Postgres. But ingest would then need the dispatch database, and dispatch's
  database would become the bottleneck for every source.
