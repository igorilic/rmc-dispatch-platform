# ADR 0005: One repository, one Cargo workspace (with one exception)

- **Status:** Accepted, amended 2026-11-11
- **Date:** 2026-11-09 (milestone 1)

## Context

Pourline is seven Rust components (`domain`, `dispatch`, `ingest`, `ingest-cli`, `plant-agent`,
`operator`, `simulators`) plus a shared `telemetry` crate, Helm charts, a compose stack and CI.
They share types. The canonical events in `domain` are written by ingest and the plant-agent and
read by dispatch, so a change to an event touches several services at once. One small team builds
and releases all of it.

## Decision

Everything lives in one repository, and every crate under `crates/` belongs to one Cargo workspace
([`Cargo.toml`](../../Cargo.toml)):

- `[workspace.dependencies]` pins each third-party crate once, and the members inherit it, so
  every service runs the same `tokio`, `serde` and `async-nats`.
- One `Cargo.lock`, one `target/`, one `make test` (fmt, clippy `-D warnings`, every test).
- One Dockerfile builds any binary (`--build-arg BIN=<name>`) into a distroless image.
- One CI workflow, and Dependabot, cover the whole repository.

**Amendment (milestone 7):** the plant-agent is its own workspace, with its own lockfile
([`crates/plant-agent/Cargo.toml`](../../crates/plant-agent/Cargo.toml)). It needs `rusqlite` 0.42,
which links `libsqlite3-sys` 0.40. The root workspace's `sqlx` 0.9 caps `libsqlite3-sys` below 0.38,
even with only its Postgres driver, and one lockfile can link only one SQLite. Older `rusqlite`
releases fit, but they bundle a SQLite with a known advisory. The agent still shares `domain` and
`telemetry` by path, and shares `target/`. It merges back by deleting two lines once `sqlx` allows
`libsqlite3-sys` 0.39 or later.

## Consequences

- An event change is one commit that updates the producer, the consumer and the tests together.
  The compiler finds every place that has to change.
- One version of each dependency means one audit (`make audit`) and one set of Dependabot updates.
- Anyone can run the whole system from one checkout (`make up`, `make demo`, `make k8s`).
- **Cost:** a change to `domain` rebuilds and retests everything. CI builds every image on every
  push, for amd64 and arm64, rather than only the services that changed.
- **Cost of the exception:** the plant-agent repeats its dependency versions, and the Makefile
  (test and audit), the Dockerfile and Dependabot each need an extra line or two for it.
- All services are versioned together. The images are tagged with the commit (`sha-<7 chars>`),
  not with a version per service.

## Alternatives considered

- **A repository per service.** It gives independent releases and smaller builds. But `domain`
  would have to become a published crate with its own release cycle, and every event change would
  turn into coordinated releases across repositories, which is a lot of ceremony for one team.
- **One repository, a workspace per service.** It isolates dependency trees, so a clash like the
  plant-agent's could never block anyone. But versions drift between services, and there are N
  lockfiles to audit. We take that cost only where the clash forces it.
- **Merge the plant-agent back by downgrading `rusqlite`.** It keeps one workspace, but it would
  mean ignoring an advisory for code we actually compile, which `make audit` is there to prevent.
