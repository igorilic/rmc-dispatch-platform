# Pourline — ready-mix concrete dispatch platform

Rust portfolio project. It dispatches ready-mix concrete trucks from batching plants to
construction sites. It takes in customer orders (REST/JSON, SFTP/CSV, REST/XML) and
truck telematics (MQTT), turning them into canonical events on NATS JetStream. It
re-dispatches incrementally when reality changes, and it runs in the cloud plus on an
edge node at each plant with store-and-forward.

**`SPEC.md` is the source of truth.** Read the relevant sections before you change anything.

## How we work

- Go one milestone at a time (list below). Never start work that belongs to a later milestone.
- For each milestone: show a short plan → wait for the user's OK → implement with tests →
  `make test` green and `make up` healthy → update README.md → Conventional Commit(s) →
  stop and summarise what changed and what to look at.
- Never weaken a hard constraint from SPEC §4 to make a test pass:
  one load per truck at a time, load ≤ drum capacity, parallel loads at a plant ≤ its loading bays,
  discharge within the mix design's time limit, and no out-of-service trucks or offline plants.
- If the spec is ambiguous or looks wrong, ask. Don't guess.
- Verification is `make test` (fmt, clippy `-D warnings`, unit/property/integration tests). Integration
  tests start containers with testcontainers, so Docker must be running. The Makefile sets `DOCKER_HOST`
  from the current Docker context.
- Customer formats live in `customers/<name>/mapping.yaml`. Never add customer-specific Rust (SPEC §5).
  Check a format change with `cargo run -p ingest-cli -- validate --customer <name> --file <sample>`.
- Use the latest stable versions of crates, container images and GitHub Actions. Pin images to exact
  tags, never `latest`. `make audit` (cargo-audit) must be clean. Check `rust-toolchain.toml` at each milestone.
- The repo is public (`igorilic/rmc-dispatch-platform`). Never commit credentials, internal hostnames or personal data.

## Domain decisions (owner clarifications on top of SPEC §3)

- Customers and their ship-tos are **our master data**, in one uniform model. Customers never send
  them. `Customer { id = our customer number, name, address }`. The ship-to keeps the spec's name
  `Site { id = our ship-to number, customer, name, address?, location?, access_notes }`. A site has
  an address, coordinates, or both. Address-only sites are placed at their postal code's centroid
  (`PostalCodeGeocoder`, a deliberately fake geocoder).
- Orders from customer systems reference **our** customer number and ship-to number, in each
  channel's own field names. Ingest validates both against master data; there is no mapping of
  customer-side codes.
- An order has exactly one mix. Two mixes means two orders.
- Order status values: `unconfirmed` (dispatch centre confirmed, customer not), `pending` (customer
  confirmed, dispatch centre not), `confirmed` (both), `in_progress` (batching/loading started, a
  delivery exists), `shipped` (loading finished, truck left the plant), `delivered` (every delivery
  unloaded), `cancelled`. No partial or returned states.
- The plan holds `Load`s, i.e. planned truck trips. A `Delivery` is created only when batching starts.
  Before that, an order has no deliveries. Re-dispatch freezes loads that already have a delivery.
- Delivery status comes from trucking events, in this order: `batching`, `loading`, `loaded`,
  `left_plant`, `arrived_at_customer`, `unloading`, `unloaded`. There is no washing or return.
  The status is the furthest event reached, so out-of-order events never move it backwards.
- Trucks hold 7–15 m³, with some special trucks outside that range. Volumes are `f64` m³.
- The JSON wire format is snake_case.

## Conventions (SPEC §15)

- No `unwrap()` / `expect()` outside tests and `main` startup.
- Errors: `thiserror` in libraries, `anyhow` in binaries.
- All config comes from environment variables with documented defaults. Keep `.env.example` up to date.
- Every public function in `dispatch` has a doc comment. Algorithmic modules have a module-level explanation.
- Tests live next to the code. Integration tests go in `crates/*/tests/`.
- Conventional Commits. One milestone = one or more focused commits.
- Never commit secrets. Local credentials live only in `.env`, which is gitignored.

## Milestones (SPEC §13)

Each one ends with a runnable repo, green CI and a commit.

1. [x] **Skeleton**: workspace, `domain` crate, compose stack with infra only, Makefile, CI with fmt/clippy/test, README stub.
2. [x] **Simulators**: truck fleet over MQTT, three fake customer systems, fake plant. Visible in logs.
3. [x] **Ingest**: mapping configs, canonical events, idempotency, DLQ + replay, `ingest-cli validate`. Integration tests.
4. [x] **Dispatch v1**: data model + migrations, construction heuristic, REST API with OpenAPI, property tests.
5. [x] **Re-dispatch**: event handling, local search, frozen loads, plan versions + diffs, benchmarks. `make demo` works end to end.
6. [ ] **Observability**: traces end to end, metrics, Grafana dashboard.
7. [ ] **Edge**: `plant-agent` with SQLite outbox, two k3d clusters, `make chaos`.
8. [ ] **Operator**: `CustomerIntegration` CRD + controller, Helm charts, e2e test in CI.
9. [ ] **Polish**: architecture doc with diagrams, 4–6 ADRs, demo GIF, README walkthrough.
