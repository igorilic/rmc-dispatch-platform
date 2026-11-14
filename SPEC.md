# Pourline — Ready-Mix Concrete Dispatch Platform

> Portfolio project. A Rust backend that dispatches ready-mix concrete trucks from batching plants to construction sites, ingests customer orders and truck telematics from many different sources, and runs both in the cloud and on an edge node at the plant.

## 1. The problem

Ready-mix concrete is perishable: once a load is batched, it has to be discharged on site within a fixed window (typically ~90 minutes, configurable per mix design). On a large pour, trucks must also arrive back-to-back. If the gap between two trucks gets too long, the concrete already placed starts to set and you get a *cold joint*, a structural defect.

So the dispatcher has to:

- split each order into truck loads,
- decide **when each load is batched** at which plant (plants have limited loading bays and m³/h capacity),
- assign **which truck** carries it,
- keep each site's pour **continuous** (gap between arrivals below a threshold),
- keep every load **inside its discharge window**,
- and **re-dispatch** quickly when reality changes: a truck breaks down, the site pump is late, a plant goes offline, or the customer changes volume mid-pour.

Orders come from contractors with very different IT setups. Trucks report GPS and drum status over cellular networks that drop out. The plant has to keep working when its connection to the cloud is down.

## 2. Scope at a glance

| Component | What it does |
|---|---|
| `dispatch` | Axum API + dispatch engine (planning and incremental re-dispatch) |
| `ingest` | Multi-source intake: REST webhooks, SFTP CSV drops, MQTT telematics → canonical events |
| `ingest-cli` | Validates a new customer's sample files against their mapping config before go-live |
| `plant-agent` | Edge service at the batching plant: batch tickets, "loaded" events, store-and-forward |
| `operator` | Kubernetes operator (kube-rs) for a `CustomerIntegration` CRD |
| `console` | Map console in the k8s stack: plants, sites and trucks live on a map with a status legend, and actions on them (§9) |
| `simulators` | Fake truck fleet, fake contractor systems, fake plant, so the whole system runs locally with no real data |
| `domain` | Shared types and canonical event schema |

## 3. Domain model

- **Plant**: id, location, loading bays, batching rate (m³/h), operating hours, status (online/offline).
- **Truck**: id, home plant, drum capacity (m³, e.g. 8–10), status (`available`, `loading`, `en_route`, `on_site`, `discharging`, `returning`, `out_of_service`), last GPS position.
- **Site**: id, customer, location, access notes.
- **MixDesign**: code, max time from batching to discharge end (minutes).
- **Order**: id, customer, site, mix, total volume (m³), requested start, pour rate (m³/h), max allowed gap between trucks (minutes), status.
- **Load**: order id, sequence no., volume, planned plant, planned truck, planned batch time, ETA on site, discharge window, actual timestamps.
- **DispatchPlan**: versioned set of loads; every re-dispatch produces a new version plus a **diff** (which loads moved, which truck changed, and why).

Travel time: haversine distance × configurable road factor ÷ average speed by time of day. Keep it behind a `TravelTimeProvider` trait so a real routing engine (OSRM) could be plugged in later. Default geography: Rhine-Neckar area (3 plants, ~25 trucks, ~40 sites).

## 4. Dispatch engine

**Hard constraints**
- A truck carries at most one load at a time; loads ≤ drum capacity.
- A plant never batches more loads in parallel than it has loading bays.
- Each load is discharged within its mix design's time limit.
- Out-of-service trucks and offline plants are never used.

**Soft objectives (weighted, configurable)**
- Minimise pour gaps on site (heavily weighted, since this is what causes cold joints).
- Minimise lateness against the requested start.
- Minimise empty truck kilometres.
- Minimise plan churn when re-dispatching (don't reshuffle the whole fleet over one late pump).

**Approach**
1. Construction heuristic: process orders by start time, split into loads, greedily assign the earliest feasible plant/truck slot.
2. Improvement: local search (swap / relocate loads between trucks and plants) under a time budget (default 500 ms).
3. Re-dispatch: freeze loads already batched or en route, repair only the affected part of the plan, and emit a diff with a human-readable reason per change.

**Events that trigger re-dispatch:** `TruckOutOfService`, `SiteDelayed{minutes}`, `PlantOffline`, `OrderVolumeChanged`, `OrderCancelled`, `NewOrder`.

**Quality bar:** property-based tests (`proptest`) for every hard constraint; `criterion` benchmarks at 50 / 200 / 1,000 loads; module-level docs explaining the algorithm.

## 5. Integration pipeline (`ingest`)

Three fake customers, each integrating differently:

| Customer | Channel | Format |
|---|---|---|
| `bauwerk-ag` | REST webhook | JSON, their own field names, sends updates as full replacements |
| `rheinbau-gmbh` | SFTP drop every N minutes | Semicolon-separated CSV, German decimal commas, local times |
| `neckar-infra` | REST webhook | XML, versioned schema (v1 and v2 live at the same time) |

Plus **truck telematics** over MQTT (`trucks/{id}/gps`, `trucks/{id}/drum`, `trucks/{id}/status`), with out-of-order and duplicate messages.

**Requirements**
- Per-customer **mapping config** in YAML (`customers/<name>/mapping.yaml`): field mapping, unit/locale conversion, timezone, and how to derive the idempotency key. No customer-specific Rust code.
- Everything becomes a **canonical event** (defined in `domain`), published to NATS JetStream.
- **Idempotency**: duplicate deliveries never create duplicate orders.
- **Schema versioning**: v1 and v2 of the same customer are processed side by side.
- Retries with exponential backoff; poison messages go to a **dead-letter stream**; `ingest-cli dlq replay` re-processes them after a mapping fix.
- **Backpressure**: bounded channels; slow downstream must not crash ingestion or lose data.
- `ingest-cli validate --customer rheinbau-gmbh --file sample.csv` reports exactly which rows/fields fail and why.

## 6. Edge: `plant-agent`

Runs at each batching plant (a separate k3s/k3d cluster representing the plant site).

- Pulls the plant's slice of the current dispatch plan, and pulls it at once when the cloud sends a sync command over MQTT (`plants/<id>/commands/sync`).
- Simulates batch tickets and emits `LoadBatched` / `TruckLoaded` events.
- **Store-and-forward**: when the cloud link is down, events go to a local SQLite outbox and are synced in order once the link returns. No loss, no duplicates (idempotency keys).
- Keeps working from the last known plan while offline, and reports how stale its plan is.
- Exposes `/healthz`, `/readyz` and a `connectivity` metric.

## 7. Kubernetes operator

`CustomerIntegration` custom resource:

```yaml
apiVersion: pourline.dev/v1alpha1
kind: CustomerIntegration
metadata:
  name: rheinbau-gmbh
spec:
  channel: sftp            # sftp | webhook
  mappingConfigMap: rheinbau-gmbh-mapping
  schedule: "*/5 * * * *"  # sftp only
  replicas: 1
  paused: false
status:
  phase: Running
  lastSuccessfulIngest: ...
  deadLetterCount: 0
```

The operator reconciles the matching Deployment/CronJob, ConfigMap and Secret refs, updates `status`, and handles pause/resume and deletion (finalizer).

## 8. Observability

- `tracing` + OpenTelemetry: a single trace from an incoming order through ingestion → NATS → dispatch → plan diff. An action in the map console is one trace too, from the click through dispatch to the plan update the console pushes; a breakdown, which travels as an MQTT 3.1.1 command, is two (§9).
- Prometheus metrics: ingest throughput per customer, DLQ size, re-dispatch latency, pour-gap distribution, edge sync lag, and the console's clients, pushes and actions.
- A provisioned Grafana dashboard ("Dispatch overview") and Jaeger for traces, in compose and in the k8s cloud cluster.

## 9. Running it locally

`make up` needs only **Docker**. The Kubernetes targets also need **k3d, kubectl and Helm**, and Docker needs at least **4 CPUs and 8 GB RAM**. Rust stable is needed for development, or the optional devcontainer.

| Command | Result |
|---|---|
| `make up` | Docker Compose: Postgres, NATS (JetStream), Mosquitto, SFTP server, Prometheus, Grafana, Jaeger, every service but the map console, all simulators |
| `make demo` | Seeds customers and orders, streams truck data, injects a "truck breaks down" and a "site delayed 30 min" event, prints the plan diffs |
| `make k8s` | Creates two k3d clusters (`cloud`, `plant-walldorf`), installs Helm charts + operator, applies example `CustomerIntegration`s; serves the map console, Grafana and Jaeger at `/console`, `/grafana` and `/jaeger` on the cloud ingress |
| `make chaos` | Cuts the plant cluster's network for 2 minutes, then restores it; shows the outbox draining |
| `make test` | fmt, clippy, unit, property and integration tests |
| `make down` | Tears everything down |

Grafana, Jaeger and the dispatch API's OpenAPI/Swagger UI are linked from the README with their local ports.

**Local Kubernetes setup (what `make k8s` and `make chaos` automate)**

- Install on macOS: `brew install k3d kubectl helm` (optional: `k9s`, `tilt`). On Linux: Docker Engine + the k3d install script.
- A local image registry `k3d-pourline-registry:5050` (`k3d registry create`) that both clusters pull from; images are pushed as `localhost:5050/<service>:dev`. `k3d image import` is the fallback.
- Both clusters join one Docker network, `pourline-net`; the `cloud` cluster exposes its ingress on `localhost:8080`.
- The plant reaches the cloud through its uplink, a Toxiproxy in the cloud cluster that the map console can cut. Prometheus and the console watch the plant over a separate monitoring path, LoadBalancer ports on the plant node, so a cut of the uplink leaves the plant visible.
- `make chaos` runs `docker network disconnect pourline-net k3d-plant-walldorf-server-0`, waits 120 s, then reconnects.
- `make down` deletes both clusters and the registry.
- Optional `Tiltfile`: `tilt up` rebuilds and redeploys a service on every save.
- The README has a "Prerequisites" section with these install commands and a one-line check (`k3d version && kubectl version --client && helm version`); `make k8s` fails early with a clear message if a tool is missing.

**Map console (`make k8s` only)**

- Runs in the cloud cluster at `http://localhost:8080/console`, in the look of the Spotlight design system, with CSS of its own.
- Shows plants, sites and trucks on a map with a legend, and a panel with the plan version, the latest diff with its reasons, the Walldorf edge (links, outbox, sync lag, plan age) and a log of actions linking to their traces.
- Trucks are small mixer-truck icons coloured by their seven statuses; out of service adds an ×, so colour is never the only signal. Below zoom 13, trucks whose icons would overlap are grouped into one badge with their count, ringed by their statuses and keeping the ×; a click or Enter zooms in on them.
- Live updates take the real path: a snapshot, then server-sent events fed by NATS (`events.trucks.>`, `events.plants.>`, and `plans.updated`, which dispatch publishes after storing a version). Dispatch's `GET /world` gives the starting truck and plant state.
- Actions: break a truck down (the simulators' MQTT command), delay a site, take a plant offline or online, re-plan (all through dispatch's API), sync the plant now (an MQTT command the plant-agent subscribes to), and make chaos.
- Make chaos runs `make chaos`'s steps inside the cluster: orders near Walldorf, a wait until the plant holds their plan, a 2-minute cut of the plant's uplink, and a report of the tickets that waited out the cut. `make chaos` stays the hard cut, and first ends a cut the console made.
- Each action starts a trace of its own. Dispatch continues a W3C `traceparent` from callers that did not come through the ingress (no `X-Forwarded-For`), and only from them, so a site delay, a plant toggle or a re-plan is one trace from the click to the console's push, and a sync is one trace across both clusters. A breakdown is two: the simulators' MQTT 3.1.1 command carries no trace context, so ingest starts a new trace when the truck reports it.
- Changes need an `X-Pourline-Console` header and a `Host` from the console's public URL. The page sends a Content-Security-Policy that allows only its own files, Leaflet 1.9.4 from unpkg (pinned with SRI) and the basemap's tiles, and no inline script or style.
- The console acts only through dispatch's API, MQTT commands, the customers' webhooks and the plant's uplink, so the hard constraints of §4 stay with dispatch.

## 10. Repository layout

```
pourline/
├── crates/
│   ├── domain/
│   ├── dispatch/
│   ├── ingest/
│   ├── ingest-cli/
│   ├── plant-agent/
│   ├── operator/
│   ├── console/
│   └── simulators/
├── customers/            # mapping.yaml + sample files per fake customer
├── deploy/
│   ├── compose/
│   ├── helm/             # pourline/files/grafana holds the Grafana provisioning and dashboards
│   └── k3d/
├── docs/
│   ├── architecture.md   # diagrams (Mermaid)
│   └── adr/
├── .github/workflows/
├── Makefile
└── README.md
```

## 11. Tech choices

Rust stable, `tokio`, `axum`, `sqlx` (Postgres, compile-time checked queries, migrations), `async-nats` (JetStream), `rumqttc`, `serde` / `serde_yaml` / `quick-xml` / `csv`, `utoipa` (OpenAPI), `clap`, `thiserror` / `anyhow`, `tracing` + `opentelemetry-otlp`, `prometheus` or `metrics`, `kube` + `k8s-openapi`, `rusqlite` (edge outbox), `proptest`, `criterion`, `testcontainers`. The map console's page uses Leaflet with no build step; Toxiproxy is the plant's uplink in k8s.

## 12. CI (GitHub Actions)

- `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test` (unit + property).
- Integration tests with `testcontainers` (Postgres, NATS).
- Build multi-arch images and push to GHCR on `main`.
- End-to-end smoke test: kind cluster, Helm install, run a short demo scenario, assert that a plan was produced and no DLQ entries exist, that Prometheus scrapes every service, and that the map console answers.
- README badges for CI status.

## 13. Milestones

Each milestone ends with a runnable repo, green CI and a commit.

1. **Skeleton**: workspace, `domain` crate, compose stack with infra only, Makefile, CI with fmt/clippy/test, README stub.
2. **Simulators**: truck fleet over MQTT, three fake customer systems, fake plant. Visible in logs.
3. **Ingest**: mapping configs, canonical events, idempotency, DLQ + replay, `ingest-cli validate`. Integration tests.
4. **Dispatch v1**: data model + migrations, construction heuristic, REST API with OpenAPI, property tests.
5. **Re-dispatch**: event handling, local search, frozen loads, plan versions + diffs, benchmarks. `make demo` works end to end.
6. **Observability**: traces end to end, metrics, Grafana dashboard.
7. **Edge**: `plant-agent` with SQLite outbox, two k3d clusters, `make chaos`.
8. **Operator**: `CustomerIntegration` CRD + controller, Helm charts, e2e test in CI.
9. **Polish**: architecture doc with diagrams, 4–6 ADRs, demo GIF, README walkthrough.
10. **Map console**: a live map of the fleet in the k8s stack, with a status legend and actions (break down, delay, plant offline, re-plan, sync, make chaos); `plans.updated` and HTTP trace context in dispatch; the plant's uplink through Toxiproxy; Prometheus and Grafana in k8s.

## 14. ADRs to write

- NATS JetStream over Kafka for a small team with edge nodes
- Repair-based re-dispatch instead of full re-optimisation
- Declarative per-customer mappings instead of per-customer code
- SQLite outbox for edge store-and-forward
- Monorepo Cargo workspace
- Haversine travel time behind a trait (and when to switch to OSRM)

## 15. Conventions (for CLAUDE.md)

- No `unwrap()` / `expect()` outside tests and `main` startup.
- Errors: `thiserror` in libraries, `anyhow` in binaries.
- All config via environment variables with documented defaults; `.env.example` kept up to date.
- Every public function in `dispatch` has a doc comment; algorithmic modules have a module-level explanation.
- Tests live next to code; integration tests in `crates/*/tests/`.
- Conventional Commits; one milestone = one or more focused commits.
- Never commit secrets; local credentials only in `.env` (gitignored).
