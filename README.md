# Pourline

[![CI](https://github.com/igorilic/rmc-dispatch-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/igorilic/rmc-dispatch-platform/actions/workflows/ci.yml)

A ready-mix concrete dispatch platform in Rust. It plans which truck carries which load from
which batching plant, so every pour stays continuous and every load is discharged before it
sets. When a truck breaks down or a site runs late, it repairs the plan and says why each load
moved.

![make demo: two orders are planned, a load starts batching and freezes, a truck breaks down and a site's pump is late; each re-dispatch prints its diff with a reason per load](docs/demo.gif)

A simulated world of 25 trucks, three batching plants and three contractors sends orders and
telematics. Ingest turns every customer's format into canonical events on NATS JetStream, through
a YAML mapping per customer, with no duplicates and a dead-letter queue for anything it can't map.
Dispatch keeps the orders in Postgres and re-plans by itself whenever something changes. It never
moves a load that is already batching, and every plan version passes a hard-constraint checker
before it is stored. One trace in Jaeger follows an order from the webhook to the re-dispatch that
planned it, and Grafana shows the plan, ingest and the edge at a glance. At the plant, a
plant-agent batches from its slice of the plan and keeps its tickets in a SQLite outbox, so a cut
link loses nothing. On Kubernetes, an operator runs each customer integration from a
`CustomerIntegration` resource.

- [Architecture](docs/architecture.md): how it fits together, with diagrams
- [Decisions](docs/adr/): six ADRs, from NATS over Kafka to haversine travel times
- [SPEC.md](SPEC.md): what it was built to do
- [Walkthrough](#walkthrough): see it all running in about half an hour

## Prerequisites

- **Docker** with Compose v2. This is all `make up` needs. `make test` needs it too: the
  integration tests start NATS and Postgres containers with testcontainers.
- **Rust** via [rustup](https://rustup.rs). `rust-toolchain.toml` pins 1.99.1, and rustup installs it on first use.
- Optional: `cargo install cargo-audit --locked` for `make audit`.
- For `make k8s` and `make chaos`: **k3d**, **kubectl** and **Helm**, and Docker with at least 4
  CPUs and 8 GB of memory. On macOS: `brew install k3d kubectl helm`; on Linux, Docker Engine,
  the [k3d install script](https://k3d.io) and [Helm's](https://helm.sh/docs/intro/install/).
  Check with `k3d version && kubectl version --client && helm version`.
- For `make e2e`, also **kind** and **jq**: `brew install kind jq`.

## Walkthrough

Steps 1 to 5 need only Docker and Rust; steps 6 to 8 also need the Kubernetes tools above. Each
step links to the section that explains what you see.

1. **Start the world.** `make up` builds the images and starts everything; the first build takes
   a while. Open Grafana at http://localhost:3000. The Dispatch overview fills in as the
   simulated customers send orders. ([Local services](#local-services),
   [Metrics](#metrics))
2. **Look at the plan.** Open the Swagger UI at http://localhost:8081/swagger-ui/, or ask for the
   latest plan: `curl -s localhost:8081/plans/latest | jq '.version, (.loads | length), .loads[0]'`.
   Each load names its order, plant, truck, batch time and ETA. ([Planning](#planning))
3. **Change reality.** `make demo` restarts the stack with empty volumes and plays the scenario
   in the GIF above: two orders, a load that starts batching and freezes, a truck breakdown and a
   late pump. Each re-dispatch prints its diff, with a reason per load. ([`make demo`](#make-demo))
4. **Follow one order.** Open the Jaeger link the demo prints at the end. One trace runs from the
   customer's webhook through NATS to the re-dispatch that planned the order. ([Traces](#traces))
5. **Break a mapping.** Follow the three steps under [Watching ingest](#watching-ingest): change
   one path in neckar-infra's mapping, watch its orders land in the dead-letter queue with
   `ingest-cli dlq list`, then fix the mapping and replay them.
6. **Move to Kubernetes.** `make down`, then `make k8s`. The operator runs each customer from a
   `CustomerIntegration` resource. Pause one and watch its phase change:
   ```sh
   kubectl --context k3d-cloud -n pourline get customerintegrations
   kubectl --context k3d-cloud -n pourline patch customerintegration bauwerk-ag --type merge -p '{"spec":{"paused":true}}'
   kubectl --context k3d-cloud -n pourline patch customerintegration bauwerk-ag --type merge -p '{"spec":{"paused":false}}'
   ```
   ([Kubernetes](#kubernetes), [The operator](#the-operator))
7. **Cut the plant off.** `make chaos` disconnects the Walldorf plant for two minutes. The
   plant-agent keeps batching, its outbox fills, and it drains in order once the link is back. On
   a fresh cluster the first cut can end before any ticket falls due; the script then says so, and
   the next `make chaos` catches some. ([`make chaos`](#make-chaos))
8. **Run what CI runs.** `make e2e` installs the cloud chart on a fresh kind cluster, plays the
   demo there and checks for a plan and an empty dead-letter queue. ([`make e2e`](#make-e2e))

`make down` removes everything again.

## Commands

| Command | What it does |
|---|---|
| `make up` | Builds and starts the local stack, and waits until it is up |
| `make demo` | Starts a fresh stack (deleting the volumes) and plays a re-dispatch scenario: see [Re-dispatch](#re-dispatch) |
| `make logs` | Follows the simulators’ logs |
| `make test` | Runs the fmt check, clippy (`-D warnings`) and all tests |
| `make bench` | Runs the planner benchmarks at 50, 200 and 1,000 loads (a few minutes) |
| `make audit` | Checks the dependencies against the RustSec advisory database |
| `make sqlx-prepare` | Refreshes `.sqlx/` after a query or migration changes (needs `make up` and `cargo install sqlx-cli --locked`) |
| `make k8s` | Runs Pourline in two k3d clusters, `cloud` and `plant-walldorf`: see [Kubernetes](#kubernetes) |
| `make chaos` | Cuts `plant-walldorf` off the cloud for 2 minutes, then reconnects it, and shows its outbox draining (after `make k8s`): see [`make chaos`](#make-chaos) |
| `make e2e` | Installs the cloud chart and the three customers on a fresh kind cluster, plays the demo, and checks for a plan and an empty DLQ, as CI does: see [`make e2e`](#make-e2e) |
| `make down` | Stops everything and deletes the volumes, and deletes the k3d and kind clusters, the registry and the network |

## Local services

Every port binds to `127.0.0.1` only.

| Service | Address | Credentials |
|---|---|---|
| Postgres | `localhost:5432` | `pourline` / `pourline`, database `pourline` |
| Ingest webhooks | http://localhost:8080/webhooks/{integration} | a bearer token per integration |
| Dispatch API | http://localhost:8081, Swagger UI at http://localhost:8081/swagger-ui/ | none |
| Walldorf's plant-agent | http://localhost:8082/status, `/healthz` and `/readyz` | none |
| NATS (JetStream) | `localhost:4222`, monitoring at http://localhost:8222 | none |
| Mosquitto (MQTT) | `localhost:1883` | anonymous |
| SFTP | `localhost:2222` | `pourline` / `pourline` |
| Prometheus | http://localhost:9090 | none |
| Grafana | http://localhost:3000, opens on the “Dispatch overview” dashboard | none to view; `admin` / `admin` to edit (you're asked to change it on first login) |
| Jaeger UI | http://localhost:16686, with OTLP on 4318 (HTTP) and 4317 (gRPC) | none |

These credentials are throwaway local defaults. To override them, copy `.env.example` to `.env`, which is gitignored.

## Watching the simulators

The simulated world lives in [`deploy/seed/rhine-neckar.json`](deploy/seed/rhine-neckar.json),
around Heidelberg and Mannheim. It holds our master data: 3 customers and their 40 ship-tos
(sites), each with an address. 6 of the ship-tos have no coordinates and are placed at their
postal code's centroid. The world also has 3 plants, 25 trucks and 4 mix designs. The [architecture doc](docs/architecture.md#context)
shows how it fits together.

```sh
make logs                                                         # status changes, tickets, orders, SFTP drops
docker exec pourline-mosquitto-1 mosquitto_sub -t 'trucks/#' -v   # live truck telematics
docker exec pourline-mosquitto-1 mosquitto_sub -t 'plants/#' -v   # batch tickets
docker exec pourline-sftp-1 ls -l /config/rheinbau-gmbh           # rheinbau-gmbh's CSV drops
```

| What | Channel | Notes |
|---|---|---|
| Truck telematics | MQTT `trucks/{id}/gps`, `/drum`, `/status` | GPS every 10 s while moving and every 60 s parked. Some messages are duplicated or reordered, and dead zones end in late bursts. `seq` and `ts_ms` let a consumer restore the order. |
| Batch tickets | MQTT `plants/{id}/tickets` | `batching`, `loading` and `loaded` for each load. A plant never runs more batches than it has loading bays. |
| `bauwerk-ag` orders | HTTP POST, JSON | Every change is re-sent as the full document, and 5 % are sent twice. |
| `neckar-infra` orders | HTTP POST, XML | Schema v1 and v2 are live side by side. |
| `rheinbau-gmbh` orders | SFTP, CSV | A full snapshot every 5 minutes: semicolons, decimal commas, Berlin local time. |

Every order references our customer number and ship-to number from the master data, in the
customer's own field names. Sample payloads for each customer are in [`customers/`](customers),
and the tests check the simulators against them.

| Variable | Default | Effect |
|---|---|---|
| `MQTT_URL` | `mqtt://localhost:1883` | Broker (compose sets the container address) |
| `SFTP_HOST`, `SFTP_PORT` | `localhost`, `2222` | SFTP server (compose sets the container address) |
| `SFTP_USER`, `SFTP_PASSWORD` | `pourline`, `pourline` | SFTP login, shared with the SFTP container |
| `BAUWERK_WEBHOOK_URL`, `NECKAR_WEBHOOK_URL` | unset (compose sets ingest) | Where to POST orders. While unset, they are logged instead |
| `WEBHOOK_TOKEN_BAUWERK_AG`, `WEBHOOK_TOKEN_NECKAR_INFRA` | `dev-token-<integration>` | Bearer tokens, shared with ingest |
| `ORDER_INTERVAL_SECS` | `180` | Time between order changes per customer, ±50 % |
| `RHEINBAU_DROP_INTERVAL_SECS` | `300` | Time between SFTP drops |
| `TELEMATICS_DUPLICATE_RATE`, `TELEMATICS_REORDER_RATE` | `0.03`, `0.03` | Share of messages duplicated or held back |
| `DEAD_ZONE_TRIP_SHARE` | `0.2` | Share of trips that cross a dead zone |
| `TRUCK_REPAIR_MINUTES` | `60` | How long a truck stays out of service after a breakdown (`sim/trucks/{id}/breakdown`) |
| `SIM_SEED` | `42` | Seed for every random choice |
| `RUST_LOG` | `info` | Log filter. Use `simulators=debug` to see every telematics message |

To run the simulators outside Docker, stop the container first (`docker compose -f deploy/compose/compose.yaml stop simulators`), then run `cargo run -p simulators`.

## Watching ingest

Ingest turns every order, telematics message and batch ticket into a canonical event on the
`EVENTS` stream (subjects `events.orders.*`, `events.trucks.*`, `events.plants.*`). Duplicates
are skipped, and anything it can't map goes to the `DLQ` stream with every error. The [architecture doc](docs/architecture.md#an-orders-path)
follows an order through the pipeline.

```sh
make logs                                    # simulators; for ingest: docker logs -f pourline-ingest-1
curl -s 'localhost:8222/jsz?streams=true'    # message counts of EVENTS and DLQ
docker run --rm --network pourline_default natsio/nats-box:0.20.2 \
  nats -s nats://nats:4222 sub 'events.orders.>'   # watch order events arrive
```

Each customer integration is described in [`customers/<name>/mapping.yaml`](customers):
where every field comes from and how to convert it (decimal commas, litres, local times),
which master-data customer it may order for, and how to recognise a duplicate. Changing a
customer's format means editing YAML, not Rust. `ingest-cli` helps:

```sh
cargo run -p ingest-cli -- validate --customer rheinbau-gmbh --file customers/rheinbau-gmbh/samples/broken.csv
cargo run -p ingest-cli -- dlq list
cargo run -p ingest-cli -- dlq replay --customer neckar-infra   # after fixing a mapping
```

To see a replay end to end, break a mapping, let orders fail, fix it, and replay:

1. In `customers/neckar-infra/mapping.yaml`, change `/Order/Delivery/@shipTo` to `@shipto`, then run
   `docker compose -f deploy/compose/compose.yaml restart ingest`.
2. Wait for neckar-infra v2 orders to fail: `cargo run -p ingest-cli -- dlq list --customer neckar-infra`.
3. Undo the change, restart ingest, and run `cargo run -p ingest-cli -- dlq replay --customer neckar-infra`.

| Variable | Default | Effect |
|---|---|---|
| `HTTP_ADDR` | `0.0.0.0:8080` | Webhook server |
| `NATS_URL` | `nats://localhost:4222` | JetStream (compose sets the container address) |
| `MQTT_URL`, `SFTP_*` | as for the simulators | Where telematics and CSV drops come from |
| `SFTP_POLL_SECS` | `30` | How often the SFTP drop is polled |
| `MAPPINGS_DIR` | `customers` | Where the `mapping.yaml` files are |
| `MAX_IN_FLIGHT` | `64` | Webhook requests processed at once. More get `503` with `Retry-After` |
| `DEDUP_TTL_DAYS` | `7` | How long order keys are remembered for duplicate detection |
| `WEBHOOK_TOKEN_<INTEGRATION>` | `dev-token-<integration>` | Bearer token per webhook integration |
| `INGEST_MODE` | `all` | What the process runs: `all` (compose), `mqtt` (telematics and batch tickets), `webhooks`, or `sftp-once` (one poll of each SFTP mapping, then exit 1 if a poll failed). In Kubernetes the chart runs `mqtt`, and the operator runs each customer integration in one of the last two |

## Planning

Dispatch keeps a Postgres copy of the master data and of every order, read from `events.orders.*`
on the `EVENTS` stream. Its durable consumer resumes after a restart, and an older revision of an
order never overwrites a newer one. A plan covers every open order (unconfirmed, pending or
confirmed) that starts in the next 48 hours, plus orders already being batched. Dispatch plans by
itself whenever something changes (see [Re-dispatch](#re-dispatch)). `POST /plans` plans
everything that isn't batching yet from scratch and stores the result as a new version.

```sh
curl -s 'localhost:8081/orders?status=pending' | jq length
curl -s -X POST localhost:8081/plans | jq '.version, .metrics, .unplanned'
curl -s localhost:8081/plans/latest | jq '.loads[0]'
```

Each order is split into full drums, with a smaller last load. Each load goes to the plant and
truck that get it to site earliest, and later loads follow on as the previous one finishes
discharging. Every plan keeps the hard constraints: one load per truck at a time, loads within
drum capacity, no more parallel loads than a plant has bays, discharge finished within the mix's
time limit, no out-of-service trucks or offline plants, and batching only during opening hours.
An order is planned whole, or listed in `unplanned` with the reason.

`crates/dispatch/src/verify.rs` re-checks those constraints independently of the planner, and
dispatch stores no plan that fails it. The property tests run it on random worlds, and the API
tests run it on plans made from stored orders. `metrics` measures the soft objectives: lateness,
pour gaps and empty kilometres.

The greedy plan is then improved by local search for up to 500 ms (`crates/dispatch/src/search.rs`).
It relocates single loads to other trucks and plants, and swaps the trucks of two loads, keeping
only moves that lower a weighted cost. That cost counts minutes beyond an order's gap limit (cold
joints, weighted heaviest), every gap minute, lateness and empty kilometres. Every candidate is
built with the heuristic's own slot search, so local search never breaks a hard constraint. At the
simulators' default order rate, demand still outgrows 25 trucks within hours, so most orders end up
planned late. [ADR 0002](docs/adr/0002-repair-based-redispatch.md) says why it repairs rather than re-optimises.

| Variable | Default | Effect |
|---|---|---|
| `DISPATCH_HTTP_ADDR` | `0.0.0.0:8081` | API server |
| `DATABASE_URL` | `postgres://pourline:pourline@localhost:5432/pourline` | Postgres (compose sets the container address) |
| `NATS_URL` | `nats://localhost:4222` | JetStream, shared with ingest |
| `LOCAL_SEARCH_BUDGET_MS` | `500` | Time local search gets per plan |
| `WEIGHT_COLD_JOINT_MINUTE` | `20` | Cost of a minute beyond an order's maximum gap |
| `WEIGHT_GAP_MINUTE` | `1` | Cost of any minute between two loads at a site |
| `WEIGHT_LATE_MINUTE` | `2` | Cost of a minute the first load arrives late |
| `WEIGHT_EMPTY_KM` | `0.5` | Cost of an empty kilometre |
| `WEIGHT_CHURN_LOAD` | `10` | Cost of moving a load during a re-dispatch |

Queries are checked at compile time against `.sqlx/`, so building needs no database. After you
change a query or a migration, run `make up` and then `make sqlx-prepare`.

## Re-dispatch

When reality changes, dispatch repairs the plan instead of starting over. Each of these is a
trigger:

| Trigger | Where it comes from |
|---|---|
| New order, changed order, cancellation | `events.orders.*`. A change counts if it touches volume, start, pour rate, gap, ship-to or mix |
| Truck out of service | `events.trucks.*`: the truck's telematics reports `out_of_service` |
| Plant offline or back online | `PUT /plants/{id}/status` with `{"status": "offline"}` or `"online"` |
| Site delayed | `POST /sites/{id}/delays` with `{"minutes": 30}`: the site's pours of the day start later |

Every trigger goes to one worker, which runs one re-dispatch at a time. Triggers that arrive in
the meantime are merged into the next run, so a burst becomes one version. A re-dispatch:

1. **Freezes** every load the plant has started batching. A batch ticket that names its order and
   load number (`order`, `load_no`) creates a delivery, and from then on the load is a fact. It
   stays even if its truck breaks down later: there are no returns.
2. **Keeps** the loads of every order no trigger touched, if they still fit at the same plant,
   truck and time. A load whose batch time has passed without batching sends its order to step 3.
3. **Plans** the touched, new and unplanned orders around them, then runs local search with a
   churn penalty, so one late pump doesn't reshuffle the fleet.
4. **Checks** the result against every hard constraint. If the repair fails the check, it plans
   everything again. If that fails too, the previous version stays.
5. **Stores** a new version, unless nothing changed, with the triggers and a diff that gives a
   reason for every added, removed or moved load.

```sh
curl -s localhost:8081/plans/latest | jq '.version, .previous_version, .triggers'
curl -s localhost:8081/plans/latest | jq -r '.version' | xargs -I{} curl -s localhost:8081/plans/{}/diff | jq '.changes[] | {order, sequence, kind, reason}'
curl -s -X PUT localhost:8081/plants/plant-walldorf/status -H 'content-type: application/json' -d '{"status":"offline"}' | jq '.diff.triggers'
docker exec pourline-mosquitto-1 mosquitto_pub -t sim/trucks/truck-07/breakdown -m ''   # a simulated truck breaks down
```

An order with a delivery reads as `in_progress`. In the live stack, Walldorf's
[plant-agent](#edge) batches Walldorf's planned loads on time, so they freeze as they would at a
real plant. Mannheim and Weinheim stay simulated: they batch for random trips without naming a
load. The simulated trucks still make random trips too, rather than following the plan. The
[architecture doc](docs/architecture.md#re-dispatch) has a diagram of the steps.

### `make demo`

`make demo` deletes the local volumes and starts a fresh stack, with order generation slowed so
the scenario stays readable. It then runs `crates/simulators/src/bin/demo.rs` in a container:

1. Two orders arrive through the customers' own channels: a bauwerk-ag JSON webhook and a
   neckar-infra XML webhook. Both start in about 45 minutes. The demo prints their plan.
2. It plays the plant: batch tickets for load #1 of the first order, on schedule, so it freezes.
3. The truck of a later load breaks down. The simulated truck reports `out_of_service` over MQTT,
   and dispatch re-dispatches.
4. The second order's site is delayed 30 minutes through the API.

Each diff is printed with a reason per load, for example:

```text
3. truck-12, which carries load #2 of 10001001/DEMO-080657-A, breaks down.
   v5 · truck-12 out of service
   order                    load  truck                  plant                ETA             reason
   10001001/DEMO-080657-A   #2    truck-12 → truck-16    weinheim             09:13           truck-12 is out of service
   10001001/DEMO-080657-A   #3    truck-03               mannheim             09:31 → 09:29   follows #2, so the pour stays continuous
   10001001/DEMO-080657-A   #4    truck-04               mannheim             09:45 → 09:43   follows #2, so the pour stays continuous
```

It ends with a Jaeger link to the first order's trace and the Grafana dashboard's address (see
[Observability](#observability)). When it's done, the simulators go back to the normal order
rate. A broken-down simulated truck is back in service after `TRUCK_REPAIR_MINUTES` (default 60).
Each step waits at most `DEMO_TIMEOUT_SECS` (default 120) for the stack.

### Benchmarks

`make bench` runs criterion on the seeded world, with one truck per 8 loads:

| Loads (trucks) | Greedy plan | Greedy plan + 100 local search moves | Repair, end to end |
|---|---|---|---|
| 56 (6) | 0.5 ms | 6.3 ms | 501 ms |
| 205 (25) | 7.8 ms | 33 ms | 504 ms |
| 1,084 (125) | 824 ms | 2.09 s | 916 ms |

Measured on a developer laptop; yours will differ.

A repair re-dispatches after the busiest truck breaks down, with the full 500 ms local search
budget, so 500 ms of each repair is search time. At 1,000 loads one local search move takes about
13 ms, because the bay check rescans every interval. That's fast enough for the 1 s target, and it
is the first thing to index if plans grow.

## Observability

Open Grafana at http://localhost:3000 for metrics and Jaeger at http://localhost:16686 for traces.

### Traces

Each delivery starts a trace in ingest: a webhook request, an SFTP file or an MQTT message. The
trace crosses NATS in the message's `traceparent` header and continues in dispatch:

```text
ingest   webhook          bauwerk-ag
ingest     publish        events.orders.10001001
dispatch     apply event  order 10001001/DEMO-100017-A, trigger new_order
dispatch       redispatch version 1, 9 changes
dispatch         load world · plan · save plan
```

A truck breakdown looks the same, starting from its `mqtt message`. The three API endpoints that
trigger a re-dispatch start their own traces. When the worker merges several triggers into one
run, the run joins the first trigger's trace and links to the others. A webhook's `202` response
carries the delivery's `trace_id`, so http://localhost:16686/trace/{trace_id} finds it. Dead
letters show as red spans.

GPS and drum readings aren't traced. At about two a second they would bury every other trace, and
they never trigger a re-dispatch. Status reports, batch tickets and every dead letter are traced.
Spans carry ids and counts, never payloads. Jaeger keeps them in memory, so `make down` clears them.

### Metrics

Prometheus (http://localhost:9090) scrapes ingest, dispatch and the plant-agent every 5 s, on
ports that stay inside the compose networks:

| Metric | What it measures |
|---|---|
| `pourline_ingest_records_total{integration, outcome}` | Records by customer (plus `telematics`, `plants`): `published`, `duplicate`, `dead_lettered` |
| `pourline_dlq_messages{integration}` | Dead letters waiting for a replay, read every 15 s |
| `pourline_redispatch_duration_seconds{outcome}` | One re-dispatch run, from loading the world to storing the version: `new_version`, `unchanged`, `failed` |
| `pourline_redispatch_triggers_total{kind}` | What drives re-planning, such as `new_order` or `truck_out_of_service` |
| `pourline_plan_pour_gaps{bucket}` | The current plan's waits between consecutive loads, per range of minutes: `0-5`, `5-10`, `10-20`, `20-30`, `30+` |
| `pourline_plan_*` | The current plan: version, planned and unplanned orders, loads, longest gap, gaps over the limit, lateness, empty km |
| `pourline_edge_connectivity{plant, link}` | 1 while the plant-agent reaches the broker (`mqtt`) or dispatch (`dispatch`) |
| `pourline_edge_outbox_messages{plant}` | Tickets waiting at the plant |
| `pourline_edge_sync_lag_seconds{plant}` | Edge sync lag: how long the oldest waiting ticket has waited |
| `pourline_edge_plan_age_seconds{plant}`, `pourline_edge_plan_version{plant}` | The plan the agent works from, and the time since dispatch last confirmed it |
| `pourline_edge_tickets_total{plant, outcome}` | Tickets `queued` into the outbox and `sent` from it |

The plan gauges always describe the current version. Dispatch sets them again from the latest
stored version when it starts. The “Dispatch overview” dashboard lives in
`deploy/grafana/dashboards/`: edit the JSON, not the UI. A test fails if it queries a metric that
no service defines.

| Variable | Default |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318`; compose sets `http://jaeger:4318`. If Jaeger is down, spans are dropped with a warning. |
| `METRICS_ADDR` (ingest) | `0.0.0.0:9464` |
| `DISPATCH_METRICS_ADDR` | `0.0.0.0:9465` |

## Edge

The plant-agent (`crates/plant-agent`) is the service that runs at a batching plant (SPEC §6). In
compose, Walldorf's agent runs next to the cloud services; [`make k8s`](#kubernetes) gives it a
cluster of its own.

- Every 15 s it fetches its plant's slice of the latest plan, `GET /plants/plant-walldorf/plan`,
  and keeps it in a SQLite file.
- When a load's batch time comes, it waits for one of the plant's loading bays, then issues
  `batching`, `loading` and `loaded` tickets that name the order and load number. Dispatch freezes
  the load at the first one.
- Every ticket goes into a SQLite outbox first. A forwarder sends the outbox over MQTT, on the
  topic the simulated plants use, one ticket at a time, and deletes a ticket only once the broker
  has acknowledged it.

When the cloud link is down, the agent keeps batching from its last plan, and the outbox fills up.
When the link is back, the outbox drains in order. Nothing is lost and nothing arrives twice:

- the outbox, the plan and the loads it has started survive a restart;
- a resent ticket carries the same bytes, so ingest drops the copy by its event id, which it keeps
  in the dedup KV for `DEDUP_TTL_DAYS`;
- Mosquitto persists what it has acknowledged.

The agent never batches a load twice, even if a newer plan moves it, and a restart resumes a load
where it stopped. A load more than 15 minutes overdue is left to dispatch. While the agent works
with Walldorf, the simulated Walldorf plant sends no tickets of its own (`AGENT_PLANTS`).

```sh
curl -s localhost:8082/status | jq        # links, outbox, sync lag, plan version and age, tickets
docker network disconnect pourline_default pourline-plant-agent-1   # cut Walldorf off the cloud
docker network connect pourline_default pourline-plant-agent-1      # and reconnect it
```

Prometheus reaches the agent over a second network, `edge`, so the dashboard's Edge row keeps
showing the outbox while the agent is cut off. `/readyz` answers once the agent has a plan,
fetched or stored, whatever the state of its links: an offline plant keeps working.

| Variable | Default |
|---|---|
| `PLANT_ID` | `plant-walldorf`, a plant in the seed, which gives the agent its bays and batching rate |
| `DISPATCH_URL` | `http://localhost:8081`; compose sets `http://dispatch:8081` |
| `MQTT_URL` | `mqtt://localhost:1883`; compose sets `mqtt://mosquitto:1883` |
| `PLANT_AGENT_DB` | `plant-agent.sqlite`; compose keeps `/data/plant-agent.sqlite` on a volume |
| `PLANT_AGENT_HTTP_ADDR` | `0.0.0.0:8082` |
| `PLANT_AGENT_METRICS_ADDR` | `0.0.0.0:9466` |
| `PLAN_POLL_SECS` | `15` |
| `AGENT_PLANTS` (simulators) | Empty; compose sets `plant-walldorf`, whose simulated plant then stays quiet |

## Kubernetes

`make k8s` runs the cloud and the plant in two k3d clusters, as SPEC §9 describes. It stops if
`localhost:8080` is taken, so run `make down` first if the compose stack is up. The two setups
are alternatives.

1. It creates the Docker network `pourline-net` with a fixed subnet, and a local registry,
   `k3d-pourline-registry`, on `localhost:5050`.
2. It creates the clusters from `deploy/k3d/cloud.yaml` and `deploy/k3d/plant-walldorf.yaml`, on
   that network, with a pinned k3s. The plant cluster gets a host alias, `cloud`, for the cloud
   node's address.
3. It builds the six images and pushes them as `localhost:5050/<service>:dev`.
4. It applies the CRD, installs the Helm chart `deploy/helm/pourline` into `cloud` with each image
   pinned to the digest it just pushed, and applies the three example customers from
   `deploy/integrations/`. It waits until the operator reports all three as `Running`.
5. It installs `deploy/helm/plant-agent` into `plant-walldorf`.

| Cluster | Runs | Reach it |
|---|---|---|
| `cloud` | Postgres, NATS, Mosquitto, SFTP, Jaeger, ingest (MQTT), dispatch, the simulators, the operator, and one workload per customer integration | http://localhost:8080: webhooks at `/webhooks/<name>`, the dispatch API everywhere else; `kubectl --context k3d-cloud` (API on `127.0.0.1:6550`) |
| `plant-walldorf` | the plant-agent, with its SQLite file on a volume | `kubectl --context k3d-plant-walldorf` (API on `127.0.0.1:6551`) |

The plant-agent fetches its plan from `http://cloud` through the cloud's ingress, and sends its
tickets to `mqtt://cloud:1883`, which the cloud's Mosquitto opens on its node. Prometheus and
Grafana run only in compose; Jaeger runs in both:
`kubectl --context k3d-cloud -n pourline port-forward svc/jaeger 16686`.

```sh
kubectl --context k3d-cloud -n pourline get customerintegrations
kubectl --context k3d-cloud -n pourline get pods
kubectl --context k3d-plant-walldorf -n pourline logs deploy/plant-agent -f
curl -s localhost:8080/plants/plant-walldorf/plan | jq '.version, (.loads | length)'
```

`make k8s` is safe to run again. It creates what is missing, rebuilds and pushes the images, and
upgrades both releases, so every service whose image changed rolls out. The infrastructure keeps
its data. `SKIP_BUILD=1 make k8s` skips the images, for a change to the charts only. Clusters
created before milestone 8 hold plain manifests that Helm won't take over: `make k8s` says so,
and one `make down` fixes it.

### The operator

Each customer integration is a `CustomerIntegration` resource (SPEC §7), and the operator in
`crates/operator` runs it as its own ingest workload. You onboard a customer with three objects,
as `deploy/integrations/` does for the three examples: the mapping as a ConfigMap, the credentials
as a Secret, and the resource that names both.

```yaml
apiVersion: pourline.dev/v1alpha1
kind: CustomerIntegration
metadata:
  name: rheinbau-gmbh                # the integration, as in customers/rheinbau-gmbh/
spec:
  channel: sftp                      # sftp | webhook; must match the mapping, can't change
  mappingConfigMap: rheinbau-gmbh-mapping
  credentialsSecret: rheinbau-gmbh-credentials   # sftp: username, password; webhook: token
  schedule: "*/5 * * * *"            # sftp only, and required for it
  replicas: 1                        # webhook pods; sftp allows only 1
  paused: false
```

```text
$ kubectl --context k3d-cloud -n pourline get customerintegrations
NAME            CHANNEL   PHASE     DEAD LETTERS   LAST INGEST   AGE
bauwerk-ag      webhook   Running   0              40s           12m
neckar-infra    webhook   Running   0              2m            12m
rheinbau-gmbh   sftp      Running   0              3m            12m
```

- A webhook integration runs as a Deployment, `ingest-<name>`, in `INGEST_MODE=webhooks`, with a
  Service and an Ingress for `/webhooks/<name>`. An sftp integration runs as a CronJob in
  `INGEST_MODE=sftp-once`: one poll per run, never two at once.
- Before a rollout the operator checks the mapping with ingest's own parser, checks that its
  channel matches, and checks that the Secret has the channel's keys. If a check fails, the phase
  is `Failed` and the message says why. The workload keeps running on the last valid mapping,
  because it mounts the operator's copy, not your ConfigMap. Fix the ConfigMap and the operator
  rolls it out. A changed mapping or Secret rolls the pods.
- `paused: true` scales a webhook integration to 0 or suspends the CronJob; `false` resumes it.
- The status is at most 30 s old. `deadLetterCount` counts the integration's dead letters.
  `lastSuccessfulIngest` is its last delivery in which a record was published or recognised as a
  duplicate, which ingest records in the NATS KV bucket `ingest-status`.
- Deleting an integration stops its workload before the resource goes (a finalizer), then
  removes its last ingest from NATS. Its dead letters stay, so a re-created integration can still
  replay them. Delete integrations before uninstalling the operator, or their finalizers keep them.
- The operator only acts in its own namespace (a Role), as one replica without leader election.

`ingest-cli` has no image yet. To replay dead letters in k3d, run
`kubectl --context k3d-cloud -n pourline port-forward svc/nats 4222` and use `ingest-cli`
locally with the mappings in `customers/`.

| Variable (operator) | Default | Effect |
|---|---|---|
| `INGEST_IMAGE` | `k3d-pourline-registry:5050/ingest:dev` | The image every integration runs. The chart sets it to its ingest image |
| `NATS_URL`, `SFTP_HOST`, `SFTP_PORT`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `RUST_LOG` | as for ingest | Passed on to every integration's workload; the operator reads NATS itself |
| `OPERATOR_METRICS_ADDR` | `0.0.0.0:9467` | Metrics listener |

### `make e2e`

CI's `e2e` job runs `make e2e` (SPEC §12), and so can you, with kind (`brew install kind`) and
`jq` on top of the Kubernetes prerequisites. `deploy/e2e/run.sh`:

1. creates the kind cluster `pourline-e2e` from a pinned node image, with its kubeconfig in
   `target/e2e/`, so `~/.kube/config` stays as it is;
2. lints both charts, builds five images (no plant-agent: one cluster tests the cloud) and loads
   them into kind;
3. installs the cloud chart with `deploy/e2e/values.yaml` and the three customers from
   `deploy/integrations/`, sets rheinbau's schedule to every minute, and waits until all three
   integrations are `Running`;
4. plays the demo scenario in a pod, which fails if an order isn't planned, a frozen load moves
   or a diff is missing;
5. checks that rheinbau's CronJob ingested a drop, that the latest plan has loads, that the DLQ
   holds nothing, and that every integration reports 0 dead letters.

On failure it prints the integrations, pods, events and logs. It deletes the cluster at the end,
unless `E2E_KEEP=1`. `SKIP_BUILD=1` reuses the `pourline/<service>:e2e` images, which saves
most of the run time.

### `make chaos`

`make chaos` runs `deploy/k3d/chaos.sh`:

1. It places three orders at the ship-tos nearest Walldorf, starting as soon as possible, through
   the demo image's `edge` mode. Simulated orders start 2 to 26 hours ahead, so without these the
   plant would have nothing to batch during the cut.
2. It waits until the agent has the plan, then runs
   `docker network disconnect pourline-net k3d-plant-walldorf-server-0`.
3. It prints the agent's status every 10 s for 120 s (`CHAOS_SECONDS`). It reads the status
   through the plant's own API server inside the node, which still answers while the node is
   cut off.
4. It reconnects the node with its old IP, because k3s doesn't survive a new one, and waits until
   the outbox is empty and both links are back. If the script is interrupted, it still
   reconnects the node.

```text
Cutting plant-walldorf off pourline-net for 120 s
  t+  0s  links up    outbox 0  sync lag    0s  plan v21,    2s old
  t+ 10s  links up    outbox 0  sync lag    0s  plan v21,   12s old
  t+ 20s  links down  outbox 3  sync lag    4s  plan v21,   22s old
  ...
  t+113s  links down  outbox 3  sync lag   96s  plan v21,  114s old
Reconnected
  t+128s  links down  outbox 0  sync lag    0s  plan v21,  130s old
  t+134s  links up    outbox 0  sync lag    0s  plan v21,    0s old
✓ 3 tickets waited out the cut, and all were sent in order within 11 s of reconnecting
```

The agent notices the cut within about 15 s. It keeps batching from the plan it has, and its
tickets wait in the outbox. Within seconds of reconnecting they reach ingest, oldest first, and
the plan sync catches up. `make k8s` deploys each image by digest, so a plant pod that restarts
during a cut starts from the image cached on its node, and finds its outbox and plan in SQLite.

During a cut, dispatch doesn't know the plant is cut off. If something else triggers a
re-dispatch meanwhile, such as a new order, the re-dispatch may move a Walldorf load whose batch
time passed without a ticket, although Walldorf batched it from its last plan. Once the tickets
arrive, the next re-dispatch freezes the load where Walldorf actually batched it.

## Images

Every push to `main` that passes all of CI publishes each service as a multi-arch image, for
`linux/amd64` and `linux/arm64`:

```text
ghcr.io/igorilic/rmc-dispatch-platform/dispatch
ghcr.io/igorilic/rmc-dispatch-platform/ingest
ghcr.io/igorilic/rmc-dispatch-platform/simulators
ghcr.io/igorilic/rmc-dispatch-platform/demo
ghcr.io/igorilic/rmc-dispatch-platform/plant-agent
ghcr.io/igorilic/rmc-dispatch-platform/operator
```

Each image is tagged `sha-<first 7 characters of the commit>`, and `main` points at the newest.
There is no `latest`. The `make` targets always build from your checkout. To install the cloud
chart from the published images instead, on a cluster of your own:

```sh
kubectl apply --server-side -f deploy/helm/pourline/crds/
helm upgrade --install pourline deploy/helm/pourline --skip-crds -n pourline --create-namespace \
  --set images.ingest=ghcr.io/igorilic/rmc-dispatch-platform/ingest:main \
  --set images.dispatch=ghcr.io/igorilic/rmc-dispatch-platform/dispatch:main \
  --set images.simulators=ghcr.io/igorilic/rmc-dispatch-platform/simulators:main \
  --set images.operator=ghcr.io/igorilic/rmc-dispatch-platform/operator:main \
  --set imagePullPolicy=Always
kubectl kustomize --load-restrictor LoadRestrictionsNone deploy/integrations | kubectl apply -f -
```

On a cluster without a load balancer, such as kind, also set `mosquitto.serviceType=ClusterIP`.

## Dependencies and security

Crates, GitHub Actions and container images are pinned to their latest releases. Dependabot
proposes updates every week. CI runs `cargo audit` on every push and on a weekly schedule.

The plant-agent is a Cargo workspace of its own, with its own `Cargo.lock`. Its SQLite library,
`rusqlite` 0.42, needs `libsqlite3-sys` 0.40, which bundles SQLite 3.54.1. sqlx 0.9.0 in the root
workspace caps `libsqlite3-sys` below 0.38, even though it never compiles SQLite, and one lockfile
can hold only one crate that links SQLite. `make test` and `make audit` cover both workspaces,
which share `target/`. The agent moves back into the root workspace once sqlx allows a newer
`libsqlite3-sys`.

## License

[MIT](LICENSE)
