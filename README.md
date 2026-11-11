# Pourline

[![CI](https://github.com/igorilic/rmc-dispatch-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/igorilic/rmc-dispatch-platform/actions/workflows/ci.yml)

A ready-mix concrete dispatch platform in Rust. It plans which truck carries which load from
which batching plant, so every pour stays continuous and every load is discharged before it
sets. When a truck breaks down or a site runs late, it re-plans. The full design is in [SPEC.md](SPEC.md).

> **Status: milestone 6 of 9 (observability).** A simulated world of 25 trucks, three batching
> plants and three contractors sends orders and telematics. Ingest turns all of it into canonical
> events on NATS JetStream, with no duplicates, and keeps a dead-letter queue for anything it can't
> map. Dispatch keeps every order in Postgres and re-plans by itself when something changes: a new
> or changed order, a truck breaking down, a plant going offline, a site running late. Loads that
> are already batching never move, every plan version passes a hard-constraint checker, and every
> diff says why each load moved. One trace in Jaeger follows an order from the webhook to the
> re-dispatch that planned it, and Grafana shows the plan, re-dispatching and ingest at a glance.
> `make demo` shows it end to end.

## Prerequisites

- **Docker** with Compose v2. This is all `make up` needs. `make test` needs it too: the
  integration tests start NATS and Postgres containers with testcontainers.
- **Rust** via [rustup](https://rustup.rs). `rust-toolchain.toml` pins 1.99.1, and rustup installs it on first use.
- Optional: `cargo install cargo-audit --locked` for `make audit`.

Kubernetes tooling (k3d, kubectl, Helm) is only needed from milestone 7 onwards.

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
| `make down` | Stops everything and deletes the volumes |

`make k8s` and `make chaos` arrive with milestone 7.

## Local services

Every port binds to `127.0.0.1` only.

| Service | Address | Credentials |
|---|---|---|
| Postgres | `localhost:5432` | `pourline` / `pourline`, database `pourline` |
| Ingest webhooks | http://localhost:8080/webhooks/{integration} | a bearer token per integration |
| Dispatch API | http://localhost:8081, Swagger UI at http://localhost:8081/swagger-ui/ | none |
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
postal code's centroid. The world also has 3 plants, 25 trucks and 4 mix designs. The milestone 2
plan in `docs/context/specs/` has diagrams of how it fits together.

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
are skipped, and anything it can't map goes to the `DLQ` stream with every error. The milestone 3
plan in `docs/context/specs/` has diagrams of the pipeline.

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
planned late. The milestone 4 plan in `docs/context/specs/` has diagrams.

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

An order with a delivery reads as `in_progress`. The simulated trucks still make random trips,
and the simulated plants batch for them without naming a load. In the live stack, loads only
freeze when `make demo` plays the plant. Trucks following the plan arrive with milestone 7's
plant-agent. The milestone 5 plan in `docs/context/specs/` has diagrams.

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

Prometheus (http://localhost:9090) scrapes ingest and dispatch every 5 s, on ports that stay
inside the compose network:

| Metric | What it measures |
|---|---|
| `pourline_ingest_records_total{integration, outcome}` | Records by customer (plus `telematics`, `plants`): `published`, `duplicate`, `dead_lettered` |
| `pourline_dlq_messages{integration}` | Dead letters waiting for a replay, read every 15 s |
| `pourline_redispatch_duration_seconds{outcome}` | One re-dispatch run, from loading the world to storing the version: `new_version`, `unchanged`, `failed` |
| `pourline_redispatch_triggers_total{kind}` | What drives re-planning, such as `new_order` or `truck_out_of_service` |
| `pourline_plan_pour_gaps{bucket}` | The current plan's waits between consecutive loads, per range of minutes: `0-5`, `5-10`, `10-20`, `20-30`, `30+` |
| `pourline_plan_*` | The current plan: version, planned and unplanned orders, loads, longest gap, gaps over the limit, lateness, empty km |

The plan gauges always describe the current version. Dispatch sets them again from the latest
stored version when it starts. The “Dispatch overview” dashboard lives in
`deploy/grafana/dashboards/`: edit the JSON, not the UI. A test fails if it queries a metric that
no service defines. Edge sync lag arrives with milestone 7's plant-agent.

| Variable | Default |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318`; compose sets `http://jaeger:4318`. If Jaeger is down, spans are dropped with a warning. |
| `METRICS_ADDR` (ingest) | `0.0.0.0:9464` |
| `DISPATCH_METRICS_ADDR` | `0.0.0.0:9465` |

## Dependencies and security

Crates, GitHub Actions and container images are pinned to their latest releases. Dependabot
proposes updates every week. CI runs `cargo audit` on every push and on a weekly schedule.

## License

[MIT](LICENSE)
