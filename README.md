# Pourline

[![CI](https://github.com/igorilic/rmc-dispatch-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/igorilic/rmc-dispatch-platform/actions/workflows/ci.yml)

A ready-mix concrete dispatch platform in Rust. It plans which truck carries which load from
which batching plant, so every pour stays continuous and every load is discharged before it
sets. When a truck breaks down or a site runs late, it re-plans. The full design is in [SPEC.md](SPEC.md).

> **Status: milestone 2 of 9 (simulators).** The local stack runs a simulated world: 25 trucks
> sending telematics, three batching plants and three contractors sending orders. Nothing consumes
> these signals yet; ingest arrives in milestone 3.

## Prerequisites

- **Docker** with Compose v2. This is all `make up` needs.
- **Rust** via [rustup](https://rustup.rs). `rust-toolchain.toml` pins 1.99.1, and rustup installs it on first use.
- Optional: `cargo install cargo-audit --locked` for `make audit`.

Kubernetes tooling (k3d, kubectl, Helm) is only needed from milestone 7 onwards.

## Commands

| Command | What it does |
|---|---|
| `make up` | Builds and starts the local stack, and waits until it is up |
| `make logs` | Follows the simulators’ logs |
| `make test` | Runs the fmt check, clippy (`-D warnings`) and all tests |
| `make audit` | Checks the dependencies against the RustSec advisory database |
| `make down` | Stops everything and deletes the volumes |

`make demo` arrives with milestone 5. `make k8s` and `make chaos` arrive with milestone 7.

## Local services

Every port binds to `127.0.0.1` only.

| Service | Address | Credentials |
|---|---|---|
| Postgres | `localhost:5432` | `pourline` / `pourline`, database `pourline` |
| NATS (JetStream) | `localhost:4222`, monitoring at http://localhost:8222 | none |
| Mosquitto (MQTT) | `localhost:1883` | anonymous |
| SFTP | `localhost:2222` | `pourline` / `pourline` |
| Prometheus | http://localhost:9090 | none |
| Grafana | http://localhost:3000 | `admin` / `admin` (you're asked to change it on first login) |
| Jaeger UI | http://localhost:16686, with OTLP on 4317/4318 | none |

These credentials are throwaway local defaults. To override them, copy `.env.example` to `.env`, which is gitignored.

## Watching the simulators

The simulated world lives in [`deploy/seed/rhine-neckar.json`](deploy/seed/rhine-neckar.json),
around Heidelberg and Mannheim. It holds our master data: 3 customers and their 40 ship-tos
(sites), each with an address. 6 of the ship-tos have no coordinates and are placed at their
postal code's centroid. The world also has 3 plants, 25 trucks and 4 mix designs. The milestone 2
plan in `docs/context/specs/` has diagrams of how it fits together.

```sh
make logs                                          # status changes, batch tickets, orders, SFTP drops
C="docker compose -f deploy/compose/compose.yaml"
$C exec mosquitto mosquitto_sub -t 'trucks/#' -v   # live truck telematics
$C exec mosquitto mosquitto_sub -t 'plants/#' -v   # batch tickets
$C exec sftp ls -l /config/rheinbau-gmbh           # rheinbau-gmbh's CSV drops
```

| What | Channel | Notes |
|---|---|---|
| Truck telematics | MQTT `trucks/{id}/gps`, `/drum`, `/status` | GPS every 10 s while moving and every 60 s parked. Some messages are duplicated or reordered, and dead zones end in late bursts. `seq` and `ts_ms` let a consumer restore the order. |
| Batch tickets | MQTT `plants/{id}/tickets` | `batching`, `loading` and `loaded` for each load. A plant never runs more batches than it has loading bays. |
| `bauwerk-ag` orders | HTTP POST, JSON | Every change is re-sent as the full document, and 5 % are sent twice. Logged until a webhook URL is set. |
| `neckar-infra` orders | HTTP POST, XML | Schema v1 and v2 are live side by side. Logged until a webhook URL is set. |
| `rheinbau-gmbh` orders | SFTP, CSV | A full snapshot every 5 minutes: semicolons, decimal commas, Berlin local time. |

Every order references our customer number and ship-to number from the master data, in the
customer's own field names. Sample payloads for each customer are in [`customers/`](customers),
and the tests check the simulators against them.

| Variable | Default | Effect |
|---|---|---|
| `MQTT_URL` | `mqtt://localhost:1883` | Broker (compose sets the container address) |
| `SFTP_HOST`, `SFTP_PORT` | `localhost`, `2222` | SFTP server (compose sets the container address) |
| `SFTP_USER`, `SFTP_PASSWORD` | `pourline`, `pourline` | SFTP login, shared with the SFTP container |
| `BAUWERK_WEBHOOK_URL`, `NECKAR_WEBHOOK_URL` | unset | Where to POST orders. While unset, they are logged instead |
| `ORDER_INTERVAL_SECS` | `180` | Time between order changes per customer, ±50 % |
| `RHEINBAU_DROP_INTERVAL_SECS` | `300` | Time between SFTP drops |
| `TELEMATICS_DUPLICATE_RATE`, `TELEMATICS_REORDER_RATE` | `0.03`, `0.03` | Share of messages duplicated or held back |
| `DEAD_ZONE_TRIP_SHARE` | `0.2` | Share of trips that cross a dead zone |
| `SIM_SEED` | `42` | Seed for every random choice |
| `RUST_LOG` | `info` | Log filter. Use `simulators=debug` to see every telematics message |

To run the simulators outside Docker, stop the container first (`docker compose -f deploy/compose/compose.yaml stop simulators`), then run `cargo run -p simulators`.

## Dependencies and security

Crates, GitHub Actions and container images are pinned to their latest releases. Dependabot
proposes updates every week. CI runs `cargo audit` on every push and on a weekly schedule.

## License

[MIT](LICENSE)
