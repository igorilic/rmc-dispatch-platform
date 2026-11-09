# Pourline

[![CI](https://github.com/igorilic/rmc-dispatch-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/igorilic/rmc-dispatch-platform/actions/workflows/ci.yml)

A ready-mix concrete dispatch platform in Rust. It plans which truck carries which load from
which batching plant, so every pour stays continuous and every load is discharged before it
sets. When a truck breaks down or a site runs late, it re-plans. The full design is in [SPEC.md](SPEC.md).

> **Status: milestone 1 of 9 (skeleton).** The shared `domain` crate and the local
> infrastructure exist. The services arrive in later milestones.

## Prerequisites

- **Docker** with Compose v2. This is all `make up` needs.
- **Rust** via [rustup](https://rustup.rs). `rust-toolchain.toml` pins 1.99.1, and rustup installs it on first use.
- Optional: `cargo install cargo-audit --locked` for `make audit`.

Kubernetes tooling (k3d, kubectl, Helm) is only needed from milestone 7 onwards.

## Commands

| Command | What it does |
|---|---|
| `make up` | Starts the local infrastructure and waits until it is up |
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

## Dependencies and security

Crates, GitHub Actions and container images are pinned to their latest releases. Dependabot
proposes updates every week. CI runs `cargo audit` on every push and on a weekly schedule.

## License

[MIT](LICENSE)
