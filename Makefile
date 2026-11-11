# Optional overrides live in .env (copy .env.example); compose falls back to its defaults.

# Integration tests start containers via testcontainers, which needs the Docker socket of the
# current context (colima, Docker Desktop, or /var/run/docker.sock on CI).
export DOCKER_HOST ?= $(shell docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)
COMPOSE := docker compose -f deploy/compose/compose.yaml $(if $(wildcard .env),--env-file .env)
# The plant-agent is its own Cargo workspace; crates/plant-agent/Cargo.toml says why.
AGENT := --manifest-path crates/plant-agent/Cargo.toml

.PHONY: up down demo logs k8s test bench audit sqlx-prepare

up: ## Build and start the local stack, and wait until it is up
	$(COMPOSE) up -d --wait --build

down: ## Stop the stack and delete its volumes; delete the k3d clusters, registry and network too
	$(COMPOSE) down -v --remove-orphans
	@if command -v k3d >/dev/null; then \
		for cluster in cloud plant-walldorf; do \
			k3d cluster get $$cluster >/dev/null 2>&1 && k3d cluster delete $$cluster; \
		done; \
		k3d registry get pourline-registry >/dev/null 2>&1 && k3d registry delete pourline-registry; \
	fi; \
	docker network inspect pourline-net >/dev/null 2>&1 && docker network rm pourline-net >/dev/null; true

k8s: ## Two k3d clusters, cloud and plant-walldorf, with Pourline deployed (needs k3d and kubectl)
	deploy/k3d/k8s.sh

demo: ## Fresh stack, then a scripted scenario: two orders, a batching load, a breakdown, a site delay
	$(COMPOSE) down -v --remove-orphans
	ORDER_INTERVAL_SECS=3600 $(COMPOSE) up -d --wait --build
	$(COMPOSE) --profile demo run --rm --build demo
	@# Back to the normal order rate.
	$(COMPOSE) up -d --wait simulators

logs: ## Follow the simulators' logs
	$(COMPOSE) logs -f simulators

test: ## fmt, clippy, unit/property/integration tests, for both workspaces
	cargo fmt --all --check
	cargo fmt $(AGENT) --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo clippy $(AGENT) --all-targets -- -D warnings
	cargo test --workspace
	cargo test $(AGENT)

bench: ## Planner benchmarks at 50, 200 and 1,000 loads (takes a few minutes)
	cargo bench -p dispatch --bench planner

audit: ## Check dependencies against the RustSec advisory DB (needs cargo-audit)
	cargo audit
	@# .cargo/audit.toml ignores a SQLite advisory because SQLite is never compiled; keep it that way.
	@test -z "$$(cargo tree --workspace --target all -e normal,build -i libsqlite3-sys 2>/dev/null)" \
		|| { echo "libsqlite3-sys is now compiled: drop the ignore in .cargo/audit.toml"; exit 1; }
	@# cargo audit reads .cargo/audit.toml from the directory it runs in, so that ignore doesn't
	@# apply to the plant-agent's lockfile, which really compiles SQLite.
	cd crates/plant-agent && cargo audit

# Compile-time-checked queries (sqlx) read .sqlx/ offline; this refreshes it from the compose Postgres.
DATABASE_URL ?= postgres://pourline:pourline@localhost:5432/pourline
sqlx-prepare: ## Refresh .sqlx/ after changing a query or migration (needs make up and sqlx-cli)
	sqlx migrate run --source crates/dispatch/migrations --database-url $(DATABASE_URL)
	DATABASE_URL=$(DATABASE_URL) SQLX_OFFLINE=false cargo sqlx prepare --workspace -- --all-targets
