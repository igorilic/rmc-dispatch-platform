# Optional overrides live in .env (copy .env.example); compose falls back to its defaults.

# Integration tests start containers via testcontainers, which needs the Docker socket of the
# current context (colima, Docker Desktop, or /var/run/docker.sock on CI).
export DOCKER_HOST ?= $(shell docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)
COMPOSE := docker compose -f deploy/compose/compose.yaml $(if $(wildcard .env),--env-file .env)

.PHONY: up down logs test bench audit sqlx-prepare

up: ## Build and start the local stack, and wait until it is up
	$(COMPOSE) up -d --wait --build

down: ## Stop the stack and delete its volumes
	$(COMPOSE) down -v --remove-orphans

logs: ## Follow the simulators' logs
	$(COMPOSE) logs -f simulators

test: ## fmt, clippy, unit/property/integration tests
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --workspace

bench: ## Planner benchmarks at 50, 200 and 1,000 loads (takes a few minutes)
	cargo bench -p dispatch --bench planner

audit: ## Check dependencies against the RustSec advisory DB (needs cargo-audit)
	cargo audit
	@# .cargo/audit.toml ignores a SQLite advisory because SQLite is never compiled; keep it that way.
	@test -z "$$(cargo tree --workspace --target all -e normal,build -i libsqlite3-sys 2>/dev/null)" \
		|| { echo "libsqlite3-sys is now compiled: drop the ignore in .cargo/audit.toml"; exit 1; }

# Compile-time-checked queries (sqlx) read .sqlx/ offline; this refreshes it from the compose Postgres.
DATABASE_URL ?= postgres://pourline:pourline@localhost:5432/pourline
sqlx-prepare: ## Refresh .sqlx/ after changing a query or migration (needs make up and sqlx-cli)
	sqlx migrate run --source crates/dispatch/migrations --database-url $(DATABASE_URL)
	DATABASE_URL=$(DATABASE_URL) SQLX_OFFLINE=false cargo sqlx prepare --workspace -- --all-targets
