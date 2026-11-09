# Optional overrides live in .env (copy .env.example); compose falls back to its defaults.
COMPOSE := docker compose -f deploy/compose/compose.yaml $(if $(wildcard .env),--env-file .env)

.PHONY: up down logs test audit

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

audit: ## Check dependencies against the RustSec advisory DB (needs cargo-audit)
	cargo audit
