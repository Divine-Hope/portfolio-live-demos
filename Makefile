.DEFAULT_GOAL := help
SHELL := /bin/bash

-include .env
export

FAKE_STREAM_URL := http://fake-stream:8090/v2/stream/recentchange
CH_TEST_ENV := CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_USER=admin CLICKHOUSE_PASSWORD=$(CLICKHOUSE_ADMIN_PASSWORD)

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.env:
	@cp .env.example .env
	@echo "Created .env from .env.example. Set INGEST_CONTACT before 'make up'."

.PHONY: setup
setup: .env ## Install Python deps (uv) and create .env
	uv sync

.PHONY: up
up: .env ## Run the stack against the real Wikimedia stream
	@if [ -z "$(INGEST_CONTACT)" ]; then \
		echo "Set INGEST_CONTACT in .env (an email or URL, per Wikimedia's User-Agent policy)."; \
		echo "Or run 'make up-offline' to use the fake stream."; exit 1; fi
	docker compose up -d --build
	@$(MAKE) --no-print-directory urls

.PHONY: up-offline
up-offline: .env ## Run the stack against the fake stream (no internet needed)
	INGEST_STREAM_URL=$(FAKE_STREAM_URL) docker compose --profile offline up -d --build
	@$(MAKE) --no-print-directory urls

.PHONY: urls
urls:
	@echo ""
	@echo "  Widget in a demo host page   http://localhost:8080"
	@echo "  Widget alone                 http://localhost:8080/embed/wikipedia/"
	@echo "  Snapshot JSON                http://localhost:8080/v1/wikipedia/live.json"
	@echo "  Query it                     http://localhost:8080/v1/wikipedia/activity?lang=en&window=1h"
	@echo "  API docs                     http://localhost:8000/docs"
	@echo ""

.PHONY: down
down: ## Stop the stack (keeps data)
	docker compose --profile offline down

.PHONY: clean
clean: ## Stop the stack and delete its data
	docker compose --profile offline down -v

.PHONY: logs
logs: ## Follow ingest and api logs
	docker compose logs -f ingest api

.PHONY: ps
ps: ## Show service status
	docker compose --profile offline ps

.PHONY: smoke
smoke: ## Check the running stack answers
	@curl -fsS localhost:8080/v1/wikipedia/live.json | python3 -c 'import json,sys; d=json.load(sys.stdin); print("status:", d["status"], "| last event age:", d["last_event_age_s"], "s | edits 5m:", d.get("langs",{}).get("all",{}).get("edits_5m"))'
	@curl -fsS -o /dev/null -w "activity: HTTP %{http_code}\n" "localhost:8080/v1/wikipedia/activity?lang=en&window=5m"
	@curl -fsS localhost:8000/readyz && echo

.PHONY: e2e
e2e: ## Browser tests for the widget (start the stack first: make up-offline)
	uv run --group e2e playwright install chromium
	uv run --group e2e pytest -m e2e -v

.PHONY: lint
lint: ## Ruff (lint + format check) and mypy
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

.PHONY: fmt
fmt: ## Format and auto-fix
	uv run ruff format .
	uv run ruff check --fix .

.PHONY: test
test: ## Unit tests (no services needed)
	uv run pytest

.PHONY: clickhouse
clickhouse: .env
	docker compose up -d --wait clickhouse

.PHONY: test-integration
test-integration: clickhouse ## Integration tests against ClickHouse, including the resume proof
	$(CH_TEST_ENV) uv run pytest -m integration

.PHONY: proof
proof: clickhouse ## Only the resume proof: SIGKILL ingest mid-stream, verify nothing lost or doubled
	$(CH_TEST_ENV) uv run pytest -m integration -k resume_proof -v
