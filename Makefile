.DEFAULT_GOAL := help
SHELL := /bin/bash

-include .env
export

FAKE_STREAM_URL := http://fake-stream:8090/v2/stream/recentchange
# Integration tests connect as admin. The password stays in the environment (from .env via
# `export` above) and is never expanded into a recipe line, so make can't echo it.
CH_TEST_ENV := CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_USER=admin CLICKHOUSE_PASSWORD="$$CLICKHOUSE_ADMIN_PASSWORD"

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

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

.PHONY: wait-live
wait-live: ## Wait up to 2 minutes for the running stack to serve live data
	@for _ in $$(seq 1 60); do \
		status=$$(curl -fsS localhost:8080/v1/wikipedia/live.json | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' 2>/dev/null || true); \
		[ "$$status" = "live" ] && echo "live" && exit 0; \
		sleep 2; \
	done; echo "no live data after 120 s"; exit 1

.PHONY: smoke
smoke: ## Check the running stack answers
	@curl -fsS localhost:8080/v1/wikipedia/live.json | python3 -c 'import json,sys; d=json.load(sys.stdin); print("status:", d["status"], "| last event age:", d["last_event_age_s"], "s | edits 5m:", d.get("langs",{}).get("all",{}).get("edits_5m"))'
	@curl -fsS -o /dev/null -w "activity: HTTP %{http_code}\n" "localhost:8080/v1/wikipedia/activity?lang=en&window=5m"
	@curl -fsS localhost:8000/readyz && echo

.PHONY: migrate
migrate: clickhouse ## Apply pending schema migrations (the stack does this on start)
	docker compose run --rm migrate

.PHONY: reconcile
reconcile: clickhouse ## Check the per-minute rollup against raw rows; REPAIR=1 stops ingest and rebuilds
ifdef REPAIR
	docker compose stop ingest
	sleep 30   # repair refuses while rows are still arriving
	docker compose run --rm migrate livedemos-reconcile --repair; \
		status=$$?; docker compose start ingest; exit $$status
else
	docker compose run --rm migrate livedemos-reconcile
endif

.PHONY: archive-hour
archive-hour: clickhouse ## Rewrite one hour's Parquet file from ClickHouse: HOUR=2026-10-06T09 (pauses the service)
	docker compose stop archive
	docker compose run --rm archive livedemos-archive --hour $(HOUR); \
		status=$$?; docker compose start archive; exit $$status

.PHONY: rebuild-rollups
rebuild-rollups: clickhouse ## Rebuild the per-minute rollup from the archive: FROM=2026-10-01 TO=2026-10-03 [ALLOW_MISSING=1] (stops ingest)
	docker compose stop ingest
	sleep 30   # the rebuild refuses while rows are still arriving
	docker compose run --rm migrate livedemos-rebuild --from $(FROM) --to $(TO) $(if $(ALLOW_MISSING),--allow-missing); \
		status=$$?; docker compose start ingest; exit $$status

.PHONY: bench
bench: clickhouse ## Benchmark every query at 7 days of retained data (separate database)
	$(CH_TEST_ENV) uv run python -m devtools.bench

.PHONY: e2e
e2e: ## Browser tests for the widget (start the stack first; LIVEDEMOS_E2E_BROWSER=firefox or webkit to switch)
	uv run --group e2e playwright install $${LIVEDEMOS_E2E_BROWSER:-chromium}
	uv run --group e2e pytest -m e2e -v

# Credentials come from your SSO profile: run `aws sso login` for your SSO session first.
TF_PROFILE ?= livedemos
TF := AWS_PROFILE=$(TF_PROFILE) terraform
AWS_REGION ?= eu-west-1

# CI runs the version in .terraform-version; a different one locally can rewrite the lock files.
.PHONY: tf-version
tf-version:
	@want=$$(cat .terraform-version); have=$$(terraform version -json | jq -r .terraform_version); \
	[ "$$want" = "$$have" ] || { echo "terraform $$have, but .terraform-version says $$want"; exit 1; }

.PHONY: tf-bootstrap
tf-bootstrap: tf-version ## AWS: create the Terraform state bucket (once per account)
	$(TF) -chdir=infra/bootstrap init -input=false
	$(TF) -chdir=infra/bootstrap apply

.PHONY: tf-init
tf-init: tf-version ## AWS: connect infra/live to the state bucket
	$(TF) -chdir=infra/live init -input=false -backend-config=backend.hcl

.PHONY: host-id
host-id: ## AWS: the live host's instance id: whichever holds the Elastic IP
	@AWS_PROFILE=$(TF_PROFILE) aws ec2 describe-addresses --region $(AWS_REGION) \
		--filters Name=tag:Name,Values=livedemos-host --query 'Addresses[0].InstanceId' --output text

.PHONY: tf-plan
tf-plan: tf-version ## AWS: show what infra/live would change, and save the plan
	$(TF) -chdir=infra/live plan -input=false -out=tfplan

.PHONY: tf-apply
tf-apply: tf-version ## AWS: apply exactly the plan saved by tf-plan
	$(TF) -chdir=infra/live apply -input=false tfplan

.PHONY: tf-lock
tf-lock: tf-version ## AWS: record provider checksums for macOS and Linux (commit the lock files)
	for stack in infra/bootstrap infra/live; do \
		terraform -chdir=$$stack providers lock -platform=darwin_arm64 -platform=linux_amd64 -platform=linux_arm64; \
	done

.PHONY: tf-fmt
tf-fmt: ## Format the Terraform code
	terraform fmt -recursive infra

.PHONY: tf-validate
tf-validate: ## Check the Terraform code without touching AWS
	terraform fmt -check -recursive infra
	for stack in infra/bootstrap infra/live; do \
		terraform -chdir=$$stack init -backend=false -input=false >/dev/null && terraform -chdir=$$stack validate || exit 1; \
	done

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
	docker compose up -d s3 s3-bucket   # the archive's local S3, for integration tests too

.PHONY: test-integration
test-integration: clickhouse ## Integration tests against ClickHouse, including the resume proof
	$(CH_TEST_ENV) uv run pytest -m integration

.PHONY: proof
proof: clickhouse ## Only the resume proof: SIGKILL ingest mid-stream, verify nothing lost or doubled
	$(CH_TEST_ENV) uv run pytest -m integration -k resume_proof -v
