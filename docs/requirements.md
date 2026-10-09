# Technical requirements

What the live demos must do, and how well. Each requirement has an id so tests, ADRs and Linear issues can point at it. Numbers marked "target" are design goals until they're measured; the live page only ever shows measured values.

## Functional

| Id | Requirement |
|---|---|
| F1 | Consume Wikimedia EventStreams `recentchange` continuously and keep edits and new pages from English, Portuguese and German Wikipedia. |
| F2 | Drop Wikimedia canary (test) events, other wikis and other change types, and count each reason. |
| F3 | Survive any restart or disconnect without losing or duplicating an event, within the source's retention. |
| F4 | Record a visible gap when a resume isn't possible, instead of drawing zeros. |
| F5 | Serve a widget payload with: edits in the last 5 minutes, distinct pages, bot share, edits per minute for the last hour, top 5 most-edited articles. Per language and for all three together. |
| F6 | Serve an allowlisted ad hoc query ("Query it") that returns ClickHouse's own timing and rows read. |
| F7 | Embed as an iframe with `lang` and `theme` URL parameters and a live theme switch from the host page. |
| F8 | Show freshness ("Live, last event N s ago") and a plain "Paused" state when data stops. Never fake movement. |
| F9 | Run fully offline against a fake stream for development and tests. |
| F10 | Keep history beyond 7 days as Parquet on S3. |

## Non-functional

| Id | Area | Requirement |
|---|---|---|
| N1 | Freshness | Edit on Wikipedia to pixels under 5 s, target. Measured and displayed. |
| N2 | Freshness SLO | At least 99.9% of minutes over 30 days with newest-event age under 60 s (43 minutes of error budget). More nines are better. Shown on the Ops tab. |
| N3 | Scale | Database work doesn't grow with viewers: one snapshot per second regardless of traffic. |
| N4 | Latency | `live.json` served from memory; "Query it" under 100 ms at p95 for the 5-minute window, target. |
| N5 | Reliability | Unattended restarts: every long-running container has a restart policy; health checks report real liveness (ingest: the consumer loop, not just the process); a broken host is replaced by its Auto Scaling Group. Docker doesn't restart unhealthy containers on its own, so stalls are caught by alerts. |
| N6 | Recovery | A lost host is replaced and restores itself from the Parquet archive and the stream, with no manual data work. |
| N7 | Resources | Whole stack fits a 2 GB host, or measurements say otherwise and we resize. |
| N8 | Cost | Budget $10 a month, with email alerts at 80% of actual spend and 100% of forecast. About $5 a month until 31 Dec 2026. |
| N9 | Security | No public database port, read-only API user, allowlisted parameters, no SSH, no stored cloud keys in CI. |
| N10 | Observability | Metrics and structured logs from every service; alerts for stale data, no data, memory, disk and errors. |
| N11 | Maintainability | Lint, format, strict typing and tests in CI; decisions recorded as ADRs. |
| N12 | Etiquette | Descriptive User-Agent with contact details, per Wikimedia's policy. |

## How each is proven

| Requirement | Proof |
|---|---|
| F1, F2 | `tests/unit/test_events.py` |
| F3 | `tests/integration/test_resume_proof.py`: SIGKILL while an insert is running and an immediate restart, compared to ground truth per event, minute and language (`make proof`); an insert that commits but reports failure; one that fails fast and commits later. `tests/unit/test_consumer.py` for retrying the sealed batch and poison messages; `tests/unit/test_batch.py` for one-day batches, id-based tokens and sequence order across restarts |
| F4 | `tests/integration/test_storage.py::test_first_boot_and_too_old_bookmarks`, `tests/unit/test_snapshot.py` |
| F5 | `tests/unit/test_snapshot.py`, `tests/integration/test_storage.py::test_snapshot_from_real_rows` |
| F6 | `tests/unit/test_activity.py`, `tests/integration/test_storage.py::test_query_it_reports_clickhouse_timing` |
| F7, F8 | `tests/e2e/test_widget.py` in Chromium, Firefox and WebKit (`make e2e`, and CI): live numbers, keyboard use, focus kept across updates, screen-reader announcements only on change, 5-minute-old data shown as paused, reduced motion, axe-core with no violations in light and dark; `tests/unit/test_api.py::test_live_refuses_to_serve_a_stale_snapshot` |
| F9 | `make up-offline`, which CI runs before the browser tests |
| F10 | `tests/integration/test_archive.py`, `tests/unit/test_archive.py`; restoring a new host from it: ADR 0010's drill |
| N1 | Measured, not tested: the page shows ingest lag and the newest event's age as it fetches it |
| N2 | `tests/unit/test_slo.py` (minutes with no sample count against it), `tests/integration/test_ops.py`, `/v1/ops.json` |
| N3 | Snapshot loop design plus the 1 s edge cache; load checks locally and on CloudFront ([architecture](architecture.md#the-core-idea-compute-once-let-the-cdn-fan-out)) |
| N4 | `make bench` ([benchmarks](benchmarks.md)); on the host, "Query it" reports ClickHouse's own timing on every answer |
| N5 | `restart: unless-stopped` and health checks in `compose.yaml`; `tests/unit/test_health.py`; the group's EC2 health check (`infra/live/host.tf`); alerts in `deploy/grafana/alerts.json` |
| N6 | ADR 0010's drill: a new host restored itself in 7 min 50 s; `tests/unit/test_archive.py` and `tests/integration/test_archive.py` for the restore |
| N7 | `make bench` at 7 days and twice the live rate ([benchmarks](benchmarks.md)); memory measured in production on 2026-10-07 ([ADR 0010](adr/0010-spot-host-in-an-auto-scaling-group.md)) |
| N8 | `infra/live/budget.tf` |
| N9 | `clickhouse/users.d/livedemos.xml`, `tests/integration/test_storage.py::test_application_users_cant_exceed_their_role` (as the real users), `tests/unit/test_activity.py`, the Terraform plan on every pull request |
| N10 | `deploy/alloy/config.alloy`, `deploy/grafana/alerts.json` (five of the nine fired on purpose in a drill, [runbook](runbook.md#fire-drill)), `tests/unit/test_dashboard.py` |
| N11 | `.github/workflows/ci.yml` |
| N12 | `IngestSettings.user_agent` in `src/livedemos/config.py`; `make up` refuses to start without a contact |
