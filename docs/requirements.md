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
| F10 | Keep history beyond 7 days as Parquet on S3 (M4). |

## Non-functional

| Id | Area | Requirement |
|---|---|---|
| N1 | Freshness | Edit on Wikipedia to pixels under 5 s, target. Measured and displayed. |
| N2 | Freshness SLO | 99% of minutes over 30 days with newest-event age under 60 s, target. Shown on the Ops tab (M6). |
| N3 | Scale | Database work doesn't grow with viewers: one snapshot per second regardless of traffic. |
| N4 | Latency | `live.json` served from memory; "Query it" under 100 ms at p95 for the 5-minute window, target. |
| N5 | Reliability | Unattended restarts: every long-running container has a restart policy; health checks report real liveness (ingest: the consumer loop, not just the process); the host has auto-recovery (M4). Docker doesn't restart unhealthy containers on its own, so stalls are caught by alerts (M4). |
| N6 | Recovery | A lost host is rebuilt with `terraform apply` and backfills without manual data work (M4). |
| N7 | Resources | Whole stack fits a 2 GB host, or the measured week says otherwise and we resize (M5). |
| N8 | Cost | About $6 a month until 31 Dec 2026; budget alert before EUR 10 (M3). |
| N9 | Security | No public database port, read-only API user, allowlisted parameters, no SSH, no stored cloud keys in CI. |
| N10 | Observability | Metrics and structured logs from every service; alerts for stale data, no data, memory, disk and errors (M4). |
| N11 | Maintainability | Lint, format, strict typing and tests in CI; decisions recorded as ADRs. |
| N12 | Etiquette | Descriptive User-Agent with contact details, per Wikimedia's policy. |

## How each is proven

| Requirement | Proof |
|---|---|
| F1, F2 | `tests/unit/test_events.py` |
| F3 | `tests/integration/test_resume_proof.py`: SIGKILL mid-stream compared to ground truth (`make proof`), and an insert that commits but reports failure; `tests/unit/test_batch.py` for one-day batches and id-based tokens |
| F4 | `tests/integration/test_storage.py::test_first_boot_and_too_old_bookmarks`, `tests/unit/test_snapshot.py` |
| F5 | `tests/unit/test_snapshot.py`, `tests/integration/test_storage.py::test_snapshot_from_real_rows` |
| F6 | `tests/unit/test_activity.py`, `tests/integration/test_storage.py::test_query_it_reports_clickhouse_timing` |
| F7, F8 | `tests/e2e/test_widget.py` in a real browser (`make e2e`, and the CI stack job): live numbers, keyboard use, focus kept across updates, screen-reader announcements only on change, 5-minute-old data shown as paused, reduced motion, axe-core with no violations in light and dark; `tests/unit/test_api.py::test_live_refuses_to_serve_a_stale_snapshot` |
| F9 | `make up-offline` |
| N3 | Snapshot loop design plus the 1 s edge cache; load check in M3 |
| N7 | Measured week (M5) |
| N9 | `clickhouse/users.d/livedemos.xml`, `tests/unit/test_activity.py`, Terraform review (M3) |
| N11 | `.github/workflows/ci.yml` |
