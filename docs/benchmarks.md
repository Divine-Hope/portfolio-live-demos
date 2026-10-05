# Benchmarks

What the API's and ingest's queries cost at full retained volume, and what the stack costs
while it runs.
Rerun the first part with `make bench`; it uses its own database (`demos_bench`) and
leaves the stack's data alone.

## Queries at 7 days of data

### Setup

- **Data:** exactly 7 days of synthetic edits at 11 a second, twice the live rate measured
  on 2026-10-04 (en 3.6/s, de 1.0/s, pt 0.9/s): 6.65 million rows, 296 MB compressed.
  Language mix, bot share and a skewed title distribution follow the live stream.
- **Limits:** the `api` profile's limits (2 threads, 200 MB, 3 s) applied to every query as
  query settings, including the whole-snapshot timing. The benchmark connects as admin,
  because the `api` user can only read the real `demos` database; the other `api`
  constraints (read-only, settings can't be raised) are tested separately in
  `test_application_users_cant_exceed_their_role`.
- **Runs:** 20 per query. Timing and rows read are ClickHouse's own; peak memory is from
  `system.query_log`.
- **Machine:** an Apple Silicon laptop, ClickHouse 25.8 in Docker under the stack's 1.2 GB
  container limit and `clickhouse/config.d/low-memory.xml`. Not the production instance
  (a t4g.small has 2 vCPUs); measuring there is the M5 measured week.

### Results, 2026-10-05

| Query | p50 ms | p95 ms | Rows read | Peak memory |
|---|---:|---:|---:|---:|
| snapshot: newest event | 0.6 | 0.9 | 41 | 4.2 MB |
| snapshot: 5 min totals | 2.0 | 2.6 | 23,227 | 4.2 MB |
| snapshot: top articles | 2.2 | 2.4 | 23,227 | 4.2 MB |
| snapshot: per minute (rollup + current minute from raw) | 1.7 | 41.2 | 32,279 | 4.2 MB |
| snapshot: ingest lag | 1.4 | 1.9 | 24,619 | 4.4 MB |
| snapshot: recent gaps | 0.4 | 1.2 | 0 | 3.1 MB |
| query it: 1 h, all langs | 6.7 | 8.2 | 57,387 | 5.8 MB |
| query it: 24 h, all langs | 71.8 | 1,659.1 | 975,019 | 75.4 MB |
| resume: max ingest_seq | 2.7 | 4.0 | 176 | 4.2 MB |
| resume: last 20k by ingest order | 6.1 | 13.1 | 41,003 | 23.7 MB |

- Whole snapshot build, five queries at once: p50 10 ms, p95 15 ms.
- Insert throughput, 5,000-row JSON batches through the rollup view (ingest's own write
  path): about 140,000 rows a second. Catching up an hour's outage (about 20,000 rows)
  takes well under a second of insert time; the stream's replay speed is the limit.
- The p95 column is noisy: the queries run while ClickHouse is still merging the freshly
  loaded data. An earlier run put the 24 h p95 at 776 ms. Both are inside the 3 s limit.

### What it says

- The per-second snapshot costs the same at 7 days as at 7 minutes: every query reads only
  its window, by primary key, or the rollup.
- Resume stays bounded: a few hundred rows to find the newest sequence, about 41,000 for
  the tail, whatever the table's size (migration 0002).
- "Query it" over 24 hours is the one expensive query: a million rows, because counting
  distinct pages needs raw rows. It stays inside the API user's limits. Caching, request
  coalescing and the admission cap mean it runs at most once per language set every 10 s,
  and never more than 2 at once. If it becomes a problem, the fix is a per-hour
  `uniqState` rollup, not a bigger host.

## The running stack, under live ingest

Measured on the same laptop on 2026-10-05, ingesting the real Wikimedia stream (about 5.5
kept edits a second) for 27 minutes, from `system.part_log` and `docker stats`.

| What | Measured |
|---|---|
| New parts, `wiki_edits` | 1,201 (0.74 a second: one per insert) |
| Merges, `wiki_edits` | 1,193, 21 ms on average |
| Active parts afterwards | 6 in `wiki_edits`, 3 in the rollup |
| Memory, ClickHouse | 501 MiB (server cap 900 MiB) |
| Memory, ingest / api / nginx | 40 / 42 / 8 MiB |

Merges keep pace with one insert a second, so parts don't pile up. Measuring the same on
the production instance, over a week and through a TTL drop, is still M5.
