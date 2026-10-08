# 0003. ClickHouse as the serving store

Date: 2026-10-04
Status: Accepted

## Context

The store takes a continuous stream of small inserts and answers windowed aggregations (counts, distinct counts, top N) every second, plus ad hoc queries from visitors. Options considered: ClickHouse, DuckDB, PostgreSQL.

## Decision

ClickHouse, single node, tuned for a 2 GB host.

## Consequences

- Concurrent inserts and reads, TTLs, materialized views for rollups, insert deduplication tokens and per-query statistics come built in. "Query it" can show ClickHouse's own timing.
- DuckDB was the main alternative. It's excellent for analysis, but it's one writer per database file, which would couple ingest and the API into one process. It stays useful for ad hoc work on the Parquet archive.
- PostgreSQL would work at this volume, but columnar scans and rollups would need more hand-tuning.
- ClickHouse's defaults assume a big server. `clickhouse/config.d/low-memory.xml` caps it, and production measurements check the numbers ([ADR 0010](0010-spot-host-in-an-auto-scaling-group.md)).
