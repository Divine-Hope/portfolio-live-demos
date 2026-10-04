# 0004. Plain Parquet on S3 for history, not Iceberg or DuckLake (yet)

Date: 2026-10-04
Status: Accepted

## Context

Raw events live 7 days in ClickHouse. History beyond that needs a cheap, durable home that any engine can read. Candidates: plain Parquet files, Apache Iceberg (for example on S3 Tables), DuckLake.

The volume is roughly tens of MB a day, from one writer, with a stable schema.

## Decision

Hourly Parquet files on S3, Hive-style partitions (`dt=YYYY-MM-DD/hour=HH`), written by one ClickHouse `INSERT INTO FUNCTION s3(...)` statement using the instance role. Read back with ClickHouse `s3()` for rebuilds, or DuckDB for ad hoc work.

## Consequences

- No catalog, no compaction jobs, no extra service. Any engine reads Parquet.
- Iceberg's strengths (concurrent writers, schema evolution, snapshot isolation, time travel) aren't needed with one writer and a fixed schema. S3 Tables would add a catalog endpoint and per-object monitoring and compaction fees for features we wouldn't use.
- DuckLake reached v1.0 in April 2026 and is a good design, but it needs a catalog database and ClickHouse can't read it.
- Revisit and move to Iceberg when a second writer or real schema evolution appears.
