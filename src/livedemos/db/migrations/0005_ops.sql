-- What the Ops tab reports (GET /v1/ops.json, ops/report.py). Small tables, kept 90 days.

-- One sample a minute of how old the newest event is, for the 30-day freshness SLO.
-- ClickHouse takes it itself (a refreshable view), so a sample doesn't depend on ingest or
-- the API being up. A minute with no sample at all (ClickHouse down, host replaced)
-- counts against the SLO too: ops/report.py counts it as unmeasured, never as fresh.
CREATE TABLE IF NOT EXISTS {database}.freshness_samples
(
    minute      DateTime('UTC') COMMENT 'the minute the sample was taken in',
    sampled_at  DateTime64(3, 'UTC'),
    age_s       Nullable(Float64) COMMENT 'sampled_at minus the newest event time; NULL if there are no rows'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY minute
TTL minute + INTERVAL 90 DAY;

-- Plain max() and count() are answered from part metadata: a few rows read a minute.
-- ClickHouse runs it once at creation, then at the start of every minute, and doesn't
-- catch up on minutes it missed while down. Created as the migrator; migration 0006 hands
-- it to a user that can only read wiki_edits and write the samples.
CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.freshness_samples_mv
REFRESH EVERY 1 MINUTE APPEND
TO {database}.freshness_samples
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT
    toStartOfMinute(now64(3)) AS minute,
    now64(3) AS sampled_at,
    if(count() = 0, NULL, dateDiff('millisecond', max(event_time), now64(3)) / 1000) AS age_s
FROM {database}.wiki_edits;

-- The Ops tab's ingest lag covers rows by when they were stored, late or replayed ones
-- included. wiki_edits is sorted by event time; this lets a range on ingested_at skip the
-- granules outside it. Parts are written in ingest order, so it skips nearly all of them.
ALTER TABLE {database}.wiki_edits
    ADD INDEX IF NOT EXISTS ingested_at_minmax ingested_at TYPE minmax GRANULARITY 1;

ALTER TABLE {database}.wiki_edits MATERIALIZE INDEX ingested_at_minmax SETTINGS mutations_sync = 1;

-- Each time ingest's stream connection ended and it reconnected, and why. Written by
-- ingest; reasons are the ones in ingest/consumer.py (idle, eof, source_closed, network,
-- http_status, clickhouse).
CREATE TABLE IF NOT EXISTS {database}.ingest_reconnects
(
    at      DateTime64(3, 'UTC'),
    reason  LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY at
TTL toDateTime(at) + INTERVAL 90 DAY
-- A retried insert with the same token is skipped instead of counted twice.
SETTINGS non_replicated_deduplication_window = 100;

-- Month-to-date AWS cost from Cost Explorer, one attempt a day at most (ops/cost.py).
-- A failed attempt is recorded too (ok = false), so it isn't retried the same day.
CREATE TABLE IF NOT EXISTS {database}.aws_cost
(
    fetched_at    DateTime64(3, 'UTC'),
    ok            Bool,
    period_start  Date COMMENT 'first day of the month, inclusive',
    period_end    Date COMMENT 'exclusive, as Cost Explorer reports it',
    amount        String COMMENT 'exactly as AWS returned it',
    currency      LowCardinality(String),
    estimated     Bool,
    error         String
)
ENGINE = MergeTree
ORDER BY fetched_at
TTL toDateTime(fetched_at) + INTERVAL 90 DAY;
