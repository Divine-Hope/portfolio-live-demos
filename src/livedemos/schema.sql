-- Schema for the live demos. Every statement is idempotent, so `migrate` can run on
-- every start. `{database}` is replaced with the configured database name.

CREATE DATABASE IF NOT EXISTS {database};

-- One row per Wikipedia edit we keep. Raw events live 7 days; history lives in the
-- per-minute rollup below (90 days) and, in production, in the Parquet archive on S3.
CREATE TABLE IF NOT EXISTS {database}.wiki_edits
(
    event_id     UUID COMMENT 'meta.id, unique per event',
    event_time   DateTime64(3, 'UTC') COMMENT 'meta.dt, when Wikimedia recorded the change',
    ingested_at  DateTime64(3, 'UTC') COMMENT 'when ingest received the event',
    wiki         LowCardinality(String) COMMENT 'enwiki, ptwiki, dewiki',
    lang         LowCardinality(String) COMMENT 'en, pt, de',
    type         LowCardinality(String) COMMENT 'edit or new',
    namespace    Int32,
    title        String,
    is_bot       Bool,
    sse_id       String COMMENT 'resume bookmark, stored with the row (ADR 0006)' CODEC(ZSTD(3)),
    ingest_seq   UInt64 COMMENT 'monotonic across restarts, orders bookmarks'
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (lang, event_time)
TTL toDateTime(event_time) + INTERVAL 7 DAY
SETTINGS
    -- Lets a retried insert with the same token be skipped instead of written twice.
    non_replicated_deduplication_window = 1000,
    -- Expire whole days at once instead of rewriting parts.
    ttl_only_drop_parts = 1;

-- Edits per minute per language, kept 90 days. Fed by the materialized view below.
CREATE TABLE IF NOT EXISTS {database}.wiki_edits_per_minute
(
    minute     DateTime('UTC'),
    lang       LowCardinality(String),
    edits      UInt64,
    bot_edits  UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (lang, minute)
TTL minute + INTERVAL 90 DAY
SETTINGS non_replicated_deduplication_window = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.wiki_edits_per_minute_mv
TO {database}.wiki_edits_per_minute
AS SELECT
    toStartOfMinute(event_time) AS minute,
    lang,
    count() AS edits,
    countIf(is_bot) AS bot_edits
FROM {database}.wiki_edits
GROUP BY minute, lang;

-- Periods we know we're missing, so charts show a labelled gap instead of a fake zero.
CREATE TABLE IF NOT EXISTS {database}.ingest_gaps
(
    detected_at  DateTime64(3, 'UTC'),
    gap_from     DateTime64(3, 'UTC'),
    gap_to       DateTime64(3, 'UTC'),
    reason       LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY detected_at
TTL toDateTime(detected_at) + INTERVAL 90 DAY;
