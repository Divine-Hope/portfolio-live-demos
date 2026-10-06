-- The Parquet archive's bookkeeping (src/livedemos/archive/).

-- What each hour's file holds, as read back after writing it. The archive compares this
-- with ClickHouse's raw count every run, and rewrites an hour whose raw rows have grown
-- (late events, a replay). The newest row per hour wins. Only needed while raw rows exist.
CREATE TABLE IF NOT EXISTS {database}.archive_hours
(
    hour        DateTime('UTC'),
    rows        UInt64 COMMENT 'rows in the file, counted from the file itself',
    written_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(written_at)
ORDER BY hour
TTL hour + INTERVAL 14 DAY;

-- A rollup rebuild counts the archive into here first, and only replaces the live rollup
-- once that worked. Same shape as wiki_edits_per_minute; empty between rebuilds.
CREATE TABLE IF NOT EXISTS {database}.wiki_edits_per_minute_staging
(
    minute     DateTime('UTC'),
    lang       LowCardinality(String),
    edits      UInt64,
    bot_edits  UInt64
)
ENGINE = SummingMergeTree
ORDER BY (lang, minute);
