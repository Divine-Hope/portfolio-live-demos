-- The Parquet archive's bookkeeping (src/livedemos/archive/).

-- What each hour's file holds, as read back after writing it. The archive compares this
-- with ClickHouse's raw count every run, and rewrites an hour whose raw rows have grown
-- (late events, a replay). The newest record per hour wins (ties: the larger count).
-- Kept for good (about 9,000 rows a year): the rollup rebuild checks every file against it.
CREATE TABLE IF NOT EXISTS {database}.archive_hours
(
    hour        DateTime('UTC'),
    rows        UInt64 COMMENT 'rows in the file, counted from the file itself',
    written_at  DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(written_at)
ORDER BY hour;

-- A rollup rebuild assembles whole replacement months here, then swaps each into
-- wiki_edits_per_minute with one atomic REPLACE PARTITION. That needs the same columns,
-- partition key and sort key as the rollup. Empty between rebuilds.
CREATE TABLE IF NOT EXISTS {database}.wiki_edits_per_minute_staging
(
    minute     DateTime('UTC'),
    lang       LowCardinality(String),
    edits      UInt64,
    bot_edits  UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (lang, minute);
