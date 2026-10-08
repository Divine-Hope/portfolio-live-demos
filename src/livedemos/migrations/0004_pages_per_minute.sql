-- Distinct pages edited, for "Query it" windows longer than raw rows can answer quickly
-- (3 and 7 days). Each minute and language keeps the exact set of pages edited in it, as a
-- uniqExact state; a window merges its minutes' sets into one count, the same count
-- uniqExact over raw rows gives for those minutes. A week is about 30,000 small states
-- instead of millions of raw rows.
--
-- Merging sets is idempotent: inserting the same page into a minute twice changes nothing.
-- So refilling it (from raw rows, or from the Parquet archive) only ever adds, with no
-- staging and no swap. Kept 14 days: the longest window is 7, and the window ends at the
-- newest event, which can be days old if ingest stalls.
CREATE TABLE IF NOT EXISTS {database}.wiki_pages_per_minute
(
    minute  DateTime('UTC'),
    lang    LowCardinality(String),
    pages   AggregateFunction(uniqExact, Int32, String) COMMENT 'set of (namespace, title)'
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (lang, minute)
TTL minute + INTERVAL 14 DAY
SETTINGS
    -- A retried insert's view block is dropped here too, as for the rollup.
    non_replicated_deduplication_window = 1000,
    ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.wiki_pages_per_minute_mv
TO {database}.wiki_pages_per_minute
AS SELECT
    toStartOfMinute(event_time) AS minute,
    lang,
    uniqExactState(namespace, title) AS pages
FROM {database}.wiki_edits
GROUP BY minute, lang;

-- Archived hours whose page sets were filled from Parquet (archive/pages.py), recorded only
-- once an hour's insert has finished. An hour with raw rows gets its sets from the view
-- instead. Without this, an interrupted fill would look done.
CREATE TABLE IF NOT EXISTS {database}.wiki_pages_filled
(
    hour       DateTime('UTC'),
    filled_at  DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(filled_at)
ORDER BY hour
TTL hour + INTERVAL 14 DAY;

-- The raw rows already here. Safe to run twice: see above.
INSERT INTO {database}.wiki_pages_per_minute
SELECT toStartOfMinute(event_time) AS minute, lang, uniqExactState(namespace, title) AS pages
FROM {database}.wiki_edits
GROUP BY minute, lang;
