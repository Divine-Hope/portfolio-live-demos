-- Distinct pages edited, for "Query it" windows longer than raw rows can answer quickly
-- (3 and 7 days). Each minute and language keeps the exact set of pages edited in it, as a
-- uniqExact state; a window merges its minutes' sets into one exact count. A week is about
-- 30,000 small states instead of millions of raw rows.
--
-- Merging sets is idempotent: inserting the same page into a minute twice changes nothing.
-- So refilling it (from raw rows, or from the Parquet archive on a rebuilt host) only ever
-- adds, with no staging and no swap. Kept 8 days: the longest window is 7.
CREATE TABLE IF NOT EXISTS {database}.wiki_pages_per_minute
(
    minute  DateTime('UTC'),
    lang    LowCardinality(String),
    pages   AggregateFunction(uniqExact, Int32, String) COMMENT 'set of (namespace, title)'
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (lang, minute)
TTL minute + INTERVAL 8 DAY
SETTINGS ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS {database}.wiki_pages_per_minute_mv
TO {database}.wiki_pages_per_minute
AS SELECT
    toStartOfMinute(event_time) AS minute,
    lang,
    uniqExactState(namespace, title) AS pages
FROM {database}.wiki_edits
GROUP BY minute, lang;

-- The raw rows already here. Safe to run twice: see above.
INSERT INTO {database}.wiki_pages_per_minute
SELECT toStartOfMinute(event_time) AS minute, lang, uniqExactState(namespace, title) AS pages
FROM {database}.wiki_edits
WHERE event_time > now() - INTERVAL 8 DAY
GROUP BY minute, lang;
