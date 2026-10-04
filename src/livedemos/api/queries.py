"""SQL used by the API. Values are bound server-side with {name:Type} placeholders.

Every window is anchored to the newest event (`to_ms`), not to the wall clock. If ingest
stalls, the numbers freeze at the last thing we actually saw, and the widget says
"Paused" instead of showing a fake drop to zero.
"""

from typing import Any

# Plain max() and count() are answered from part metadata (a few rows read).
# maxOrNull() would scan the whole table every second. Callers treat n = 0 as empty.
NEWEST = """
SELECT toUnixTimestamp64Milli(max(event_time)) AS newest_ms, count() AS n
FROM wiki_edits
"""


def newest_ms(rows: list[dict[str, Any]]) -> int | None:
    """Newest event time in ms from a NEWEST result, or None for an empty table."""
    if not rows or not int(rows[0]["n"]):
        return None
    return int(rows[0]["newest_ms"])


WINDOW_TOTALS = """
SELECT
    lang,
    count() AS edits,
    uniqExact(namespace, title) AS pages,
    countIf(is_bot) AS bot_edits
FROM wiki_edits
WHERE event_time > fromUnixTimestamp64Milli({to_ms:Int64}) - toIntervalSecond({window_s:UInt32})
  AND event_time <= fromUnixTimestamp64Milli({to_ms:Int64})
  AND lang IN {langs:Array(String)}
GROUP BY lang
"""

TOP_ARTICLES = """
SELECT lang, title, count() AS edits
FROM wiki_edits
WHERE event_time > fromUnixTimestamp64Milli({to_ms:Int64}) - toIntervalSecond({window_s:UInt32})
  AND event_time <= fromUnixTimestamp64Milli({to_ms:Int64})
  AND namespace = 0
GROUP BY lang, title
ORDER BY edits DESC, title ASC
LIMIT {per_lang:UInt8} BY lang
"""

EDITS_PER_MINUTE = """
SELECT toUnixTimestamp(minute) AS minute_s, lang, sum(edits) AS edits
FROM wiki_edits_per_minute
WHERE minute > toStartOfMinute(fromUnixTimestamp64Milli({to_ms:Int64}))
               - toIntervalMinute({minutes:UInt16})
GROUP BY minute, lang
"""

INGEST_LAG = """
SELECT
    quantileExact(0.5)(dateDiff('millisecond', event_time, ingested_at)) AS p50_ms,
    quantileExact(0.95)(dateDiff('millisecond', event_time, ingested_at)) AS p95_ms
FROM wiki_edits
WHERE event_time > fromUnixTimestamp64Milli({to_ms:Int64}) - INTERVAL 1 MINUTE
"""

RECENT_GAPS = """
SELECT
    toUnixTimestamp64Milli(gap_from) AS from_ms,
    toUnixTimestamp64Milli(gap_to) AS to_ms
FROM ingest_gaps
WHERE gap_to > fromUnixTimestamp64Milli({to_ms:Int64}) - toIntervalMinute({minutes:UInt16})
"""
