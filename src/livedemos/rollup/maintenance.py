"""Rewriting rollup minutes safely: shared by reconcile's repair and the archive rebuild.

Rewriting a minute while ingest writes could count a late event twice: once through the
materialized view, once in the rewrite. So a rewrite first proves ingest is stopped, and
afterwards proves nothing landed while it worked.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from livedemos.db.clickhouse import ClickHouse
from livedemos.ingest.resume import INSERT_QUERY_ID_PREFIX, MAX_SEQ, RUNNING_INSERTS

_LAST_INGEST = "SELECT toUnixTimestamp64Milli(max(ingested_at)) AS ms, count() AS n FROM wiki_edits"


class IngestRunning(RuntimeError):
    """Ingest is writing, or wrote during the rewrite: the rewrite could double-count."""


async def _last_ingest_ms(ch: ClickHouse) -> int:
    row = (await ch.query(_LAST_INGEST)).rows[0]
    return int(row["ms"]) if int(row["n"]) else 0


async def _ingest_state(ch: ClickHouse) -> tuple[int, int]:
    """(inserts running now, highest committed ingest_seq).

    An insert whose raw rows are already visible can still be writing the rollup (the view
    runs after the raw part commits), so "no new rows" isn't enough: none may be running.
    """
    running = await ch.query(RUNNING_INSERTS, params={"prefix": INSERT_QUERY_ID_PREFIX})
    seq = await ch.query(MAX_SEQ)
    return int(running.rows[0]["n"]), int(seq.rows[0]["seq"]) if seq.rows else 0


@dataclass(frozen=True, slots=True)
class IngestMark:
    """Where ingest was when a rollup rewrite started: highest seq, last ingested_at."""

    seq: int
    last_ms: int


async def require_ingest_stopped(
    ch: ClickHouse, *, quiet: timedelta = timedelta(seconds=30)
) -> IngestMark:
    """Raise IngestRunning unless no ingest insert runs and nothing landed for `quiet`.

    Rewriting rollup minutes while ingest writes could count a late event twice: once
    through the view, once in the rebuild. Pass the mark to `require_ingest_still_stopped`
    when done.
    """
    running, seq = await _ingest_state(ch)
    if running:
        raise IngestRunning("an ingest insert is still running; stop ingest first")
    last_ms = await _last_ingest_ms(ch)
    if last_ms and datetime.now(UTC).timestamp() * 1000 - last_ms < quiet.total_seconds() * 1000:
        raise IngestRunning("rows are still arriving; stop ingest first")
    return IngestMark(seq=seq, last_ms=last_ms)


async def require_ingest_still_stopped(ch: ClickHouse, mark: IngestMark) -> None:
    running, seq = await _ingest_state(ch)
    if running or seq != mark.seq or await _last_ingest_ms(ch) != mark.last_ms:
        raise IngestRunning("ingest wrote during the rewrite; stop it and run again")


# The rollup keeps hashes of recent inserts to drop retried ones. A rebuild can produce a
# block identical to an earlier one (the view's original block for those minutes, or a
# previous rebuild's), which dedup would drop silently, after the DELETE. 26.8 doesn't
# dedup INSERT ... SELECT by default, but `deduplicate_insert_select` can turn it on, and
# then it wins over `insert_deduplicate`. Rebuilds are deliberate: turn both off.
# A rebuild reads a day of Parquet per query: well over the writer profile's 30 s limit on
# a busy small host, so it gets 10 minutes.
REBUILD_INSERT_SETTINGS = {
    "insert_deduplicate": "0",
    "deduplicate_insert_select": "disable",
    "max_execution_time": "600",
}
