"""Where to pick up after a restart. See docs/adr/0006-bookmark-stored-with-rows.md."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from livedemos.clickhouse import Database

log = logging.getLogger(__name__)

# Every ingest INSERT carries a query_id with this prefix, so a restarted process can see
# inserts that its predecessor started and that are still running on the server.
INSERT_QUERY_ID_PREFIX = "ingest-"

# The first range of ingest_seq (wall-clock ns, so about an hour of ingest) searched for
# the last rows ingested. Widened until it holds enough; see _ingest_tail.
SEAM_WINDOW_NS = 3_600 * 10**9


@dataclass(frozen=True, slots=True)
class ResumeState:
    bookmark: str | None  # SSE id to send as Last-Event-ID, or None to start fresh
    since: datetime | None  # used only when there's no usable bookmark
    floor: datetime | None  # drop events older than this: the rollup already counts them
    seam_ids: list[str]  # ids of the most recently ingested events, newest first
    last_seq: int  # highest ingest_seq committed; new ids must be above it
    newest_event_time: datetime | None  # newest event committed, to age the bookmark
    gap: tuple[datetime, datetime] | None  # history we know we've lost, to record


async def wait_for_inflight_inserts(ch: Database, *, timeout_s: float) -> bool:
    """Wait until no ingest INSERT is still running on the server. False on timeout.

    A process that was killed mid-insert may leave that insert running server-side, and
    it can commit after we read the bookmark. Reading state only once those have finished
    closes that window. Needs `SELECT(query_id, current_database) ON system.processes`
    (users.d). Every user with that grant sees every user's queries, so only this
    database's inserts count: ingest for another database (or a test run next to a local
    stack) mustn't hold this one up.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        result = await ch.query(
            "SELECT count() AS n FROM system.processes "
            "WHERE startsWith(query_id, {prefix:String}) AND current_database = currentDatabase()",
            params={"prefix": INSERT_QUERY_ID_PREFIX},
        )
        if not int(result.rows[0]["n"]):
            return True
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(0.5)


async def load_resume_state(
    ch: Database,
    *,
    now: datetime,
    retention: timedelta,
    lookback: timedelta,
    seam_ids: int,
) -> ResumeState:
    newest = await _newest_event_time(ch)

    if newest is None:
        prior = await _newest_rollup_minute(ch)
        if prior is None:  # a first boot
            since = now - lookback
            log.info("no raw rows, starting fresh", extra={"since": since.isoformat()})
            return ResumeState(None, since, None, [], 0, None, None)
        # Raw rows are gone but the rollup remembers earlier runs: an outage outlived raw
        # retention. Replay what the source still has, after the rollup's last minute, and
        # mark everything from that minute on as a gap: it may be partial.
        floor = prior + timedelta(minutes=1)
        since = min(max(floor, now - retention), now)
        log.info("no raw rows, resuming after the rollup", extra={"since": since.isoformat()})
        return ResumeState(None, since, floor, [], 0, None, (prior, since))

    last_seq = await _max_ingest_seq(ch)
    if newest < now - retention:
        # The source no longer has these events. Start fresh and say what's missing.
        since = now - lookback
        log.warning("bookmark older than retention", extra={"newest": newest.isoformat()})
        return ResumeState(None, since, None, [], last_seq, None, (_minute(newest), since))

    bookmark, recent_ids = await _ingest_tail(ch, last_seq, limit=seam_ids)
    if not bookmark:
        return await _after_restore(
            ch, now=now, retention=retention, recent_ids=recent_ids, last_seq=last_seq
        )
    log.info(
        "resuming from bookmark",
        extra={"newest": newest.isoformat(), "seam_ids": len(recent_ids), "last_seq": last_seq},
    )
    return ResumeState(bookmark, None, None, recent_ids, last_seq, newest, None)


# How far before the archive's newest event a rebuilt host replays from. Covers events the
# old host ingested after its last archive run (it runs every 5 minutes, and an hour is
# written 5 minutes after it ends) and ingest lag. Replayed events it restored are skipped
# by id, so a wide margin costs only replay time.
RESTORE_REPLAY_MARGIN = timedelta(minutes=30)


async def _after_restore(
    ch: Database, *, now: datetime, retention: timedelta, recent_ids: list[str], last_seq: int
) -> ResumeState:
    """The newest raw rows came back from the archive (archive/restore.py), no bookmark.

    Replay from before the newest restored event and skip the restored ids: everything
    the old host ingested after these were archived is still in the source. `recent_ids`
    holds the restored rows ingested last, which is where the replay overlaps them.
    Events older than the restored raw rows belong to minutes rebuilt into the rollup, so
    they're dropped rather than counted twice.
    """
    span = (await ch.query(_RESTORED_SPAN)).rows[0]
    oldest = _from_ms(int(span["oldest_ms"]))
    hour_end = _ceil_hour(_from_ms(int(span["newest_ms"])))
    last_ingested = _from_ms(int(span["ingested_ms"]))
    since = min(last_ingested, hour_end) - RESTORE_REPLAY_MARGIN
    gap = None
    if since < now - retention:
        gap, since = (hour_end, now - retention), now - retention
    log.info(
        "resuming after a restore from the archive",
        extra={"since": since.isoformat(), "seam_ids": len(recent_ids)},
    )
    # Restored raw rows are whole days (archive/restore.py); before them, the rollup.
    floor = oldest.replace(hour=0, minute=0, second=0, microsecond=0)
    return ResumeState(None, since, floor, recent_ids, last_seq, None, gap)


_RESTORED_SPAN = """
SELECT toUnixTimestamp64Milli(min(event_time)) AS oldest_ms,
       toUnixTimestamp64Milli(max(event_time)) AS newest_ms,
       toUnixTimestamp64Milli(max(ingested_at)) AS ingested_ms
FROM wiki_edits
"""


def _from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


def _minute(t: datetime) -> datetime:
    """The start of t's minute: a minute ingest stopped in may be partial."""
    return t.replace(second=0, microsecond=0)


def _ceil_hour(t: datetime) -> datetime:
    start = t.replace(minute=0, second=0, microsecond=0)
    return start if start == t else start + timedelta(hours=1)


async def _newest_event_time(ch: Database) -> datetime | None:
    # Plain max() and count() on the partition key column are answered from part
    # metadata (the min/max/count projection): a few rows read, not the whole table.
    # maxOrNull() looks equivalent but disables that optimisation.
    result = await ch.query(
        "SELECT toUnixTimestamp64Milli(max(event_time)) AS newest_ms, count() AS n FROM wiki_edits"
    )
    row = result.rows[0] if result.rows else {"n": 0}
    if not int(row["n"]):
        return None
    return datetime.fromtimestamp(int(row["newest_ms"]) / 1000, UTC)


async def _newest_rollup_minute(ch: Database) -> datetime | None:
    # `minute` is in the partition key, so this is answered from part metadata too.
    result = await ch.query(
        "SELECT toUnixTimestamp(max(minute)) AS newest_s, count() AS n FROM wiki_edits_per_minute"
    )
    row = result.rows[0] if result.rows else {"n": 0}
    if not int(row["n"]):
        return None
    return datetime.fromtimestamp(int(row["newest_s"]), UTC)


async def _max_ingest_seq(ch: Database) -> int:
    # Answered by the `seq_max` projection: one row per part (migration 0002).
    result = await ch.query("SELECT max(ingest_seq) AS seq FROM wiki_edits")
    return int(result.rows[0]["seq"]) if result.rows else 0


async def _ingest_tail(ch: Database, last_seq: int, *, limit: int) -> tuple[str | None, list[str]]:
    """The last `limit` rows by ingest order: the bookmark, and the ids to dedupe against.

    The row ingested last carries the bookmark. Ordering by ingest_seq, not event time,
    matters: the stream delivers late and interleaved events, so the newest event and
    the last one delivered are often different rows.

    ClickHouse only uses the `by_ingest_seq` projection for a range on its key, so this
    reads from a floor below `last_seq`, doubling the range until it holds `limit` rows or
    covers everything. The floor bounds the read; it never decides which rows count, so a
    clock that jumped between two inserts can't split the seam.
    """
    span = SEAM_WINDOW_NS
    while True:
        floor = max(0, last_seq - span)
        result = await ch.query(
            """
            SELECT toString(event_id) AS id, sse_id
            FROM wiki_edits
            WHERE ingest_seq >= {floor:UInt64}
            ORDER BY ingest_seq DESC
            LIMIT {limit:UInt32}
            """,
            params={"floor": floor, "limit": limit},
        )
        if len(result.rows) >= limit or floor == 0:
            break
        span *= 2
    if not result.rows:
        return None, []
    return str(result.rows[0]["sse_id"]), [str(r["id"]) for r in result.rows]


async def record_gap(ch: Database, *, gap_from: datetime, gap_to: datetime, reason: str) -> None:
    await ch.insert(
        "ingest_gaps",
        [
            {
                "detected_at": datetime.now(UTC).isoformat(),
                "gap_from": gap_from.isoformat(),
                "gap_to": gap_to.isoformat(),
                "reason": reason,
            }
        ],
    )
