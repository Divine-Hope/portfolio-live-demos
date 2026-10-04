"""Where to pick up after a restart. See docs/adr/0006-bookmark-stored-with-rows.md."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from livedemos.clickhouse import ClickHouse

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ResumeState:
    bookmark: str | None  # SSE id to send as Last-Event-ID, or None to start fresh
    since: datetime | None  # used only when there's no usable bookmark
    seam_ids: list[str]  # ids of the most recently ingested events, newest first
    gap_from: datetime | None  # set when the bookmark was too old to resume from


async def load_resume_state(
    ch: ClickHouse,
    *,
    now: datetime,
    retention: timedelta,
    lookback: timedelta,
    seam_ids: int,
) -> ResumeState:
    newest = await _newest_event_time(ch)

    if newest is None:
        log.info("no data yet, first boot", extra={"since": (now - lookback).isoformat()})
        return ResumeState(bookmark=None, since=now - lookback, seam_ids=[], gap_from=None)

    if newest < now - retention:
        # The source no longer has these events. Start from now and say so.
        log.warning("bookmark older than retention", extra={"newest": newest.isoformat()})
        return ResumeState(bookmark=None, since=now, seam_ids=[], gap_from=newest)

    bookmark, recent_ids = await _ingest_tail(ch, newest, limit=seam_ids)
    log.info(
        "resuming from bookmark",
        extra={"newest": newest.isoformat(), "seam_ids": len(recent_ids)},
    )
    return ResumeState(bookmark=bookmark, since=None, seam_ids=recent_ids, gap_from=None)


async def _newest_event_time(ch: ClickHouse) -> datetime | None:
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


async def _ingest_tail(
    ch: ClickHouse, newest: datetime, *, limit: int
) -> tuple[str | None, list[str]]:
    """The last `limit` rows by ingest order: the bookmark, and the ids to dedupe against.

    The row ingested last carries the bookmark. Ordering by ingest_seq, not event time,
    matters: the stream delivers late and interleaved events, so the newest event and
    the last one delivered are often different rows. The event-time filter only keeps
    the scan to the last day of partitions.
    """
    result = await ch.query(
        """
        SELECT toString(event_id) AS id, sse_id
        FROM wiki_edits
        WHERE event_time >= fromUnixTimestamp64Milli({from_ms:Int64})
        ORDER BY ingest_seq DESC
        LIMIT {limit:UInt32}
        """,
        params={"from_ms": _ms(newest - timedelta(days=1)), "limit": limit},
    )
    if not result.rows:
        return None, []
    return str(result.rows[0]["sse_id"]), [str(r["id"]) for r in result.rows]


async def record_gap(ch: ClickHouse, *, gap_from: datetime, gap_to: datetime, reason: str) -> None:
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


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)
