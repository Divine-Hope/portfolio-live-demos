"""Month-to-date AWS cost, from Cost Explorer, at most once a day.

Cost Explorer charges $0.01 a request and its numbers move once a day or so, so the archive
service (the one process that runs only on the live host) asks once per UTC day and keeps
the answer in ClickHouse (`aws_cost`, migration 0005) for the API to serve.

"Once a day" has to hold across crashes and host replacements, and while two hosts overlap,
so it isn't decided by ClickHouse (each host has its own) or by memory. Before asking, the
fetcher claims the day in the archive bucket with a conditional write that only one caller
can win: `<claims>/YYYY-MM-DD.json`. The winner asks, then writes the answer into the same
object. Everyone else, and a new host later that day, copies the answer from there. A claim
with no answer (the winner died mid-way) means no new number that day, never a second call.
An answer that couldn't be saved yet is kept in memory and saved on the next runs.

The host's role may call `ce:GetCostAndUsage` and nothing else in Cost Explorer, and may
read and write only the claims prefix (infra/live/host.tf). The filter is the `project` tag
every resource carries.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from livedemos.clickhouse import ClickHouseError, Database

log = logging.getLogger(__name__)

METRIC = "UnblendedCost"
_RECORDED_TODAY = "SELECT count() AS n FROM aws_cost WHERE toDate(fetched_at) = {day:Date}"


def period(today: date) -> tuple[date, date]:
    """The month so far: its first day, to tomorrow (Cost Explorer's end is exclusive)."""
    return today.replace(day=1), today + timedelta(days=1)


def request(today: date, tag_key: str, tag_value: str) -> dict[str, Any]:
    start, end = period(today)
    return {
        "TimePeriod": {"Start": start.isoformat(), "End": end.isoformat()},
        "Granularity": "MONTHLY",
        "Metrics": [METRIC],
        "Filter": {"Tags": {"Key": tag_key, "Values": [tag_value]}},
    }


def parse(response: dict[str, Any]) -> tuple[str, str, bool]:
    """(amount, currency, estimated) from a GetCostAndUsage response for one month."""
    results = response.get("ResultsByTime") or []
    if len(results) != 1:
        raise ValueError(f"expected one month of results, got {len(results)}")
    total = results[0]["Total"][METRIC]
    return str(total["Amount"]), str(total["Unit"]), bool(results[0].get("Estimated", False))


def parse_claims(url: str) -> tuple[str, str]:
    """`s3://bucket/prefix` to (bucket, prefix)."""
    if not url.startswith("s3://"):
        raise ValueError("claims must be an s3://bucket/prefix URL")
    bucket, _, prefix = url.removeprefix("s3://").partition("/")
    if not bucket or not prefix.strip("/"):
        raise ValueError("claims must name a bucket and a prefix")
    return bucket, prefix.strip("/")


def _error_code(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str(response.get("Error", {}).get("Code", ""))
    return ""


class _DayEnded(Exception):
    """Midnight UTC passed between claiming a day and asking for it."""


@dataclass(slots=True)
class _Unsaved:
    """An answer Cost Explorer gave that isn't in both places yet."""

    key: str
    body: dict[str, Any]  # the claim, with the answer in it
    published: bool = False  # in the claim object, for other hosts
    recorded: bool = False  # in this host's ClickHouse, for the API


class CostFetcher:
    def __init__(
        self,
        ch: Database,
        *,
        tag: str,
        claims: str,
        ce_factory: Callable[[], Any],
        s3_factory: Callable[[], Any],
        host: str = "",
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ):
        key, sep, value = tag.partition("=")
        if not (key and sep and value):
            raise ValueError("tag must look like key=value")
        self._ch = ch
        self._tag = (key, value)
        self._bucket, self._prefix = parse_claims(claims)
        self._ce_factory = ce_factory
        self._s3_factory = s3_factory
        self._host = host
        self._clock = clock
        self._done_on: date | None = None  # today is settled: recorded, or nothing to record
        self._unsaved: _Unsaved | None = None

    async def refresh_if_due(self, now: datetime) -> bool:
        """Settle today's cost. True if this call asked Cost Explorer. Never raises."""
        today = now.astimezone(UTC).date()
        try:
            if self._unsaved is not None:
                await self._save()
                if self._unsaved is not None:
                    return False  # nowhere to put another answer yet: don't ask for one
            if self._done_on == today:
                return False
            return await self._refresh(now.astimezone(UTC), today)
        except Exception:  # a broken Ops number must never stop the archive
            log.exception("cost refresh failed")
            return False

    async def _refresh(self, now: datetime, today: date) -> bool:
        try:
            recorded = await self._ch.query(_RECORDED_TODAY, params={"day": today.isoformat()})
        except ClickHouseError as exc:  # try again next run
            log.warning("checking for today's cost failed", extra={"error": str(exc)})
            return False
        if int(recorded.rows[0]["n"]):
            self._done_on = today
            return False

        s3 = await asyncio.to_thread(self._s3_factory)
        key = f"{self._prefix}/{today.isoformat()}.json"
        claim = {"claimed_at": now.isoformat(), "host": self._host}
        try:
            await asyncio.to_thread(
                s3.put_object,
                Bucket=self._bucket,
                Key=key,
                Body=json.dumps(claim).encode(),
                ContentType="application/json",
                IfNoneMatch="*",  # only one caller can create it
            )
        except Exception as exc:
            if _error_code(exc) not in {"PreconditionFailed", "ConditionalRequestConflict"}:
                raise  # couldn't claim: don't ask, try again next run
            # Someone else has today. Copy their answer, if it's there yet.
            await self._copy_answer(s3, key, today)
            return False

        row = self._row(now, today)
        try:
            response = await asyncio.to_thread(self._ask, today)
        except _DayEnded:
            # Asking now would land in tomorrow's CloudTrail day, which gets its own call:
            # leave this claim unanswered.
            log.warning("the day ended before asking; not asking", extra={"day": str(today)})
            return False
        except Exception as exc:  # recorded as a failed attempt; not retried today
            log.error("cost explorer request failed", extra={"error": repr(exc)[:300]})
            row["ok"], row["error"] = False, repr(exc)[:300]
        else:
            try:
                row["amount"], row["currency"], row["estimated"] = parse(response)
            except Exception as exc:
                log.error("unexpected cost explorer answer", extra={"error": repr(exc)[:300]})
                row["ok"], row["error"] = False, repr(exc)[:300]
        self._done_on = today  # asked: whatever happens next, not again today
        self._unsaved = _Unsaved(key=key, body={**claim, "result": row})
        await self._save()
        return True

    def _ask(self, today: date) -> dict[str, Any]:
        """Build the client, then check the day once more right before the billed request."""
        ce = self._ce_factory()
        if self._clock().astimezone(UTC).date() != today:
            raise _DayEnded
        response: dict[str, Any] = ce.get_cost_and_usage(**request(today, *self._tag))
        return response

    async def _save(self) -> None:
        """Put the answer in the claim and in ClickHouse; whatever fails is retried later."""
        unsaved = self._unsaved
        if unsaved is None:
            return
        if not unsaved.published:
            try:
                s3 = await asyncio.to_thread(self._s3_factory)
                await asyncio.to_thread(
                    s3.put_object,
                    Bucket=self._bucket,
                    Key=unsaved.key,
                    Body=json.dumps(unsaved.body).encode(),
                    ContentType="application/json",
                )
                unsaved.published = True
            except Exception:
                log.exception("saving the cost to the claim failed; will retry")
        if not unsaved.recorded:
            unsaved.recorded = await self._record(unsaved.body["result"])
        if unsaved.published and unsaved.recorded:
            self._unsaved = None

    async def _copy_answer(self, s3: Any, key: str, today: date) -> None:
        obj = await asyncio.to_thread(s3.get_object, Bucket=self._bucket, Key=key)
        body = await asyncio.to_thread(obj["Body"].read)
        result = json.loads(body).get("result")
        if result is None:
            # Claimed and not answered: in progress elsewhere, or its caller died. Look
            # again next run; there will be no second request either way.
            return
        if await self._record(result):  # if not, the next run copies it again
            self._done_on = today

    async def _record(self, row: dict[str, Any]) -> bool:
        try:
            await self._ch.insert("aws_cost", [row])
        except ClickHouseError as exc:
            log.error("saving the cost failed", extra={"error": str(exc)})
            return False
        log.info("cost recorded", extra={"ok": row["ok"], "amount": row["amount"]})
        return True

    @staticmethod
    def _row(now: datetime, today: date) -> dict[str, Any]:
        start, end = period(today)
        return {
            "fetched_at": now.isoformat(),
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
            "ok": True,
            "amount": "",
            "currency": "",
            "estimated": False,
            "error": "",
        }
