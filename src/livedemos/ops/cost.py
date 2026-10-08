"""Month-to-date AWS cost, from Cost Explorer, at most once a day.

Cost Explorer charges $0.01 a request and its numbers move once a day or so, so the archive
service (the one process that runs only on the live host) asks once per UTC day and keeps
the answer in ClickHouse (`aws_cost`, migration 0005) for the API to serve. Failed attempts
are recorded too, so a broken permission isn't retried every few minutes.

The host's role may call `ce:GetCostAndUsage` and nothing else in Cost Explorer
(infra/live/host.tf). The filter is the `project` tag every resource carries.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

from livedemos.clickhouse import ClickHouseError, Database

log = logging.getLogger(__name__)

METRIC = "UnblendedCost"
_ATTEMPTED_TODAY = "SELECT count() AS n FROM aws_cost WHERE toDate(fetched_at) = {day:Date}"


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


class CostFetcher:
    def __init__(
        self,
        ch: Database,
        *,
        tag: str,
        client_factory: Callable[[], Any],
    ):
        key, sep, value = tag.partition("=")
        if not (key and sep and value):
            raise ValueError("tag must look like key=value")
        self._ch = ch
        self._tag = (key, value)
        self._client_factory = client_factory
        self._attempted_on: date | None = None

    async def refresh_if_due(self, now: datetime) -> bool:
        """Ask Cost Explorer unless today's attempt is already made. True if it asked."""
        today = now.astimezone(UTC).date()
        if self._attempted_on == today:
            return False
        try:
            done = await self._ch.query(_ATTEMPTED_TODAY, params={"day": today.isoformat()})
        except ClickHouseError as exc:  # can't tell, so don't spend a request
            log.warning("checking for today's cost failed", extra={"error": str(exc)})
            return False
        if int(done.rows[0]["n"]):
            self._attempted_on = today
            return False

        # From here on, today's attempt counts, whatever happens to the insert below.
        self._attempted_on = today
        start, end = period(today)
        row: dict[str, Any] = {
            "fetched_at": now.astimezone(UTC).isoformat(),
            "period_start": start.isoformat(),
            "period_end": end.isoformat(),
            "ok": True,
            "amount": "",
            "currency": "",
            "estimated": False,
            "error": "",
        }
        try:
            client = self._client_factory()
            response = await asyncio.to_thread(
                client.get_cost_and_usage, **request(today, *self._tag)
            )
            row["amount"], row["currency"], row["estimated"] = parse(response)
        except Exception as exc:  # recorded, and shown nowhere but logs and the table
            log.error("cost explorer request failed", extra={"error": repr(exc)[:300]})
            row["ok"], row["error"] = False, repr(exc)[:300]
        try:
            await self._ch.insert("aws_cost", [row])
        except ClickHouseError as exc:
            log.error("saving the cost failed", extra={"error": str(exc)})
        else:
            log.info("cost fetched", extra={"ok": row["ok"], "amount": row["amount"]})
        return True
