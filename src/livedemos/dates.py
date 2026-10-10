"""Timestamps as the API, the logs and ClickHouse write them."""

from __future__ import annotations

from datetime import UTC, date, datetime


def iso(value: float | datetime) -> str:
    """ISO 8601 UTC with milliseconds, `2026-10-04T18:00:00.123Z`, from Unix seconds or a
    datetime."""
    moment = (
        value.astimezone(UTC) if isinstance(value, datetime) else datetime.fromtimestamp(value, UTC)
    )
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def day_start(day: date) -> int:
    """Unix seconds at the start of a UTC day."""
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())
