"""The maintenance lock: one migration, rollup repair or rebuild at a time.

`CREATE TABLE` without IF NOT EXISTS is atomic, so exactly one caller gets it. A run that
crashed leaves the table behind; `livedemos-migrate --unlock` removes it.
"""

from __future__ import annotations

import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from livedemos.db.clickhouse import ClickHouse, ClickHouseError, quote

LOCK_TABLE = "schema_migrations_lock"


class LockHeld(RuntimeError):
    """Another migration, repair or rebuild holds the lock, or a crashed one left it."""


@asynccontextmanager
async def exclusive(ch: ClickHouse) -> AsyncIterator[None]:
    """Hold the maintenance lock for the duration of the block."""
    await _take(ch)
    try:
        yield
    finally:
        await unlock(ch)


async def unlock(ch: ClickHouse) -> None:
    await ch.execute(f"DROP TABLE IF EXISTS {ch.database}.{LOCK_TABLE}")


async def _take(ch: ClickHouse) -> None:
    holder = f"{socket.gethostname()} at {datetime.now(UTC).isoformat(timespec='seconds')}"
    try:
        # No IF NOT EXISTS: if the table is there, someone else holds the lock.
        await ch.execute(
            f"CREATE TABLE {ch.database}.{LOCK_TABLE} (x UInt8) ENGINE = Memory "
            f"COMMENT {quote(holder)}"
        )
    except ClickHouseError as exc:
        if "TABLE_ALREADY_EXISTS" not in str(exc) and "already exists" not in str(exc):
            raise
        raise LockHeld(held_message(await _holder(ch))) from exc


async def _holder(ch: ClickHouse) -> str:
    held = await ch.query(
        "SELECT comment FROM system.tables WHERE database = {db:String} AND name = {t:String}",
        params={"db": ch.database, "t": LOCK_TABLE},
    )
    return str(held.rows[0]["comment"]) if held.rows else "unknown"


def held_message(holder: str) -> str:
    return (
        f"another migration, repair or rebuild holds the lock ({holder}). If none is "
        "running, a previous run crashed: livedemos-migrate --unlock"
    )
