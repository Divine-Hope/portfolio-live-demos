"""Versioned schema migrations: `python -m livedemos.migrate`.

Each file in `migrations/` is applied once, in order, and recorded in `schema_migrations`
with its name and a checksum of its statements (full-line comments excluded). Refused,
before anything runs:

- a migration edited after it was applied (write a new one instead),
- a database ahead of this code (an older build deployed over a newer schema),
- a second `migrate` running at the same time: `CREATE TABLE schema_migrations_lock` is
  atomic, so exactly one run gets it. A run that crashed leaves it behind; check nothing
  else is migrating, then `python -m livedemos.migrate --unlock`.

ClickHouse has no transactional DDL, so a migration that fails halfway is retried from the
top on the next run. Write each statement so running it twice is harmless (IF NOT EXISTS,
MATERIALIZE, and so on).

Runs as the `migrator` user, the only one allowed to change the schema. Ingest and the API
only read and write rows.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import re
import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.resources import files

from livedemos.clickhouse import ClickHouse, ClickHouseError
from livedemos.config import clickhouse_settings
from livedemos.logs import setup_logging

log = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_END_OF_STATEMENT = re.compile(r";[ \t]*$", re.MULTILINE)
_FILENAME = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")

_LEDGER = """
CREATE TABLE IF NOT EXISTS {database}.schema_migrations
(
    version     UInt32,
    name        String,
    checksum    String,
    applied_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY version
"""


_LOCK = "schema_migrations_lock"


class MigrationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    statements: tuple[str, ...]

    @property
    def checksum(self) -> str:
        # Over the statements, not the file: editing a comment isn't a schema change.
        return hashlib.sha256("\n;\n".join(self.statements).encode()).hexdigest()[:16]


def load(database: str) -> list[Migration]:
    """Every migration, in version order, rendered for `database`."""
    if not _IDENTIFIER.match(database):
        raise ValueError(f"invalid database name: {database!r}")
    out = []
    for path in files("livedemos").joinpath("migrations").iterdir():
        match = _FILENAME.match(path.name)
        if not match:
            continue
        out.append(
            Migration(
                version=int(match[1]),
                name=match[2],
                statements=tuple(_statements(path.read_text(), database)),
            )
        )
    out.sort(key=lambda m: m.version)
    versions = [m.version for m in out]
    if versions != list(range(1, len(out) + 1)):
        raise MigrationError(f"migration versions must be 1..n with no gaps, got {versions}")
    return out


def _statements(text: str, database: str) -> list[str]:
    without_comments = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("--")
    )
    rendered = without_comments.replace("{database}", database)
    # Statements end with a semicolon at the end of a line, so a ';' inside a string is safe.
    return [stmt.strip() for stmt in _END_OF_STATEMENT.split(rendered) if stmt.strip()]


async def migrate(ch: ClickHouse, *, upto: int | None = None) -> list[int]:
    """Apply pending migrations (up to version `upto`). Returns the versions applied."""
    db = ch.database
    known = load(db)
    # CREATE DATABASE must not run inside the database it creates.
    await ch.execute(f"CREATE DATABASE IF NOT EXISTS {db}", settings={"database": "default"})
    await _lock(ch)
    try:
        await ch.execute(_LEDGER.replace("{database}", db))
        return await _apply(ch, known, upto=upto)
    finally:
        await ch.execute(f"DROP TABLE IF EXISTS {db}.{_LOCK}")


async def _apply(ch: ClickHouse, known: list[Migration], *, upto: int | None) -> list[int]:
    applied = await _applied(ch)
    head = known[-1].version if known else 0
    ahead = sorted(v for v in applied if v > head)
    if ahead:
        raise MigrationError(
            f"the database has migrations {ahead} that this code doesn't know (it stops at "
            f"{head}). Deploy a build that includes them; rolling back means a new migration."
        )

    done = []
    for migration in known:
        if upto is not None and migration.version > upto:
            break
        recorded = applied.get(migration.version)
        if recorded is not None:
            if recorded != (migration.name, migration.checksum):
                raise MigrationError(
                    f"migration {migration.version:04d}_{migration.name} changed after it was "
                    "applied. Add a new migration instead of editing an old one."
                )
            continue
        log.info(
            "applying migration",
            extra={"version": migration.version, "migration": migration.name},
        )
        for sql in migration.statements:
            if sql.upper().startswith("CREATE DATABASE"):
                await ch.execute(sql, settings={"database": "default"})
            else:
                await ch.execute(sql)
        await ch.insert(
            "schema_migrations",
            [
                {
                    "version": migration.version,
                    "name": migration.name,
                    "checksum": migration.checksum,
                }
            ],
        )
        done.append(migration.version)
    log.info("schema up to date", extra={"database": ch.database, "applied": done})
    return done


async def _lock(ch: ClickHouse) -> None:
    holder = f"{socket.gethostname()} at {datetime.now(UTC).isoformat(timespec='seconds')}"
    try:
        # No IF NOT EXISTS: if the table is there, someone else holds the lock.
        await ch.execute(
            f"CREATE TABLE {ch.database}.{_LOCK} (x UInt8) ENGINE = Memory COMMENT {_quote(holder)}"
        )
    except ClickHouseError as exc:
        if "TABLE_ALREADY_EXISTS" not in str(exc) and "already exists" not in str(exc):
            raise
        held = await ch.query(
            "SELECT comment FROM system.tables WHERE database = {db:String} AND name = {t:String}",
            params={"db": ch.database, "t": _LOCK},
        )
        by = held.rows[0]["comment"] if held.rows else "unknown"
        raise MigrationError(
            f"another migrate holds the lock ({by}). If none is running, a previous run "
            "crashed: python -m livedemos.migrate --unlock"
        ) from exc


async def _applied(ch: ClickHouse) -> dict[int, tuple[str, str]]:
    result = await ch.query("SELECT version, name, checksum FROM schema_migrations")
    out: dict[int, tuple[str, str]] = {}
    for row in result.rows:
        version = int(row["version"])
        if version in out:
            raise MigrationError(f"migration {version} is recorded twice in schema_migrations")
        out[version] = (str(row["name"]), str(row["checksum"]))
    return out


def _quote(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


async def _main(unlock: bool) -> None:
    setup_logging()
    ch = ClickHouse(clickhouse_settings())
    try:
        if unlock:
            await ch.execute(f"DROP TABLE IF EXISTS {ch.database}.{_LOCK}")
            log.info("lock removed")
        else:
            await migrate(ch)
    finally:
        await ch.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply pending schema migrations.")
    parser.add_argument("--unlock", action="store_true", help="remove a crashed run's lock")
    asyncio.run(_main(parser.parse_args().unlock))
