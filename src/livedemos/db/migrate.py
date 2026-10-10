"""Versioned schema migrations.

Each file in `migrations/` is applied once, in order, and recorded in `schema_migrations`
with a checksum of its statements (full-line comments excluded). Refused before anything
runs: a migration edited after it was applied, a database ahead of this code, and a second
run at the same time (the maintenance lock, rollup/lock.py).

ClickHouse has no transactional DDL, so a migration that fails halfway is retried from the
top. Write each statement so running it twice is harmless (IF NOT EXISTS, and so on).
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from importlib.resources import files

from livedemos.db.clickhouse import ClickHouse
from livedemos.rollup.lock import exclusive

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
    for path in files("livedemos.db").joinpath("migrations").iterdir():
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
    async with exclusive(ch):
        await ch.execute(_LEDGER.replace("{database}", db))
        return await _apply(ch, known, upto=upto)


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


async def _applied(ch: ClickHouse) -> dict[int, tuple[str, str]]:
    result = await ch.query("SELECT version, name, checksum FROM schema_migrations")
    out: dict[int, tuple[str, str]] = {}
    for row in result.rows:
        version = int(row["version"])
        if version in out:
            raise MigrationError(f"migration {version} is recorded twice in schema_migrations")
        out[version] = (str(row["name"]), str(row["checksum"]))
    return out
