"""The Parquet archive and the rollup rebuild, against real ClickHouse and a real S3 API.

ClickHouse reaches S3 by the URL in ARCHIVE_TEST_URL, as ClickHouse sees it: SeaweedFS on
the same Docker network (`make test-integration` starts it). Each test writes under its
own prefix.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from livedemos.archive.job import (
    HOUR_S,
    Archiver,
    FewerRows,
    NothingToArchive,
    hour_url,
    s3_function,
)
from livedemos.archive.rebuild import ArchiveIncomplete, rebuild
from livedemos.clickhouse import ClickHouse, ClickHouseError
from livedemos.config import ArchiveSettings
from livedemos.migrate import MigrationError, exclusive

from .conftest import clickhouse_test_settings, rows, user_settings

pytestmark = pytest.mark.integration

NOW = datetime.now(UTC)
# Three hours on one UTC day, two days ago: inside the lookback, outside any settle window.
DAY = (NOW - timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
H0, H1, H2 = (int(DAY.timestamp()) + h * HOUR_S for h in (3, 4, 5))
WHOLE_DAY = {"first": DAY.date(), "end": DAY.date() + timedelta(days=1)}
OUTSIDE_DAY = f"SELECT sum(edits) FROM wiki_edits_per_minute WHERE toDate(minute) != '{DAY.date()}'"


@pytest.fixture
def settings() -> ArchiveSettings:
    base = os.environ.get("ARCHIVE_TEST_URL", "http://s3:8333/archive/test")
    return ArchiveSettings(url=f"{base}/{uuid4().hex}", nosign=True)


def at(seconds: int) -> datetime:
    return datetime.fromtimestamp(seconds, UTC)


@pytest.fixture
async def edits(ch: ClickHouse) -> ClickHouse:
    """50, 40 and 30 edits in H0, H1 and H2, and one just now, so ingest is past them all.
    One more the evening before, so ingest's coverage starts before H0: it's a whole hour."""
    await ch.insert("wiki_edits", rows(1, start=at(H0 - 4 * HOUR_S + 600), first_seq=1000))
    seq = 1
    for hour_s, n in ((H0, 50), (H1, 40), (H2, 30)):
        await ch.insert("wiki_edits", rows(n, start=at(hour_s + 10), bot_every=4, first_seq=seq))
        seq += n
    await ch.insert("wiki_edits", rows(1, first_seq=seq))
    return ch


async def scalar(ch: ClickHouse, sql: str) -> int:
    return int(next(iter((await ch.query(sql)).rows[0].values())))


async def file_count(ch: ClickHouse, settings: ArchiveSettings, hour_s: int) -> int:
    result = await ch.query(
        f"SELECT count() AS n FROM {s3_function(settings, 'Parquet')}",
        params={"url": hour_url(settings.url, hour_s)},
        settings={"use_hive_partitioning": "0"},
    )
    return int(result.rows[0]["n"])


async def per_minute(ch: ClickHouse) -> list[tuple[str, str, int, int]]:
    result = await ch.query(
        "SELECT toString(minute) AS m, lang, sum(edits) AS e, sum(bot_edits) AS b "
        "FROM wiki_edits_per_minute WHERE toDate(minute) = {day:Date} "
        "GROUP BY minute, lang ORDER BY minute, lang",
        params={"day": DAY.date().isoformat()},
    )
    return [(r["m"], r["lang"], int(r["e"]), int(r["b"])) for r in result.rows]


async def test_each_finished_hour_becomes_one_file_with_every_row(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    archiver = Archiver(settings, edits)
    results = await archiver.run_once(NOW)

    assert {r.hour_s: (r.result, r.rows) for r in results} == {
        H0: ("written", 50),
        H1: ("written", 40),
        H2: ("written", 30),
    }
    for hour_s, n in ((H0, 50), (H1, 40), (H2, 30)):
        assert await file_count(edits, settings, hour_s) == n
    assert await archiver.run_once(NOW) == []  # nothing left to do


async def test_an_hour_ingest_hasnt_passed_yet_waits(
    ch: ClickHouse, settings: ArchiveSettings
) -> None:
    await ch.insert("wiki_edits", rows(10, start=at(H0 + 10)))
    # Newest event 2 minutes after H0 ends: inside the 5-minute settle window.
    await ch.insert("wiki_edits", rows(1, start=at(H0 + HOUR_S + 120), first_seq=11))
    assert (await Archiver(settings, ch).plan(NOW)).due == []


async def test_a_late_event_rewrites_its_hour(edits: ClickHouse, settings: ArchiveSettings) -> None:
    """An old event that arrives after its hour was archived (stream skew, a replay)."""
    archiver = Archiver(settings, edits)
    await archiver.run_once(NOW)
    await edits.insert("wiki_edits", rows(1, start=at(H0 + 1800), first_seq=500))

    results = await archiver.run_once(NOW)
    assert [(r.hour_s, r.result, r.rows) for r in results] == [(H0, "written", 51)]
    assert await file_count(edits, settings, H0) == 51


async def test_a_file_is_never_replaced_by_one_with_fewer_rows(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    """A host rebuilt from scratch: thinner raw rows, and no record of what it wrote."""
    archiver = Archiver(settings, edits)
    await archiver.run_once(NOW)
    await edits.execute("TRUNCATE TABLE archive_hours")
    await edits.execute(
        "ALTER TABLE wiki_edits DELETE WHERE event_time >= fromUnixTimestamp({from_s:Int64}) "
        "AND event_time < fromUnixTimestamp({to_s:Int64}) SETTINGS mutations_sync = 1",
        params={"from_s": H0, "to_s": H0 + 30},
    )

    assert await archiver.run_once(NOW) == []
    assert await file_count(edits, settings, H0) == 50
    assert (await archiver.plan(NOW)).archived[H0] == 50  # found and recorded

    # Rewriting it anyway is a deliberate act.
    rewritten = await archiver.archive_hour(H0, manual=True)
    assert (rewritten.result, rewritten.rows) == ("written", 30)


async def test_a_scheduled_write_never_goes_ahead_with_fewer_rows(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    """Raw rows shrank between planning and writing: the file is left as it is."""
    archiver = Archiver(settings, edits)
    await archiver.run_once(NOW)
    with pytest.raises(FewerRows):
        await archiver.archive_hour(H0, floor=60)
    assert await file_count(edits, settings, H0) == 50


async def test_a_manual_rewrite_of_an_empty_hour_is_refused(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    with pytest.raises(NothingToArchive):
        await Archiver(settings, edits).archive_hour(H0 - HOUR_S, manual=True)


async def test_rebuild_drill_restores_the_rollup_from_the_archive(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    await Archiver(settings, edits).run_once(NOW)
    before = await per_minute(edits)
    assert sum(e for _, _, e, _ in before) == 120

    other_days = await scalar(edits, OUTSIDE_DAY)
    assert other_days >= 1  # the evening before, at least

    await edits.execute(
        "ALTER TABLE wiki_edits_per_minute DELETE WHERE toDate(minute) = {day:Date} "
        "SETTINGS mutations_sync = 1",
        params={"day": DAY.date().isoformat()},
    )
    # The test day only has three hours of edits; the other 21 have no file.
    rollup_rows = await rebuild(
        edits, settings, **WHOLE_DAY, allow_missing=True, quiet=timedelta(0)
    )

    assert await per_minute(edits) == before
    assert rollup_rows == len(before)
    assert await scalar(edits, OUTSIDE_DAY) == other_days  # the rest of the month kept


async def test_rebuild_refuses_hours_without_a_file_even_after_the_rollup_is_gone(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    await Archiver(settings, edits).archive_hour(H0)  # H1 and H2 never archived
    await edits.execute("TRUNCATE TABLE wiki_edits_per_minute")
    with pytest.raises(ArchiveIncomplete, match="23 hour"):
        await rebuild(edits, settings, **WHOLE_DAY, quiet=timedelta(0))


async def test_a_rebuild_that_cant_read_the_archive_leaves_the_rollup_alone(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    await Archiver(settings, edits).run_once(NOW)
    # H1's file is now garbage: not Parquet at all.
    await edits.execute(
        f"INSERT INTO FUNCTION {s3_function(settings, 'CSV')} SELECT 'not parquet'",
        params={"url": hour_url(settings.url, H1)},
        settings={"use_hive_partitioning": "0", "s3_truncate_on_insert": "1"},
    )
    before = await per_minute(edits)
    with pytest.raises(ClickHouseError):
        await rebuild(edits, settings, **WHOLE_DAY, allow_missing=True, quiet=timedelta(0))
    assert await per_minute(edits) == before


async def test_a_rebuild_refuses_a_file_that_lost_rows(
    edits: ClickHouse, settings: ArchiveSettings
) -> None:
    """A valid file for the right hour, but only part of it: archive_hours knows better."""
    await Archiver(settings, edits).run_once(NOW)
    await edits.execute(
        f"INSERT INTO FUNCTION {s3_function(settings, 'Parquet')} "
        "SELECT toString(event_id) AS event_id, event_time, ingested_at, wiki, lang, type, "
        "namespace, title, is_bot FROM wiki_edits "
        "WHERE toStartOfHour(event_time) = fromUnixTimestamp({h:Int64}) LIMIT 10",
        params={"url": hour_url(settings.url, H1), "h": H1},
        settings={"use_hive_partitioning": "0", "s3_truncate_on_insert": "1"},
    )
    before = await per_minute(edits)
    with pytest.raises(ArchiveIncomplete, match="don't hold the rows"):
        await rebuild(edits, settings, **WHOLE_DAY, allow_missing=True, quiet=timedelta(0))
    assert await per_minute(edits) == before


async def test_a_rebuild_waits_its_turn(edits: ClickHouse, settings: ArchiveSettings) -> None:
    """One migration, repair or rebuild at a time: they share the staging table and lock."""
    await Archiver(settings, edits).run_once(NOW)
    async with exclusive(edits):
        with pytest.raises(MigrationError, match="holds the lock"):
            await rebuild(edits, settings, **WHOLE_DAY, allow_missing=True, quiet=timedelta(0))


async def test_duckdb_reads_every_column(edits: ClickHouse, settings: ArchiveSettings) -> None:
    """The README's way in: DuckDB over S3. Every column, its type and its values."""
    duckdb = pytest.importorskip("duckdb")
    await Archiver(settings, edits).run_once(NOW)
    expected = await edits.query(
        "SELECT toString(event_id) AS event_id, wiki, lang, type, namespace, title, is_bot, "
        "toUnixTimestamp64Milli(event_time) AS ms FROM wiki_edits "
        "WHERE toStartOfHour(event_time) = fromUnixTimestamp({h:Int64}) "
        "ORDER BY event_time, event_id",
        params={"h": H0},
    )

    endpoint = os.environ.get("ARCHIVE_TEST_S3_ENDPOINT", "localhost:8333")
    path = "s3://" + settings.url.split("://", 1)[1].split("/", 1)[1]  # drop scheme and host
    db = duckdb.connect()
    db.execute("INSTALL httpfs; LOAD httpfs;")
    db.execute(
        "CREATE SECRET (TYPE s3, KEY_ID 'any', SECRET 'any', ENDPOINT ?, URL_STYLE 'path', "
        "USE_SSL false, REGION 'us-east-1')",
        [endpoint],
    )
    source = f"read_parquet('{path}/dt=*/hour=*.parquet', hive_partitioning = true)"
    types = {row[0]: row[1] for row in db.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()}
    assert types == {
        "event_id": "VARCHAR",
        "event_time": "TIMESTAMP WITH TIME ZONE",
        "ingested_at": "TIMESTAMP WITH TIME ZONE",
        "wiki": "VARCHAR",
        "lang": "VARCHAR",
        "type": "VARCHAR",
        "namespace": "INTEGER",
        "title": "VARCHAR",
        "is_bot": "BOOLEAN",
        "dt": "DATE",
    }
    got = db.execute(
        "SELECT event_id, wiki, lang, type, namespace, title, is_bot, epoch_ms(event_time) "
        f"FROM {source} WHERE date_trunc('hour', event_time) = to_timestamp(?) "
        "ORDER BY event_time, event_id",
        [H0],
    ).fetchall()
    assert [tuple(r.values()) for r in expected.rows] == got
    assert db.execute(f"SELECT DISTINCT dt FROM {source}").fetchall() == [(DAY.date(),)]


class DedupEverything(ClickHouse):
    """A server whose default profile turns INSERT ... SELECT deduplication on: the
    settings the code passes still apply on top."""

    async def execute(self, sql: str, *, params: Any = None, settings: Any = None) -> None:
        forced = {"deduplicate_insert_select": "enable_even_for_bad_queries"}
        await super().execute(sql, params=params, settings={**forced, **(settings or {})})


async def test_a_rebuild_twice_is_still_right(edits: ClickHouse, settings: ArchiveSettings) -> None:
    """Each rebuild's block matches one ClickHouse has seen (the view's, then the first
    rebuild's). With INSERT ... SELECT dedup on, it would be dropped after the DELETE."""
    await Archiver(settings, edits).run_once(NOW)
    before = await per_minute(edits)
    forced = DedupEverything(clickhouse_test_settings())
    try:
        for _ in range(2):
            await rebuild(forced, settings, **WHOLE_DAY, allow_missing=True, quiet=timedelta(0))
    finally:
        await forced.aclose()
    assert await per_minute(edits) == before


@pytest.fixture
async def archiver_user() -> AsyncIterator[ClickHouse]:
    client = ClickHouse(user_settings("archiver", "CLICKHOUSE_ARCHIVER_PASSWORD"))
    yield client
    await client.aclose()


@pytest.fixture
async def migrator_user() -> AsyncIterator[ClickHouse]:
    client = ClickHouse(user_settings("migrator", "CLICKHOUSE_MIGRATOR_PASSWORD"))
    yield client
    await client.aclose()


@pytest.mark.usefixtures("demos_schema")
async def test_the_archiver_user_can_do_its_job(
    archiver_user: ClickHouse, settings: ArchiveSettings
) -> None:
    """As the real user, on `demos`: plan, write a file, list it, read it back."""
    archiver = Archiver(settings, archiver_user)
    await archiver.plan(NOW)
    await archiver_user.execute(
        f"INSERT INTO FUNCTION {s3_function(settings, 'Parquet')} SELECT 1 AS x",
        params={"url": hour_url(settings.url, H0)},
        settings={"use_hive_partitioning": "0", "s3_truncate_on_insert": "1"},
    )
    assert await archiver.existing_hours([H0]) == {H0}
    assert await file_count(archiver_user, settings, H0) == 1


@pytest.mark.usefixtures("demos_schema")
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count() FROM demos.ingest_gaps",
        "INSERT INTO demos.wiki_edits (title) VALUES ('x')",
        "SELECT * FROM url('http://example.com', 'One')",
        "SELECT * FROM file('anything', 'One')",
        "ALTER TABLE demos.wiki_edits DELETE WHERE 1",
        # S3, but not the archive: refused, so the user can't copy data elsewhere.
        "INSERT INTO FUNCTION s3('http://s3:8333/elsewhere/x.parquet', NOSIGN, 'Parquet') "
        "SELECT 1 AS x",
    ],
)
async def test_the_archiver_user_can_do_nothing_else(archiver_user: ClickHouse, sql: str) -> None:
    with pytest.raises(ClickHouseError, match=r"ACCESS_DENIED|Not enough privileges"):
        await archiver_user.execute(sql)


@pytest.mark.usefixtures("demos_schema")
async def test_the_migrator_reads_the_archive_but_cant_write_it(
    migrator_user: ClickHouse, settings: ArchiveSettings
) -> None:
    await Archiver(settings, migrator_user).existing_hours([H0])  # listing is a read
    with pytest.raises(ClickHouseError, match=r"ACCESS_DENIED|Not enough privileges"):
        await migrator_user.execute(
            f"INSERT INTO FUNCTION {s3_function(settings, 'Parquet')} SELECT 1 AS x",
            params={"url": hour_url(settings.url, H0)},
            settings={"use_hive_partitioning": "0"},
        )


@pytest.mark.usefixtures("demos_schema")
@pytest.mark.parametrize(
    ("user", "password_env", "allowed"),
    [
        ("archiver", "CLICKHOUSE_ARCHIVER_PASSWORD", "1"),
        ("migrator", "CLICKHOUSE_MIGRATOR_PASSWORD", "1"),
        ("ingest", "CLICKHOUSE_INGEST_PASSWORD", "0"),
        ("api", "CLICKHOUSE_API_PASSWORD", "0"),
    ],
)
async def test_only_the_archive_users_may_sign_s3_with_the_instance_role(
    user: str, password_env: str, allowed: str
) -> None:
    """26.8 refuses the server's own credentials to s3() unless the profile allows it.
    Locally S3 is unsigned, so this is the only test that sees the production path."""
    client = ClickHouse(user_settings(user, password_env))
    try:
        result = await client.query(
            "SELECT value FROM system.settings "
            "WHERE name = 's3_allow_server_credentials_in_user_queries'"
        )
    finally:
        await client.aclose()
    assert result.rows[0]["value"] == allowed
