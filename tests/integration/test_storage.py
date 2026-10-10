"""Schema, inserts and queries against a real ClickHouse."""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from livedemos.api.activity import ActivityService, parse_request
from livedemos.api.snapshot import Snapshotter
from livedemos.config import ApiSettings
from livedemos.db.clickhouse import ClickHouse, ClickHouseError
from livedemos.db.migrate import MigrationError, _statements, migrate
from livedemos.ingest.resume import load_resume_state, wait_for_inflight_inserts
from livedemos.rollup.lock import LockHeld
from livedemos.rollup.maintenance import IngestRunning
from livedemos.rollup.reconcile import Mismatch, find_mismatches, repair

from .conftest import TEST_DB, clickhouse_test_settings, rows, user_settings

pytestmark = pytest.mark.integration


async def scalar(ch: ClickHouse, sql: str) -> int:
    result = await ch.query(sql)
    return int(next(iter(result.rows[0].values())))


async def test_migrations_run_once_and_are_recorded(ch: ClickHouse) -> None:
    assert await migrate(ch) == []  # the fixture already applied everything
    tables = await ch.query(
        "SELECT name FROM system.tables WHERE database = {db:String} ORDER BY name",
        params={"db": ch.database},
    )
    assert [r["name"] for r in tables.rows] == [
        "archive_hours",
        "aws_cost",
        "freshness_samples",
        "freshness_samples_mv",
        "ingest_gaps",
        "ingest_reconnects",
        "schema_migrations",
        "wiki_edits",
        "wiki_edits_per_minute",
        "wiki_edits_per_minute_mv",
        "wiki_edits_per_minute_staging",
        "wiki_pages_filled",
        "wiki_pages_per_minute",
        "wiki_pages_per_minute_mv",
    ]
    applied = await ch.query("SELECT version FROM schema_migrations ORDER BY version")
    assert [r["version"] for r in applied.rows] == [1, 2, 3, 4, 5, 6]


async def test_an_existing_database_upgrades_in_place(ch: ClickHouse) -> None:
    """An install from before migration 2 keeps its rows and gains the projections."""
    await ch.execute(f"DROP DATABASE {TEST_DB} SYNC", settings={"database": "default"})
    assert await migrate(ch, upto=1) == [1]
    await ch.insert("wiki_edits", rows(50), dedup_token="before-upgrade")

    assert await migrate(ch) == [2, 3, 4, 5, 6]
    projections = await ch.query(
        "SELECT DISTINCT name FROM system.projection_parts "
        "WHERE database = {db:String} AND table = 'wiki_edits' AND active ORDER BY name",
        params={"db": ch.database},
    )
    # Materialized for the parts written before the upgrade, not only new ones.
    assert [r["name"] for r in projections.rows] == ["by_ingest_seq", "seq_max"]
    assert await scalar(ch, "SELECT count() FROM wiki_edits") == 50


async def test_a_database_from_before_versioned_migrations_is_adopted(ch: ClickHouse) -> None:
    """Created by the old unversioned schema.sql, with rows in it: nothing is lost."""
    await ch.execute(f"DROP DATABASE {TEST_DB} SYNC", settings={"database": "default"})
    legacy = (Path(__file__).parents[1] / "fixtures" / "legacy_schema.sql").read_text()
    for sql in _statements(legacy, TEST_DB):
        db = "default" if sql.startswith("CREATE DATABASE") else TEST_DB
        await ch.execute(sql, settings={"database": db})
    await ch.insert("wiki_edits", rows(40), dedup_token="legacy")

    assert await migrate(ch) == [1, 2, 3, 4, 5, 6]  # 1 is a no-op that records the baseline
    assert await scalar(ch, "SELECT count() FROM wiki_edits") == 40
    assert await scalar(ch, "SELECT sum(edits) FROM wiki_edits_per_minute") == 40


async def test_a_database_ahead_of_the_code_is_refused(ch: ClickHouse) -> None:
    """An older build deployed over a newer schema must not start on it."""
    await ch.insert("schema_migrations", [{"version": 99, "name": "future", "checksum": "x"}])
    with pytest.raises(MigrationError, match=r"\[99\]"):
        await migrate(ch)


async def test_a_duplicated_ledger_row_is_refused(ch: ClickHouse) -> None:
    await ch.insert("schema_migrations", [{"version": 1, "name": "initial", "checksum": "x"}])
    with pytest.raises(MigrationError, match="recorded twice"):
        await migrate(ch)


async def test_only_one_migrate_runs_at_a_time(ch: ClickHouse) -> None:
    second = ClickHouse(clickhouse_test_settings())
    try:
        results = await asyncio.gather(migrate(ch), migrate(second), return_exceptions=True)
    finally:
        await second.aclose()
    # Both may succeed one after the other (nothing left to apply for the second); what
    # must never happen is both applying, or a lock left behind.
    assert all(isinstance(r, (list, LockHeld)) for r in results), results
    applied = await ch.query("SELECT version, count() AS n FROM schema_migrations GROUP BY version")
    assert all(int(r["n"]) == 1 for r in applied.rows)
    lock = (
        "SELECT count() FROM system.tables "
        "WHERE database = currentDatabase() AND name = 'schema_migrations_lock'"
    )
    assert await scalar(ch, lock) == 0


async def test_a_crashed_runs_lock_blocks_until_removed(ch: ClickHouse) -> None:
    await ch.execute(
        "CREATE TABLE schema_migrations_lock (x UInt8) ENGINE = Memory COMMENT 'crashed'"
    )
    with pytest.raises(LockHeld, match="crashed"):
        await migrate(ch)
    await ch.execute("DROP TABLE schema_migrations_lock")
    assert await migrate(ch) == []


async def test_editing_an_applied_migration_is_refused(ch: ClickHouse) -> None:
    await ch.execute(
        "ALTER TABLE schema_migrations UPDATE checksum = 'edited' WHERE version = 1 "
        "SETTINGS mutations_sync = 1"
    )
    with pytest.raises(MigrationError, match="changed after it was applied"):
        await migrate(ch)


async def test_a_retried_batch_is_written_once_including_the_rollup(ch: ClickHouse) -> None:
    batch = rows(50)
    await ch.insert("wiki_edits", batch, dedup_token="batch-1")
    await ch.insert("wiki_edits", batch, dedup_token="batch-1")  # timeout-then-retry
    assert await scalar(ch, "SELECT count() FROM wiki_edits") == 50
    assert await scalar(ch, "SELECT sum(edits) FROM wiki_edits_per_minute") == 50


async def test_rollup_matches_raw_per_minute(ch: ClickHouse) -> None:
    for i in range(5):
        await ch.insert("wiki_edits", rows(40, lang=["en", "pt"][i % 2]), dedup_token=f"b{i}")
    mismatches = await scalar(
        ch,
        """
        SELECT count() FROM (
            SELECT toStartOfMinute(event_time) AS minute, lang, count() AS raw
            FROM wiki_edits GROUP BY minute, lang
        ) AS r
        FULL OUTER JOIN (
            SELECT minute, lang, sum(edits) AS rolled
            FROM wiki_edits_per_minute GROUP BY minute, lang
        ) AS m USING (minute, lang)
        WHERE raw != rolled
        """,
    )
    assert mismatches == 0


def seq_at(when: datetime) -> int:
    return int(when.timestamp() * 1e9)  # ingest_seq is wall-clock ns


async def test_resume_state_points_at_the_newest_bookmark(ch: ClickHouse) -> None:
    start_seq = seq_at(datetime.now(UTC))
    await ch.insert("wiki_edits", rows(30, first_seq=start_seq), dedup_token="b")
    state = await load_resume_state(
        ch,
        now=datetime.now(UTC),
        retention=timedelta(days=7),
        lookback=timedelta(hours=1),
        seam_ids=20_000,
    )
    assert state.bookmark == '[{"timestamp":29}]'  # highest ingest_seq
    assert state.since is None
    assert state.last_seq == start_seq + 29
    assert len(state.seam_ids) == 30


async def test_the_bookmark_is_the_last_row_ingested_however_old_its_event(
    ch: ClickHouse,
) -> None:
    """The last row committed can carry an event older than a day. It's still the bookmark."""
    now = datetime.now(UTC)
    await ch.insert("wiki_edits", rows(20, first_seq=seq_at(now) - 10**9), dedup_token="new")
    late = rows(1, start=now - timedelta(days=2), first_seq=seq_at(now))
    late[0]["sse_id"] = "the-late-one"
    await ch.insert("wiki_edits", late, dedup_token="late")
    state = await load_resume_state(
        ch, now=now, retention=timedelta(days=7), lookback=timedelta(hours=1), seam_ids=100
    )
    assert state.bookmark == "the-late-one"
    assert state.last_seq == seq_at(now)


async def test_the_seam_is_the_last_rows_even_across_a_clock_jump(ch: ClickHouse) -> None:
    """The wall clock jumped two hours forward between two inserts. The seam must still be
    the last N rows ingested, not only those within an hour of the newest."""
    now = datetime.now(UTC)
    before = rows(30, first_seq=seq_at(now - timedelta(hours=2)))
    after = rows(5, first_seq=seq_at(now))
    await ch.insert("wiki_edits", before, dedup_token="before-jump")
    await ch.insert("wiki_edits", after, dedup_token="after-jump")
    state = await load_resume_state(
        ch, now=now, retention=timedelta(days=7), lookback=timedelta(hours=1), seam_ids=20
    )
    assert len(state.seam_ids) == 20
    assert set(state.seam_ids) >= {str(r["event_id"]) for r in after}


async def test_first_boot_and_too_old_bookmarks(ch: ClickHouse) -> None:
    now = datetime.now(UTC)
    first = await load_resume_state(
        ch,
        now=now,
        retention=timedelta(days=7),
        lookback=timedelta(hours=1),
        seam_ids=20_000,
    )
    assert first.bookmark is None
    assert first.since == now - timedelta(hours=1)

    assert first.gap is None  # nothing ran before: not a gap

    # Two hours old, with a one-hour retention: the source can't replay that far back.
    await ch.insert("wiki_edits", rows(5, start=now - timedelta(hours=2)), dedup_token="old")
    old = await load_resume_state(
        ch,
        now=now,
        retention=timedelta(hours=1),
        lookback=timedelta(minutes=30),
        seam_ids=20_000,
    )
    assert old.bookmark is None
    assert old.since == now - timedelta(minutes=30)
    assert old.gap is not None
    assert old.gap[1] == old.since


async def test_raw_rows_expired_but_rollup_remembers_is_a_gap_not_a_first_boot(
    ch: ClickHouse,
) -> None:
    now = datetime.now(UTC)
    await ch.insert("wiki_edits", rows(10, start=now - timedelta(days=9)), dedup_token="old")
    # Raw TTL has dropped them; the 90-day rollup still has their minutes.
    await ch.execute("TRUNCATE TABLE wiki_edits")
    state = await load_resume_state(
        ch, now=now, retention=timedelta(days=7), lookback=timedelta(hours=1), seam_ids=100
    )
    assert state.bookmark is None
    assert state.gap is not None
    gap_from, gap_to = state.gap
    assert now - timedelta(days=9, minutes=1) < gap_from < now - timedelta(days=8)
    assert gap_from.second == 0  # the whole last minute: ingest may have stopped inside it
    assert gap_to == state.since == now - timedelta(days=7)  # replays all the source has


async def test_a_rollup_minute_without_raw_rows_is_marked_as_a_gap(ch: ClickHouse) -> None:
    """Raw rows gone, the rollup's last minute recent: it may be partial, so it's a gap."""
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    last = now - timedelta(minutes=50)
    await ch.insert(
        "wiki_edits_per_minute",
        [{"minute": last.isoformat(), "lang": "en", "edits": 3, "bot_edits": 0}],
    )
    state = await load_resume_state(
        ch, now=now, retention=timedelta(days=7), lookback=timedelta(hours=1), seam_ids=100
    )
    # Not an hour back: that would count the rollup's minutes twice.
    assert state.since == state.floor == last + timedelta(minutes=1)
    assert state.gap == (last, last + timedelta(minutes=1))


async def test_start_waits_for_an_insert_still_running_on_the_server(ch: ClickHouse) -> None:
    await ch.execute("CREATE TABLE slow_sink (z UInt8) ENGINE = Null")
    await ch.execute(
        "CREATE MATERIALIZED VIEW slow_mv TO slow_sink AS SELECT sleep(1.5) AS z FROM wiki_edits"
    )
    # A predecessor's insert, still running when the new process starts.
    slow = asyncio.create_task(ch.insert("wiki_edits", rows(5), query_id="ingest-left-behind"))
    await asyncio.sleep(0.3)
    started = time.monotonic()
    assert await wait_for_inflight_inserts(ch, timeout_s=10)
    assert time.monotonic() - started > 0.5  # it really waited
    assert slow.done()
    assert await scalar(ch, "SELECT count() FROM wiki_edits") == 5
    assert await wait_for_inflight_inserts(ch, timeout_s=0)  # nothing running now


async def test_snapshot_from_real_rows(ch: ClickHouse) -> None:
    await ch.insert("wiki_edits", rows(100, lang="en", bot_every=4), dedup_token="en")
    await ch.insert("wiki_edits", rows(20, lang="de"), dedup_token="de")
    snapshotter = Snapshotter(ch, langs=["en", "pt", "de"], interval_s=1, stale_after_s=60)
    payload = json.loads((await snapshotter.build()).body)
    assert payload["status"] == "live"
    assert payload["langs"]["en"]["edits_5m"] == 100
    assert payload["langs"]["all"]["edits_5m"] == 120
    assert payload["langs"]["en"]["bot_share_5m"] == 0.25
    assert payload["langs"]["en"]["pages_5m"] == 3
    assert payload["langs"]["all"]["top_articles"][0]["edits"] >= 34
    assert len(payload["langs"]["all"]["per_minute"]) == 60


async def test_query_it_reports_clickhouse_timing(ch: ClickHouse) -> None:
    await ch.insert("wiki_edits", rows(200), dedup_token="x")
    service = ActivityService(ch, ApiSettings())
    started = time.perf_counter()
    payload = await service.get(parse_request("en", "1h", allowed=["en", "pt", "de"]))
    assert time.perf_counter() - started < 1.0
    assert payload["edits"] == 200
    assert payload["query"]["rows_read"] >= 200
    assert payload["query"]["elapsed_ms"] > 0

    # A week comes from per-minute tables: the same edits and the same exact page count as
    # raw rows give, reading fewer rows.
    week = await service.get(parse_request("en", "7d", allowed=["en", "pt", "de"]))
    assert week["edits"] == 200
    assert week["pages_edited"] == payload["pages_edited"]
    assert week["query"]["rows_read"] < 200


async def test_long_windows_count_the_same_pages_as_raw_rows(ch: ClickHouse) -> None:
    """Pages edited in several minutes, several inserts and two languages count once each,
    and the per-minute sets agree exactly with counting raw rows."""
    now = datetime.now(UTC)
    for i, lang in enumerate(("en", "de", "en", "de", "en")):  # the same titles, again
        start = now - timedelta(minutes=40 - i * 7)
        await ch.insert("wiki_edits", rows(90, lang=lang, start=start), dedup_token=f"p{i}")
    service = ActivityService(ch, ApiSettings())
    langs = ["en", "pt", "de"]
    raw = await service.get(parse_request("en,de", "24h", allowed=langs))
    for window in ("3d", "7d"):
        sets = await service.get(parse_request("en,de", window, allowed=langs))
        assert (sets["edits"], sets["pages_edited"]) == (raw["edits"], raw["pages_edited"])
    assert raw["pages_edited"] == 6  # three titles a language (rows() cycles three)
    assert raw["edits"] == 450


async def test_a_repair_completes_a_page_set_the_view_missed(ch: ClickHouse) -> None:
    now = datetime.now(UTC) - timedelta(hours=1)
    await ch.insert("wiki_edits", rows(120, start=now), dedup_token="r")
    week = parse_request("en", "7d", allowed=["en", "pt", "de"])
    before = await ActivityService(ch, ApiSettings()).get(week)
    minute = int(now.replace(second=0, microsecond=0).timestamp())
    await ch.execute(
        "ALTER TABLE wiki_pages_per_minute DELETE WHERE toUnixTimestamp(minute) >= {m:UInt32} "
        "SETTINGS mutations_sync = 1",
        params={"m": minute},
    )
    missed = [
        Mismatch(minute=datetime.fromtimestamp(minute + 60 * i, UTC), lang="en", raw=0, rolled=0)
        for i in range(3)
    ]
    await repair(ch, missed, quiet=timedelta(0))
    after = await ActivityService(ch, ApiSettings()).get(week)
    assert after["pages_edited"] == before["pages_edited"] == 3


async def test_a_retry_cant_run_alongside_the_insert_it_retries(ch: ClickHouse) -> None:
    """What ingest's retry relies on: while an insert runs, its query id is taken; once it
    finishes the id is free again, and the same token makes the repeat a no-op."""
    await ch.execute("CREATE TABLE slow_sink (z UInt8) ENGINE = Null")
    await ch.execute(
        "CREATE MATERIALIZED VIEW slow_mv TO slow_sink AS SELECT sleep(1) AS z FROM wiki_edits"
    )
    batch = rows(10)
    first = asyncio.create_task(
        ch.insert("wiki_edits", batch, dedup_token="tok", query_id="ingest-tok")
    )
    await asyncio.sleep(0.3)
    with pytest.raises(ClickHouseError, match="QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING"):
        await ch.insert("wiki_edits", batch, dedup_token="tok", query_id="ingest-tok")
    await first
    await ch.insert("wiki_edits", batch, dedup_token="tok", query_id="ingest-tok")
    assert await scalar(ch, "SELECT count() FROM wiki_edits") == 10


async def test_repair_refuses_while_ingest_is_writing(ch: ClickHouse) -> None:
    await ch.insert("wiki_edits", rows(10), dedup_token="fresh")  # ingested just now
    minute = datetime.now(UTC).replace(second=0, microsecond=0)
    with pytest.raises(IngestRunning, match="still arriving"):
        await repair(ch, [Mismatch(minute=minute, lang="en", raw=10, rolled=0)])


async def test_repair_refuses_while_an_insert_is_still_writing_the_rollup(
    ch: ClickHouse,
) -> None:
    """Raw rows commit before the views run. An insert whose rows are already visible but
    whose view is still working must stop a repair: "no new rows" isn't enough."""
    old = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(hours=1)
    await ch.insert("wiki_edits", rows(5, start=old), dedup_token="old")
    await asyncio.sleep(0.1)
    await ch.execute("CREATE TABLE slow_sink (z UInt8) ENGINE = Null")
    await ch.execute(
        "CREATE MATERIALIZED VIEW slow_mv TO slow_sink AS SELECT sleep(2) AS z FROM wiki_edits"
    )
    slow = asyncio.create_task(
        ch.insert("wiki_edits", rows(5, start=old), dedup_token="slow", query_id="ingest-slow")
    )
    await asyncio.sleep(0.5)  # raw rows visible, view still sleeping
    mismatch = Mismatch(minute=old, lang="en", raw=5, rolled=0)
    with pytest.raises(IngestRunning, match="still running"):
        await repair(ch, [mismatch], quiet=timedelta(0))
    await slow


async def test_reconcile_finds_and_rebuilds_a_rollup_that_drifted(ch: ClickHouse) -> None:
    start = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(hours=1)
    await ch.insert("wiki_edits", rows(240, start=start, bot_every=3), dedup_token="a")
    await ch.insert("wiki_edits", rows(120, lang="pt", start=start), dedup_token="b")
    now = datetime.now(UTC)
    assert await find_mismatches(ch, now=now, settle=timedelta(0)) == []

    # Simulate the view's half of an insert never landing for one minute.
    lost = start + timedelta(minutes=2)
    await ch.execute(
        "ALTER TABLE wiki_edits_per_minute DELETE WHERE minute = fromUnixTimestamp({m:Int64}) "
        "AND lang = 'en' SETTINGS mutations_sync = 1",
        params={"m": int(lost.timestamp())},
    )
    found = await find_mismatches(ch, now=now, settle=timedelta(0))
    assert [(m.minute, m.lang, m.raw, m.rolled) for m in found] == [(lost, "en", 60, 0)]

    assert await repair(ch, found, quiet=timedelta(0)) == 1
    assert await find_mismatches(ch, now=now, settle=timedelta(0)) == []
    bots = await scalar(ch, "SELECT sum(bot_edits) FROM wiki_edits_per_minute")
    assert bots == await scalar(ch, "SELECT countIf(is_bot) FROM wiki_edits")


async def test_migrator_hands_the_freshness_view_to_its_definer() -> None:
    """Migration 0006 as production runs it: the real migrator, on a database at version 5.
    Then the view must still refresh, now with freshness_definer's rights. In CI it starts
    `demos` over; locally it leaves a stack's `demos` alone and skips."""
    admin = ClickHouse(clickhouse_test_settings().model_copy(update={"database": "demos"}))
    migrator = ClickHouse(user_settings("migrator", "CLICKHOUSE_MIGRATOR_PASSWORD"))
    try:
        exists = await admin.query(
            "SELECT count() AS n FROM system.databases WHERE name = 'demos'",
            settings={"database": "default"},
        )
        if int(exists.rows[0]["n"]):
            if not os.environ.get("CI"):
                pytest.skip("`demos` holds a local stack's data; this runs in CI")
            await admin.execute("DROP DATABASE demos SYNC", settings={"database": "default"})
        assert await migrate(admin, upto=5) == [1, 2, 3, 4, 5]
        assert await migrate(migrator) == [6]
        view = await admin.query(
            "SELECT definer FROM system.tables "
            "WHERE database = 'demos' AND name = 'freshness_samples_mv'"
        )
        assert view.rows == [{"definer": "freshness_definer"}]
        await admin.insert("wiki_edits", rows(5), dedup_token="definer")
        await admin.execute("SYSTEM REFRESH VIEW freshness_samples_mv")
        await admin.execute("SYSTEM WAIT VIEW freshness_samples_mv")
        sampled = await admin.query("SELECT count() AS n, max(age_s) AS age FROM freshness_samples")
        assert int(sampled.rows[0]["n"]) >= 1
        assert sampled.rows[0]["age"] is not None
    finally:
        await migrator.aclose()
        await admin.aclose()


@pytest.mark.usefixtures("demos_schema")
@pytest.mark.parametrize(
    ("user", "password_env", "sql"),
    [
        ("api", "CLICKHOUSE_API_PASSWORD", "INSERT INTO demos.ingest_gaps (reason) VALUES ('x')"),
        ("api", "CLICKHOUSE_API_PASSWORD", "CREATE TABLE demos.probe (x UInt8) ENGINE = Memory"),
        ("api", "CLICKHOUSE_API_PASSWORD", "SELECT 1 SETTINGS max_execution_time = 60"),
        (
            "ingest",
            "CLICKHOUSE_INGEST_PASSWORD",
            "CREATE TABLE demos.probe (x UInt8) ENGINE = Memory",
        ),
        ("ingest", "CLICKHOUSE_INGEST_PASSWORD", "ALTER TABLE demos.wiki_edits DROP COLUMN title"),
        ("ingest", "CLICKHOUSE_INGEST_PASSWORD", "DROP TABLE demos.wiki_edits"),
        # Counts come only from raw rows, through the views.
        (
            "ingest",
            "CLICKHOUSE_INGEST_PASSWORD",
            "INSERT INTO demos.wiki_edits_per_minute (lang, edits) VALUES ('en', 1000)",
        ),
        ("api", "CLICKHOUSE_API_PASSWORD", "SELECT count() FROM demos.schema_migrations"),
        # The Ops tab's numbers come from tables ingest can't write.
        (
            "ingest",
            "CLICKHOUSE_INGEST_PASSWORD",
            "INSERT INTO demos.aws_cost (amount) VALUES ('0')",
        ),
        (
            "ingest",
            "CLICKHOUSE_INGEST_PASSWORD",
            "INSERT INTO demos.freshness_samples (age_s) VALUES (1)",
        ),
        (
            "archiver",
            "CLICKHOUSE_ARCHIVER_PASSWORD",
            "INSERT INTO demos.freshness_samples (age_s) VALUES (1)",
        ),
    ],
)
async def test_application_users_cant_exceed_their_role(
    user: str, password_env: str, sql: str
) -> None:
    """The real users from clickhouse/users.d, not admin: the limits are what's tested."""
    client = ClickHouse(user_settings(user, password_env))
    try:
        with pytest.raises(ClickHouseError) as refused:
            await client.execute(sql)
        message = str(refused.value)
        assert any(
            code in message
            for code in ("ACCESS_DENIED", "READONLY", "SETTING_CONSTRAINT_VIOLATION")
        ), message
    finally:
        await client.aclose()


@pytest.mark.usefixtures("demos_schema")
async def test_migrator_can_run_migrations_and_release_its_lock() -> None:
    """As the real migrator user, against `demos`: everything is already applied there,
    so this checks the grants the run itself needs (ledger, lock), not the DDL."""
    client = ClickHouse(user_settings("migrator", "CLICKHOUSE_MIGRATOR_PASSWORD"))
    try:
        await migrate(client)
        await migrate(client)  # the lock was released, or this would be refused
    finally:
        await client.aclose()


@pytest.mark.usefixtures("demos_schema")
async def test_ingest_can_read_what_it_needs_to_resume() -> None:
    """As the real ingest user: every query resume makes must be allowed."""
    client = ClickHouse(user_settings("ingest", "CLICKHOUSE_INGEST_PASSWORD"))
    try:
        await wait_for_inflight_inserts(client, timeout_s=0)
        await load_resume_state(
            client,
            now=datetime.now(UTC),
            retention=timedelta(days=7),
            lookback=timedelta(hours=1),
            seam_ids=10,
        )
    finally:
        await client.aclose()


async def test_awkward_strings_round_trip_as_parameters(ch: ClickHouse) -> None:
    awkward = ["Rock 'n' roll", "C:\\temp", "tab\there", 'say "hi"', "cr\rlf\n", "nul\0"]
    result = await ch.query(
        "SELECT {titles:Array(String)} AS titles, {one:String} AS one;",
        params={"titles": awkward, "one": awkward[1]},
    )
    assert result.rows == [{"titles": awkward, "one": awkward[1]}]


async def test_an_insert_with_an_unknown_column_fails(ch: ClickHouse) -> None:
    row = rows(1)[0] | {"titel": "typo"}
    with pytest.raises(ClickHouseError):
        await ch.insert("wiki_edits", [row])
