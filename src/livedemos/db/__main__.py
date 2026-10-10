"""`livedemos-migrate`: apply pending migrations, then restore a host that has lost its
data from the archive (rollup/restore.py). Runs as the migrator, before anything else."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import UTC, datetime

from livedemos.config import archive_settings, clickhouse_settings
from livedemos.db.clickhouse import ClickHouse
from livedemos.db.migrate import migrate
from livedemos.logs import setup_logging
from livedemos.rollup.lock import LockHeld, unlock
from livedemos.rollup.maintenance import IngestRunning
from livedemos.rollup.rebuild import ArchiveIncomplete
from livedemos.rollup.restore import RestoreFailed, restore

log = logging.getLogger("livedemos.db")


async def run(*, unlock_only: bool) -> int:
    async with ClickHouse(clickhouse_settings()) as ch:
        if unlock_only:
            await unlock(ch)
            log.info("lock removed")
            return 0
        await migrate(ch)
        settings = archive_settings()
        if not settings.restore:
            log.warning("restoring from the archive is off (ARCHIVE_RESTORE=false)")
            return 0
        try:
            done = await restore(ch, settings, now=datetime.now(UTC))
        except (RestoreFailed, ArchiveIncomplete, IngestRunning, LockHeld) as exc:
            # Nothing else starts until it's fixed (docs/runbook.md).
            log.error("restore refused", extra={"reason": str(exc)})
            return 1
        if done.rollup_hours or done.raw_hours or done.pages_hours:
            log.info(
                "restored from the archive",
                extra={
                    "rollup_hours": len(done.rollup_hours),
                    "raw_hours": len(done.raw_hours),
                    "raw_rows": done.raw_rows,
                    "pages_hours": len(done.pages_hours),
                },
            )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply pending migrations, then restore.")
    parser.add_argument("--unlock", action="store_true", help="remove a crashed run's lock")
    args = parser.parse_args()
    setup_logging()
    sys.exit(asyncio.run(run(unlock_only=args.unlock)))


if __name__ == "__main__":
    main()
