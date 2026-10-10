"""Entry point: `livedemos-archive`.

The service also fetches the month's AWS cost once a day (ops/cost.py), when
ARCHIVE_COST_TAG and ARCHIVE_COST_CLAIMS are set: it's the one process that runs only on
the live host.

livedemos-archive                        # the service: archive due hours, repeat
livedemos-archive --once                 # one pass, then exit
livedemos-archive --hour 2026-10-06T09   # rewrite that hour's file, even if it exists
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from datetime import UTC, datetime

import httpx
from prometheus_client import start_http_server

from livedemos.aio import sleep_unless_stopped
from livedemos.archive import metrics
from livedemos.archive.job import Archiver, NothingToArchive
from livedemos.archive.s3 import HOUR_S
from livedemos.config import archive_settings, clickhouse_settings, cost_settings
from livedemos.db.clickhouse import ClickHouse, ClickHouseError
from livedemos.logs import setup_logging
from livedemos.ops.cost import CostFetcher

log = logging.getLogger("livedemos.archive")


def _hour(value: str) -> int:
    start = datetime.strptime(value, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    return int(start.timestamp()) // HOUR_S * HOUR_S


async def public_ip() -> str | None:
    """This host's public IP, from the instance metadata service (IMDSv2)."""
    imds = "http://169.254.169.254/latest"
    try:
        async with httpx.AsyncClient(timeout=2) as client:
            token = await client.put(
                f"{imds}/api/token", headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}
            )
            token.raise_for_status()
            ip = await client.get(
                f"{imds}/meta-data/public-ipv4", headers={"X-aws-ec2-metadata-token": token.text}
            )
            ip.raise_for_status()
            return ip.text.strip()
    except httpx.HTTPError:
        return None


async def _serve(
    archiver: Archiver, cost: CostFetcher | None, interval_s: float, only_on_ip: str
) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    was_live: bool | None = None
    while not stop.is_set():
        live = not only_on_ip or await public_ip() == only_on_ip
        if live != was_live:
            log.info("archiving" if live else "not the live host; not archiving")
            was_live = live
        try:
            if live:
                await archiver.run_once(datetime.now(UTC))
        except ClickHouseError:  # couldn't even plan; try again next time
            log.exception("archive run failed")
        if live and cost is not None:
            await cost.refresh_if_due(datetime.now(UTC))
        metrics.LAST_RUN.set(time.time())
        await sleep_unless_stopped(stop, interval_s)


async def run(args: argparse.Namespace) -> int:
    settings = archive_settings()
    async with ClickHouse(clickhouse_settings()) as ch:
        archiver = Archiver(settings, ch)
        if args.hour is not None:
            try:
                outcome = await archiver.archive_hour(args.hour, manual=True)
            except NothingToArchive as exc:
                log.error("not rewritten", extra={"reason": str(exc)})
                return 2
            log.info("hour rewritten", extra={"result": outcome.result, "rows": outcome.rows})
            return 0 if outcome.result == "written" else 1
        if args.once:
            results = await archiver.run_once(datetime.now(UTC))
            return 0 if all(r.result == "written" for r in results) else 1
        start_http_server(settings.metrics_port)
        cost = None
        costs = cost_settings()
        if costs.enabled:
            import boto3  # only in production; credentials come from the instance role

            cost = CostFetcher(
                ch,
                tag=costs.cost_tag,
                claims=costs.cost_claims,
                # Cost Explorer has one endpoint, in us-east-1.
                ce_factory=lambda: boto3.client("ce", region_name="us-east-1"),
                s3_factory=lambda: boto3.client("s3"),
                host=settings.only_on_ip,
            )
        await _serve(archiver, cost, settings.interval_s, settings.only_on_ip)
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Hourly Parquet archive of raw edits.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="one pass, then exit")
    mode.add_argument("--hour", type=_hour, help="rewrite one hour (UTC), e.g. 2026-10-06T09")
    args = parser.parse_args()
    setup_logging()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
