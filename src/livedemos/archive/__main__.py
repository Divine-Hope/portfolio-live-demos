"""Entry point: `python -m livedemos.archive`.

The service also fetches the month's AWS cost once a day (ops/cost.py), when
ARCHIVE_COST_TAG is set: it's the one process that runs only on the live host.

python -m livedemos.archive                        # the service: archive due hours, repeat
python -m livedemos.archive --once                 # one pass, then exit
python -m livedemos.archive --hour 2026-10-06T09   # rewrite that hour's file, even if it exists
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
import time
from datetime import UTC, datetime

import httpx
from prometheus_client import start_http_server

from livedemos.archive import metrics
from livedemos.archive.job import HOUR_S, Archiver, NothingToArchive
from livedemos.clickhouse import ClickHouse, ClickHouseError
from livedemos.config import archive_settings, clickhouse_settings
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
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval_s)


async def main(args: argparse.Namespace) -> int:
    setup_logging()
    settings = archive_settings()
    ch = ClickHouse(clickhouse_settings())
    archiver = Archiver(settings, ch)
    try:
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
        if settings.cost_tag:
            import boto3  # only in production; credentials come from the instance role

            cost = CostFetcher(
                ch,
                tag=settings.cost_tag,
                # Cost Explorer has one endpoint, in us-east-1.
                client_factory=lambda: boto3.client("ce", region_name="us-east-1"),
            )
        await _serve(archiver, cost, settings.interval_s, settings.only_on_ip)
        return 0
    finally:
        await ch.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hourly Parquet archive of raw edits.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="one pass, then exit")
    mode.add_argument("--hour", type=_hour, help="rewrite one hour (UTC), e.g. 2026-10-06T09")
    sys.exit(asyncio.run(main(parser.parse_args())))
