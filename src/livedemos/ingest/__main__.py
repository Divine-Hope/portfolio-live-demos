"""Entry point: `python -m livedemos.ingest`."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from prometheus_client import start_http_server

from livedemos.clickhouse import ClickHouse
from livedemos.config import WIKIMEDIA_STREAM_URL, clickhouse_settings, ingest_settings
from livedemos.ingest.consumer import Consumer
from livedemos.logs import setup_logging
from livedemos.migrate import migrate

log = logging.getLogger("livedemos.ingest")


async def main() -> int:
    setup_logging()
    settings = ingest_settings()
    if settings.stream_url == WIKIMEDIA_STREAM_URL and not settings.contact.strip():
        log.error(
            "INGEST_CONTACT is empty. Wikimedia's User-Agent policy needs a way to reach "
            "you (an email or a URL). Set it in .env, or use `make up-offline`."
        )
        return 2

    start_http_server(settings.metrics_port)
    ch = ClickHouse(clickhouse_settings())
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    try:
        await migrate(ch)
        consumer = asyncio.create_task(Consumer(settings, ch).run(stop))
        stopper = asyncio.create_task(stop.wait())
        done, _ = await asyncio.wait({consumer, stopper}, return_when=asyncio.FIRST_COMPLETED)
        if consumer in done and consumer.exception() is not None:
            log.error("consumer crashed", exc_info=consumer.exception())
            return 1
        # Anything not yet committed is replayed from the bookmark on the next start.
        consumer.cancel()
        stopper.cancel()
        await asyncio.gather(consumer, stopper, return_exceptions=True)
    finally:
        await ch.aclose()
    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
