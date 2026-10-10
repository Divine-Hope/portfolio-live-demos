"""Entry point: `livedemos-ingest`."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from prometheus_client import start_http_server

from livedemos.config import WIKIMEDIA_STREAM_URL, clickhouse_settings, ingest_settings
from livedemos.db.clickhouse import ClickHouse
from livedemos.ingest.consumer import Consumer
from livedemos.logs import setup_logging

log = logging.getLogger("livedemos.ingest")

_STOP_TIMEOUT_S = 6.0


async def run() -> int:
    setup_logging()
    settings = ingest_settings()
    if settings.stream_url == WIKIMEDIA_STREAM_URL and not settings.contact.strip():
        log.error(
            "INGEST_CONTACT is empty. Wikimedia's User-Agent policy needs a way to reach "
            "you (an email or a URL). Set it in .env, or use `make up-offline`."
        )
        return 2

    start_http_server(settings.metrics_port)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    async with ClickHouse(clickhouse_settings()) as ch:
        consumer = asyncio.create_task(Consumer(settings, ch).run(stop))
        stopper = asyncio.create_task(stop.wait())
        done, _ = await asyncio.wait({consumer, stopper}, return_when=asyncio.FIRST_COMPLETED)
        if consumer in done:
            stopper.cancel()
            if consumer.exception() is not None:
                log.error("consumer crashed", exc_info=consumer.exception())
                return 1
        else:
            # The consumer sees `stop` within a flush interval. Its last reconnect flush can
            # take 2 s more after a cancel, so 6 s here plus that stays inside the 15 s grace
            # compose gives ingest. Anything not committed is replayed on the next start.
            try:
                await asyncio.wait_for(consumer, timeout=_STOP_TIMEOUT_S)
            except TimeoutError:
                log.warning("consumer didn't stop in time; cancelled")
    log.info("stopped")
    return 0


def main() -> int:
    return asyncio.run(run())


if __name__ == "__main__":
    sys.exit(main())
