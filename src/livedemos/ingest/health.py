"""Container health check: is the consumer loop alive, not just the process?

    python -m livedemos.ingest.health

Exits 0 if the loop's heartbeat is recent. The loop beats while reading, flushing and
waiting for ClickHouse, so a stuck loop (not a busy one) is what fails this check.
"""

from __future__ import annotations

import sys
import time
import urllib.request

from livedemos.config import ingest_settings

MAX_SILENCE_S = 120.0  # longer than the idle timeout plus the longest backoff
METRIC = "ingest_loop_heartbeat_timestamp_seconds"


def heartbeat_age(metrics_text: str, now: float) -> float | None:
    for line in metrics_text.splitlines():
        if line.startswith(METRIC + " "):
            return now - float(line.split()[1])
    return None


def main() -> int:
    url = f"http://127.0.0.1:{ingest_settings().metrics_port}/metrics"
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            text = response.read().decode()
    except OSError as exc:
        print(f"metrics unreachable: {exc}")
        return 1
    age = heartbeat_age(text, time.time())
    if age is None or age > MAX_SILENCE_S:
        print(f"consumer loop silent for {age}s")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
