"""Prometheus metrics for the ingest service."""

from prometheus_client import Counter, Gauge, Histogram

EVENTS = Counter(
    "ingest_events_total",
    "Events read from the stream, by outcome.",
    ["outcome"],  # kept, duplicate, canary, other_wiki, other_type, malformed
)
ROWS_WRITTEN = Counter("ingest_rows_written_total", "Rows committed to ClickHouse.")
BATCHES = Counter("ingest_batches_total", "Batches flushed, by result.", ["result"])
BATCH_ROWS = Histogram(
    "ingest_batch_rows",
    "Rows per flushed batch.",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000),
)
INSERT_SECONDS = Histogram(
    "ingest_insert_seconds",
    "Time to insert one batch.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
RECONNECTS = Counter("ingest_reconnects_total", "Stream reconnects, by reason.", ["reason"])
CONNECTED = Gauge("ingest_connected", "1 while connected to the stream.")
LAST_EVENT_TS = Gauge(
    "ingest_last_event_timestamp_seconds", "Event time of the newest committed row."
)
LAG_SECONDS = Gauge(
    "ingest_lag_seconds", "Commit time minus event time for the newest committed row."
)
GAPS = Counter("ingest_gaps_recorded_total", "Gaps recorded because a resume wasn't possible.")
HEARTBEAT = Gauge(
    "ingest_loop_heartbeat_timestamp_seconds",
    "Last time the consumer loop made progress or was deliberately waiting.",
)
