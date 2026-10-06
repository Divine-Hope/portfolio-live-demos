"""Prometheus metrics for the archive service."""

from prometheus_client import Counter, Gauge

HOURS = Counter(
    "archive_hours_total",
    "Hours written, by result.",
    ["result"],  # written, mismatch, error
)
ROWS = Counter("archive_rows_written_total", "Rows written to Parquet files.")
BEHIND = Gauge(
    "archive_hours_behind",
    "Hours whose file holds fewer rows than ClickHouse, or can't be read, after the last run.",
)
OLDEST_BEHIND = Gauge(
    "archive_oldest_behind_hour_timestamp_seconds",
    "Start of the oldest such hour, or 0. Raw rows expire after 7 days: alert well before.",
)
NEWEST_HOUR = Gauge(
    "archive_newest_hour_timestamp_seconds",
    "Start of the newest hour with a file. Alert on time() minus this.",
)
LAST_SUCCESS = Gauge(
    "archive_last_success_timestamp_seconds",
    "Wall-clock time of the last run that left no hour behind.",
)
LAST_RUN = Gauge(
    "archive_last_run_timestamp_seconds", "Wall-clock time the last run finished, ok or not."
)
