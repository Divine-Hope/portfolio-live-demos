"""Prometheus metrics for the archive service."""

from prometheus_client import Counter, Gauge

HOURS = Counter(
    "archive_hours_total",
    "Hours handled, by result.",
    ["result"],  # written, empty, mismatch, error
)
ROWS = Counter("archive_rows_written_total", "Rows written to Parquet files.")
FAILED = Gauge(
    "archive_failed_hours",
    "Hours the last run couldn't archive, or whose file didn't match the raw rows.",
)
NEWEST_HOUR = Gauge(
    "archive_newest_hour_timestamp_seconds",
    "Start of the newest hour known to be in S3. Alert on time() minus this.",
)
LAST_RUN = Gauge(
    "archive_last_run_timestamp_seconds", "Wall-clock time the last run finished, ok or not."
)
