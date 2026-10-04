"""Prometheus metrics for the API."""

from prometheus_client import Counter, Gauge, Histogram

SNAPSHOTS = Counter("api_snapshots_total", "Snapshot builds, by result.", ["result"])
SNAPSHOT_SECONDS = Histogram(
    "api_snapshot_build_seconds",
    "Time to build one snapshot (all queries).",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)
LAST_EVENT_AGE = Gauge(
    "api_last_event_age_seconds", "Age of the newest event when the snapshot was built."
)
SNAPSHOT_AGE = Gauge("api_snapshot_age_seconds", "Seconds since the served snapshot was built.")
REQUESTS = Counter("api_requests_total", "HTTP requests, by route and status.", ["route", "status"])
REQUEST_SECONDS = Histogram(
    "api_request_seconds",
    "HTTP request latency, by route.",
    ["route"],
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)
ACTIVITY_QUERIES = Counter("api_activity_queries_total", "Query-it requests, by cache.", ["cache"])
