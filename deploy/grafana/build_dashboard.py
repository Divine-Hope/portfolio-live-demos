"""Write deploy/grafana/livedemos.json, the pipeline dashboard.

    uv run python deploy/grafana/build_dashboard.py

Kept as code so every panel is reviewed like any other change. The JSON is committed too,
so it can be imported from scratch (Dashboards > New > Import) without running this.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROM = {"type": "prometheus", "uid": "${prometheus}"}
LOKI = {"type": "loki", "uid": "${loki}"}


def panel(
    title: str,
    exprs: list[tuple[str, str]],
    *,
    unit: str = "short",
    x: int = 0,
    y: int = 0,
    w: int = 12,
    h: int = 8,
    kind: str = "timeseries",
    desc: str = "",
) -> dict[str, Any]:
    return {
        "type": kind,
        "title": title,
        "description": desc,
        "datasource": PROM,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "targets": [
            {"refId": chr(65 + i), "expr": expr, "legendFormat": legend, "datasource": PROM}
            for i, (expr, legend) in enumerate(exprs)
        ],
    }


def quantiles(bucket: str) -> list[tuple[str, str]]:
    return [
        (f"histogram_quantile({q}, sum by (le) (rate({bucket}[5m])))", f"p{int(q * 100)}")
        for q in (0.5, 0.95)
    ]


PANELS = [
    panel(
        "Freshness: age of the newest event in the snapshot",
        [("max(api_last_event_age_seconds)", "age")],
        unit="s",
        x=0,
        y=0,
        desc="What the live page shows. Over 60 s, the widget says Paused.",
    ),
    panel(
        "Ingest lag: commit time minus event time",
        [("max(ingest_lag_seconds)", "lag")],
        unit="s",
        x=12,
        y=0,
    ),
    panel(
        "Events by outcome (per second)",
        [("sum by (outcome) (rate(ingest_events_total[5m]))", "{{outcome}}")],
        unit="ops",
        x=0,
        y=8,
    ),
    panel(
        "Insert latency",
        quantiles("ingest_insert_seconds_bucket"),
        unit="s",
        x=12,
        y=8,
    ),
    panel(
        "Stream reconnects by reason (per hour)",
        [("sum by (reason) (increase(ingest_reconnects_total[1h]))", "{{reason}}")],
        x=0,
        y=16,
    ),
    panel(
        "Snapshot build time",
        quantiles("api_snapshot_build_seconds_bucket"),
        unit="s",
        x=12,
        y=16,
    ),
    panel(
        "API latency by route (p95)",
        [
            (
                "histogram_quantile(0.95,"
                " sum by (le, route) (rate(api_request_seconds_bucket[5m])))",
                "{{route}}",
            )
        ],
        unit="s",
        x=0,
        y=24,
        desc="At the origin. CloudFront serves most viewers from its cache.",
    ),
    panel(
        "API responses by status (per second)",
        [("sum by (status) (rate(api_requests_total[5m]))", "{{status}}")],
        unit="reqps",
        x=12,
        y=24,
    ),
    panel(
        "Memory per container, and its limit",
        [
            ("max by (service) (container_memory_working_set_bytes)", "{{service}}"),
            ("max by (service) (container_spec_memory_limit_bytes)", "{{service}} limit"),
        ],
        unit="bytes",
        x=0,
        y=32,
    ),
    panel(
        "Host memory, swap and OOM kills",
        [
            ("node_memory_MemAvailable_bytes", "available"),
            ("node_memory_SwapTotal_bytes - node_memory_SwapFree_bytes", "swap used"),
            ("increase(node_vmstat_oom_kill[1h]) * 1e6", "OOM kills in the last hour (x 1e6)"),
        ],
        unit="bytes",
        x=12,
        y=32,
    ),
    panel(
        "Disk used (the root disk, where ClickHouse keeps its data)",
        [
            (
                "max(ClickHouseAsyncMetrics_FilesystemMainPathUsedBytes)"
                " / max(ClickHouseAsyncMetrics_FilesystemMainPathTotalBytes)",
                "/",
            )
        ],
        unit="percentunit",
        x=0,
        y=40,
    ),
    panel(
        "Archive: hours behind, and age of the newest archived hour",
        [
            ("max(archive_hours_behind)", "hours behind"),
            ("time() - max(archive_newest_hour_timestamp_seconds)", "newest hour age (s)"),
        ],
        x=12,
        y=40,
    ),
    {
        "type": "logs",
        "title": "Warnings and errors",
        "datasource": LOKI,
        "gridPos": {"x": 0, "y": 48, "w": 24, "h": 10},
        "options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending"},
        "targets": [
            {
                "refId": "A",
                "datasource": LOKI,
                "expr": '{host="livedemos", level=~"warning|error|critical"}',
            }
        ],
    },
]

DASHBOARD = {
    "uid": "livedemos-pipeline",
    "title": "Live demos: pipeline",
    "tags": ["livedemos"],
    "timezone": "utc",
    "refresh": "1m",
    "time": {"from": "now-6h", "to": "now"},
    "schemaVersion": 39,
    "templating": {
        "list": [
            {"name": "prometheus", "type": "datasource", "query": "prometheus", "label": "Metrics"},
            {"name": "loki", "type": "datasource", "query": "loki", "label": "Logs"},
        ]
    },
    "panels": [{**p, "id": i + 1} for i, p in enumerate(PANELS)],
}

if __name__ == "__main__":
    out = Path(__file__).with_name("livedemos.json")
    out.write_text(json.dumps(DASHBOARD, indent=2) + "\n")
    print(f"wrote {out}")
