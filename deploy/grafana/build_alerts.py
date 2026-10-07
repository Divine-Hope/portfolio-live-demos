"""Write deploy/grafana/alerts.json: the alert rules, as one Grafana rule group.

    uv run python deploy/grafana/build_alerts.py

Each rule is a PromQL value and a threshold. Every rule carries `project=livedemos`, which
the notification policy routes to the email contact point, and links its runbook entry.
docs/runbook.md ("Grafana Cloud") says how to load the group into the stack.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROM_UID = "${prometheus}"  # replaced with the stack's Prometheus data source uid on load
RUNBOOK = "https://github.com/Divine-Hope/portfolio-live-demos/blob/main/docs/runbook.md"


def slug(heading: str) -> str:
    """GitHub's anchor for a markdown heading: each alert has one in docs/runbook.md."""
    kept = "".join(c for c in heading.lower() if c.isalnum() or c in " -")
    return kept.replace(" ", "-")


def rule(
    uid: str,
    title: str,
    expr: str,
    op: str,
    threshold: float,
    *,
    pending: str,
    summary: str,
    no_data: str = "OK",
) -> dict[str, Any]:
    return {
        "uid": uid,
        "title": title,
        "condition": "B",
        "for": pending,
        "noDataState": no_data,
        "execErrState": "Error",
        "labels": {"project": "livedemos"},
        "annotations": {"summary": summary, "runbook_url": f"{RUNBOOK}#{slug(title)}"},
        "data": [
            {
                "refId": "A",
                "datasourceUid": PROM_UID,
                "relativeTimeRange": {"from": 600, "to": 0},
                "model": {"refId": "A", "expr": expr, "instant": True, "range": False},
            },
            {
                "refId": "B",
                "datasourceUid": "__expr__",
                "model": {
                    "refId": "B",
                    "type": "threshold",
                    "expression": "A",
                    "conditions": [{"evaluator": {"type": op, "params": [threshold]}}],
                },
            },
        ],
    }


RULES = [
    rule(
        "ld-ingest-stalled",
        "Ingest stalled",
        # Age at scrape time, not now: scrapes are a minute apart, so time() minus the
        # value would read up to 60 s even when nothing is wrong.
        "max(timestamp(ingest_last_event_timestamp_seconds) - ingest_last_event_timestamp_seconds)",
        "gt",
        60,
        pending="5m",
        no_data="Alerting",
        summary="The newest committed event is over 60 s old. The live page says Paused.",
    ),
    rule(
        "ld-target-down",
        "No data from a service",
        'min by (job) (up{job=~"ingest|api|archive|clickhouse"})',
        "lt",
        1,
        pending="5m",
        no_data="Alerting",
        summary="A service isn't answering its metrics scrape, or nothing arrives at all "
        "(Alloy or the host is down).",
    ),
    rule(
        "ld-container-memory",
        "Container near its memory limit",
        "max by (service) (container_memory_working_set_bytes / container_spec_memory_limit_bytes)",
        "gt",
        0.85,
        pending="5m",
        summary="A container has used over 85% of its memory limit for 5 minutes.",
    ),
    rule(
        "ld-oom-kill",
        "OOM kill",
        "increase(node_vmstat_oom_kill[10m])",
        "gt",
        0,
        pending="0s",
        summary="The kernel killed a process for memory in the last 10 minutes.",
    ),
    rule(
        "ld-swap-in",
        "Host swapping in",
        "rate(node_vmstat_pswpin[5m])",
        "gt",
        100,
        pending="10m",
        summary="Over 100 pages a second read back from swap for 10 minutes: the host is "
        "short of memory.",
    ),
    rule(
        "ld-disk",
        "Disk over 80%",
        "max(ClickHouseAsyncMetrics_FilesystemMainPathUsedBytes)"
        " / max(ClickHouseAsyncMetrics_FilesystemMainPathTotalBytes)",
        "gt",
        0.8,
        pending="5m",
        summary="The root disk, which holds ClickHouse's data, is over 80% full.",
    ),
    rule(
        "ld-api-5xx",
        "API 5xx over 1%",
        'sum(rate(api_requests_total{status=~"5.."}[5m]))'
        " / clamp_min(sum(rate(api_requests_total[5m])), 1e-9)",
        "gt",
        0.01,
        pending="10m",
        summary="Over 1% of API responses at the origin are errors. CloudFront is serving "
        "its fallback copy.",
    ),
    rule(
        "ld-archive-stale",
        "Archive behind",
        "time() - max(archive_newest_hour_timestamp_seconds)",
        "gt",
        3 * 3600,
        pending="15m",
        summary="No new hour archived for three hours.",
    ),
    rule(
        "ld-live-json-check",
        "live.json check failing",
        'min(probe_success{job="livedemos-live-json"})',
        "lt",
        1,
        pending="3m",
        no_data="Alerting",
        summary="The outside-in check of the public live.json is failing: an error, or "
        "the payload isn't live.",
    ),
]

GROUP = {"title": "livedemos", "interval": "1m", "rules": RULES}

if __name__ == "__main__":
    out = Path(__file__).with_name("alerts.json")
    out.write_text(json.dumps(GROUP, indent=2) + "\n")
    print(f"wrote {out}")
