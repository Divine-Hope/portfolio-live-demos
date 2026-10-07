"""The committed dashboard JSON is what its generator writes, so neither drifts."""

import json
import runpy
from pathlib import Path

GRAFANA = Path(__file__).parents[2] / "deploy" / "grafana"


def test_the_dashboard_json_is_up_to_date() -> None:
    built = runpy.run_path(str(GRAFANA / "build_dashboard.py"))["DASHBOARD"]
    committed = json.loads((GRAFANA / "livedemos.json").read_text())
    assert committed == built, "run: uv run python deploy/grafana/build_dashboard.py"


def test_no_panel_groups_by_a_high_cardinality_label() -> None:
    text = (GRAFANA / "livedemos.json").read_text()
    for label in ("title", "event_id", "ip", "url"):
        assert f"by ({label}" not in text
