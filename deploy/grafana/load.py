"""Load the dashboard and the alert rules into a Grafana stack.

    GRAFANA_URL=https://<stack>.grafana.net GRAFANA_TOKEN=<service account token> \\
        uv run python deploy/grafana/load.py

The token is a Grafana service account token with the Editor role (Administration > Users
and access > Service accounts); it isn't stored anywhere. Rules are upserted one by one, in
folder `livedemos`, group `livedemos`, and stay editable in the UI. The email contact point
and its notification route are set once by hand (docs/runbook.md, "Grafana Cloud").
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import httpx

HERE = Path(__file__).parent
FOLDER = "livedemos"


def main() -> None:
    client = httpx.Client(
        base_url=os.environ["GRAFANA_URL"],
        headers={
            "Authorization": f"Bearer {os.environ['GRAFANA_TOKEN']}",
            "X-Disable-Provenance": "true",
        },
        timeout=30,
    )
    sources = client.get("/api/datasources").raise_for_status().json()
    prom_uid = next(d["uid"] for d in sources if d["type"] == "prometheus" and "-prom" in d["name"])

    if client.get(f"/api/folders/{FOLDER}").status_code == 404:
        client.post("/api/folders", json={"uid": FOLDER, "title": FOLDER}).raise_for_status()

    dashboard = json.loads((HERE / "livedemos.json").read_text())
    client.post(
        "/api/dashboards/db",
        json={"dashboard": dashboard, "overwrite": True, "message": "deploy/grafana/load.py"},
    ).raise_for_status()
    print("dashboard loaded")

    group = json.loads((HERE / "alerts.json").read_text().replace("${prometheus}", prom_uid))
    for rule in group["rules"]:
        body = {**rule, "folderUID": FOLDER, "ruleGroup": group["title"], "orgID": 1}
        exists = client.get(f"/api/v1/provisioning/alert-rules/{rule['uid']}").status_code == 200
        if exists:
            response = client.put(f"/api/v1/provisioning/alert-rules/{rule['uid']}", json=body)
        else:
            response = client.post("/api/v1/provisioning/alert-rules", json=body)
        response.raise_for_status()
        print(f"rule {rule['uid']} loaded")


if __name__ == "__main__":
    main()
