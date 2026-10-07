import subprocess
import sys
import time
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from livedemos.api.app import create_app
from livedemos.api.snapshot import Snapshot
from livedemos.config import ApiSettings, ClickHouseSettings


@pytest.fixture
def client() -> Iterator[TestClient]:
    # Point at a closed port and tick rarely: these tests drive the snapshot by hand.
    app = create_app(
        ApiSettings(tick_interval_s=3_600),
        ClickHouseSettings(url="http://127.0.0.1:9", timeout_s=0.2),
    )
    with TestClient(app) as test_client:
        yield test_client


def test_live_is_503_until_the_first_snapshot(client: TestClient) -> None:
    client.app.state.snapshotter.latest = None  # type: ignore[attr-defined]
    response = client.get("/v1/wikipedia/live.json")
    assert response.status_code == 503
    assert response.headers["cache-control"] == "no-store"


def test_live_serves_the_snapshot_bytes_with_a_short_cache(client: TestClient) -> None:
    body = b'{"dataset":"wikipedia","status":"live"}'
    client.app.state.snapshotter.latest = Snapshot(  # type: ignore[attr-defined]
        body=body, built_at=time.monotonic(), last_event_age_s=1.0
    )
    response = client.get("/v1/wikipedia/live.json")  # no Origin header, like a CDN probe
    assert response.status_code == 200
    assert response.content == body
    assert response.headers["cache-control"] == "public, max-age=1"
    # Sent on every response, so a cached copy works for cross-origin widgets too.
    assert response.headers["access-control-allow-origin"] == "*"
    assert "Date" in response.headers["access-control-expose-headers"]


def test_live_refuses_to_serve_a_stale_snapshot(client: TestClient) -> None:
    # The tick loop stopped (ClickHouse down): serving the old bytes would look live forever.
    client.app.state.snapshotter.latest = Snapshot(  # type: ignore[attr-defined]
        body=b"{}", built_at=time.monotonic() - 60, last_event_age_s=1.0
    )
    response = client.get("/v1/wikipedia/live.json")
    assert response.status_code == 503
    assert response.json()["error"] == "snapshot is stale"


def test_activity_rejects_unknown_parameters(client: TestClient) -> None:
    response = client.get("/v1/wikipedia/activity", params={"lang": "fr"})
    assert response.status_code == 400
    assert "lang must be" in response.json()["error"]


def test_readyz_fails_without_clickhouse(client: TestClient) -> None:
    response = client.get("/readyz")
    assert response.status_code == 503
    assert "clickhouse unreachable" in response.json()["problems"]


def test_health_and_metrics(client: TestClient) -> None:
    assert client.get("/healthz").json()["status"] == "ok"
    metrics = client.get("/metrics").text
    assert "api_requests_total" in metrics


@pytest.fixture
def guarded_client() -> Iterator[TestClient]:
    app = create_app(
        ApiSettings(tick_interval_s=3_600, origin_secret="s3cret"),
        ClickHouseSettings(url="http://127.0.0.1:9", timeout_s=0.2),
    )
    with TestClient(app) as test_client:
        yield test_client


def test_origin_secret_is_required_when_configured(guarded_client: TestClient) -> None:
    assert guarded_client.get("/v1/wikipedia/live.json").status_code == 403
    wrong = guarded_client.get("/v1/wikipedia/live.json", headers={"X-Origin-Verify": "nope"})
    assert wrong.status_code == 403
    right = guarded_client.get("/v1/wikipedia/live.json", headers={"X-Origin-Verify": "s3cret"})
    assert right.status_code == 503  # past the check; no snapshot yet in this test


def test_the_fire_drill_switch_fails_every_v1_request_and_counts_it() -> None:
    app = create_app(
        ApiSettings(tick_interval_s=3_600, drill_5xx=True),
        ClickHouseSettings(url="http://127.0.0.1:9", timeout_s=0.2),
    )
    with TestClient(app) as client:
        assert client.get("/v1/wikipedia/live.json").status_code == 503
        assert client.get("/healthz").status_code == 200
        metrics_text = client.get("/metrics").text
    assert 'api_requests_total{route="unmatched",status="503"}' in metrics_text


def test_health_and_metrics_stay_open_for_the_host(guarded_client: TestClient) -> None:
    assert guarded_client.get("/healthz").status_code == 200
    assert guarded_client.get("/metrics").status_code == 200


def test_no_secret_configured_means_no_check(client: TestClient) -> None:
    assert client.get("/v1/wikipedia/activity", params={"lang": "fr"}).status_code == 400


def test_activity_is_503_with_retry_after_when_clickhouse_is_down(client: TestClient) -> None:
    response = client.get("/v1/wikipedia/activity", params={"lang": "en"})
    assert response.status_code == 503
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["retry-after"] == "5"


def test_importing_the_app_leaves_logging_alone() -> None:
    # In a fresh interpreter: other tests start the app, which does configure logging.
    code = "import logging; import livedemos.api.app; print(len(logging.getLogger().handlers))"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "0"
