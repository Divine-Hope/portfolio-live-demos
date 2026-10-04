"""HTTP API: `uvicorn livedemos.api.app:app`.

Routes
  GET /v1/wikipedia/live.json   widget payload, rebuilt every second, served from memory
  GET /v1/wikipedia/activity    "Query it": allowlisted ad hoc query with ClickHouse timing
  GET /healthz                  process is up
  GET /readyz                   snapshot is fresh and ClickHouse answers
  GET /metrics                  Prometheus
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from livedemos import __version__
from livedemos.api import metrics
from livedemos.api.activity import ActivityService, BadRequest, parse_request
from livedemos.api.snapshot import Snapshotter
from livedemos.clickhouse import ClickHouse, ClickHouseError
from livedemos.config import ApiSettings, ClickHouseSettings, api_settings, clickhouse_settings
from livedemos.logs import setup_logging

log = logging.getLogger("livedemos.api")


def create_app(
    settings: ApiSettings | None = None,
    ch_settings: ClickHouseSettings | None = None,
) -> FastAPI:
    settings = settings or api_settings()
    ch_settings = ch_settings or clickhouse_settings()
    langs = [code.strip() for code in settings.langs.split(",") if code.strip()]

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        ch = ClickHouse(ch_settings)
        snapshotter = Snapshotter(
            ch,
            langs=langs,
            interval_s=settings.tick_interval_s,
            stale_after_s=settings.stale_after_s,
        )
        stop = asyncio.Event()
        task = asyncio.create_task(snapshotter.run(stop))
        app.state.ch = ch
        app.state.snapshotter = snapshotter
        app.state.activity = ActivityService(ch, ttl_s=settings.activity_cache_ttl_s)
        log.info("api started", extra={"version": __version__, "langs": langs})
        try:
            yield
        finally:
            stop.set()
            await asyncio.gather(task, return_exceptions=True)
            await ch.aclose()

    app = FastAPI(
        title="livedemos",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )

    @app.middleware("http")
    async def observe(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        started = time.perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        name = getattr(route, "path", "unmatched")
        metrics.REQUESTS.labels(route=name, status=str(response.status_code)).inc()
        metrics.REQUEST_SECONDS.labels(route=name).observe(time.perf_counter() - started)
        # Public, read-only data: allow any origin, on every response. Sending it only
        # when a request carries Origin would let a CDN cache a copy without it.
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Expose-Headers"] = "Age, Date, X-Cache-Status"
        return response

    @app.get("/v1/wikipedia/live.json")
    async def live(request: Request) -> Response:
        snapshot = request.app.state.snapshotter.latest
        age = None if snapshot is None else time.monotonic() - snapshot.built_at
        if snapshot is None or (age is not None and age > settings.max_snapshot_age_s):
            # No snapshot yet, or we can't build new ones (ClickHouse down). Serving the
            # old one would look live forever. A 503 makes CloudFront fail over to the
            # last snapshot in S3, whose own timestamp tells the widget it's stale.
            return JSONResponse(
                {"error": "warming up" if snapshot is None else "snapshot is stale"},
                status_code=503,
                headers={"Retry-After": "1", "Cache-Control": "no-store"},
            )
        metrics.SNAPSHOT_AGE.set(age or 0.0)
        return Response(
            content=snapshot.body,
            media_type="application/json",
            headers={"Cache-Control": f"public, max-age={settings.live_cache_max_age_s}"},
        )

    @app.get("/v1/wikipedia/activity")
    async def activity(
        request: Request, lang: str | None = None, window: str | None = None
    ) -> Response:
        try:
            req = parse_request(lang, window, allowed=langs)
        except BadRequest as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        try:
            payload = await request.app.state.activity.get(req)
        except ClickHouseError:
            log.exception("activity query failed")
            return JSONResponse(
                {"error": "query failed"}, status_code=503, headers={"Cache-Control": "no-store"}
            )
        return JSONResponse(
            payload,
            headers={"Cache-Control": f"public, max-age={settings.activity_cache_ttl_s}"},
        )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    @app.get("/readyz")
    async def readyz(request: Request) -> Response:
        snapshot = request.app.state.snapshotter.latest
        problems: list[str] = []
        if snapshot is None:
            problems.append("no snapshot yet")
        elif time.monotonic() - snapshot.built_at > settings.max_snapshot_age_s:
            problems.append("snapshot is old")
        if not await request.app.state.ch.ping():
            problems.append("clickhouse unreachable")
        if problems:
            return JSONResponse({"status": "not ready", "problems": problems}, status_code=503)
        return JSONResponse({"status": "ready"})

    @app.get("/metrics")
    async def prometheus() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


setup_logging()
app = create_app()
