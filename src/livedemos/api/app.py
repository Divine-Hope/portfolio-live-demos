"""HTTP API: `uvicorn livedemos.api.app:app`.

Routes
  GET /v1/wikipedia/live.json   widget payload, rebuilt every second, served from memory
  GET /v1/wikipedia/activity    "Query it": allowlisted ad hoc query with ClickHouse timing
  GET /v1/ops.json              the Ops tab: lag, bookmark, reconnects, SLO, gaps, cost
  GET /healthz                  process is up
  GET /readyz                   snapshot is fresh and ClickHouse answers
  GET /metrics                  Prometheus
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from livedemos import __version__
from livedemos.api import metrics
from livedemos.api.activity import ActivityService, BadRequest, Unavailable, parse_request
from livedemos.api.fallback import FallbackWriter
from livedemos.api.ops import OpsService, OpsUnavailable
from livedemos.api.snapshot import Snapshotter
from livedemos.clickhouse import ClickHouse
from livedemos.config import ApiSettings, ClickHouseSettings, api_settings, clickhouse_settings
from livedemos.logs import setup_logging

log = logging.getLogger("livedemos.api")


def create_app(
    settings: ApiSettings | None = None,
    ch_settings: ClickHouseSettings | None = None,
) -> FastAPI:
    settings = settings or api_settings()
    ch_settings = ch_settings or clickhouse_settings()
    langs = settings.lang_list

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Here, not at import: importing the app (tests, tools) mustn't replace the
        # caller's logging setup.
        setup_logging()
        ch = ClickHouse(ch_settings)
        snapshotter = Snapshotter(
            ch,
            langs=langs,
            interval_s=settings.tick_interval_s,
            stale_after_s=settings.stale_after_s,
        )
        stop = asyncio.Event()
        tasks = [asyncio.create_task(snapshotter.run(stop))]
        if settings.snapshot_bucket:
            import boto3  # only in production; credentials come from the instance role

            writer = FallbackWriter(
                snapshotter,
                boto3.client("s3"),
                bucket=settings.snapshot_bucket,
                interval_s=settings.snapshot_interval_s,
                max_snapshot_age_s=settings.max_snapshot_age_s,
                stale_after_s=settings.stale_after_s,
            )
            tasks.append(asyncio.create_task(writer.run(stop)))
        app.state.ch = ch
        app.state.snapshotter = snapshotter
        activity = ActivityService(
            ch,
            ttl_s=settings.activity_cache_ttl_s,
            max_concurrency=settings.activity_max_concurrency,
            max_pending=settings.activity_max_pending,
            wait_s=settings.activity_wait_s,
            error_cooldown_s=settings.activity_error_cooldown_s,
        )
        app.state.activity = activity
        app.state.ops = OpsService(
            ch,
            ttl_s=settings.ops_cache_ttl_s,
            threshold_s=settings.slo_threshold_s,
            target=settings.slo_target,
            days=settings.slo_days,
            error_cooldown_s=settings.activity_error_cooldown_s,
        )
        log.info("api started", extra={"version": __version__, "langs": langs})
        try:
            yield
        finally:
            stop.set()
            await activity.aclose()
            await asyncio.gather(*tasks, return_exceptions=True)
            await ch.aclose()

    app = FastAPI(
        title="livedemos",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )

    # Only CloudFront knows the secret. The security group already limits the port to
    # CloudFront's address ranges; this stops someone else's distribution pointing at us.
    # /healthz and /metrics stay open for the container healthcheck and a local scraper.
    open_paths = {"/healthz", "/metrics"}
    origin_secret = settings.origin_secret.encode()

    @app.middleware("http")
    async def require_origin_secret(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if origin_secret and request.url.path not in open_paths:
            sent = request.headers.get("x-origin-verify", "").encode()
            if not hmac.compare_digest(sent, origin_secret):
                return JSONResponse({"error": "forbidden"}, status_code=403)
        return await call_next(request)

    @app.middleware("http")
    async def drill(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if settings.drill_5xx and request.url.path.startswith("/v1/"):
            return JSONResponse({"error": "fire drill"}, status_code=503)
        return await call_next(request)

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
        except Unavailable as exc:
            log.warning("activity unavailable", extra={"reason": str(exc)}, exc_info=exc.__cause__)
            return JSONResponse(
                {"error": str(exc)},
                status_code=503,
                headers={"Cache-Control": "no-store", "Retry-After": "5"},
            )
        return JSONResponse(
            payload,
            headers={"Cache-Control": f"public, max-age={settings.activity_cache_ttl_s}"},
        )

    @app.get("/v1/ops.json")
    async def ops(request: Request) -> Response:
        try:
            body, age = await request.app.state.ops.get()
        except OpsUnavailable as exc:
            log.warning("ops unavailable", exc_info=exc.__cause__)
            return JSONResponse(
                {"error": str(exc)},
                status_code=503,
                headers={"Cache-Control": "no-store", "Retry-After": "10"},
            )
        return Response(
            content=body,
            media_type="application/json",
            # What's left of the build's minute, so a CDN copy is never older than that.
            headers={
                "Cache-Control": f"public, max-age={max(0, int(settings.ops_cache_ttl_s - age))}"
            },
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


app = create_app()
