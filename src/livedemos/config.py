"""Runtime settings, read from environment variables (12-factor style)."""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Annotated, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from livedemos import __version__

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
_LANG = re.compile(r"[a-z]{2,3}")
_WIKI = re.compile(r"[a-z]{2,3}wiki")


class ClickHouseSettings(BaseSettings):
    """Connection to ClickHouse over its HTTP interface."""

    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_", extra="ignore")

    url: str = "http://localhost:8123"
    database: str = "demos"
    user: str = "default"
    password: SecretStr = SecretStr("")
    timeout_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 10.0


class IngestSettings(BaseSettings):
    """Settings for the stream consumer."""

    model_config = SettingsConfigDict(env_prefix="INGEST_", extra="ignore")

    stream_url: str = WIKIMEDIA_STREAM_URL
    # Wikimedia's User-Agent policy asks for a way to contact the operator.
    contact: str = ""
    # Which wikis to keep, and which change types count as edits. Comma lists in the
    # environment; everything else in the stream is skipped.
    wikis: Annotated[frozenset[str], NoDecode] = frozenset({"enwiki", "ptwiki", "dewiki"})
    types: Annotated[frozenset[str], NoDecode] = frozenset({"edit", "new"})
    flush_interval_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 1.0
    # Also bounds memory: the read queue holds at most twice this many parsed edits.
    flush_max_rows: Annotated[int, Field(ge=1, le=100_000)] = 5_000
    # recentchange never goes quiet; silence this long means a half-open socket.
    idle_timeout_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 30.0
    backoff_initial_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 1.0
    backoff_max_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 30.0
    # On start, ingest waits for any insert a killed predecessor left running, for as
    # long as it takes. It warns (and counts) every this many seconds of waiting.
    inflight_warn_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 60.0
    # On first boot (no bookmark yet) start this far back so charts are full.
    first_boot_lookback_s: Annotated[int, Field(ge=0)] = 3_600
    # How many of the most recently ingested event ids to remember, to drop the
    # events a resume sends twice. Roughly the last few minutes of traffic.
    seam_ids: Annotated[int, Field(ge=1, le=1_000_000)] = 20_000
    # How far back a bookmark can resume. Wikimedia kept about 11 days (measured
    # 2026-10-04), but raw rows, and the bookmarks on them, only live 7 days.
    retention_s: Annotated[int, Field(gt=0)] = 7 * 24 * 3_600
    metrics_port: Annotated[int, Field(ge=1, le=65_535)] = 9101

    @field_validator("wikis", "types", mode="before")
    @classmethod
    def _comma_list(cls, value: object) -> object:
        if isinstance(value, str):
            return frozenset(part.strip() for part in value.split(",") if part.strip())
        return value

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if self.backoff_initial_s > self.backoff_max_s:
            raise ValueError("backoff_initial_s must not exceed backoff_max_s")
        if not self.wikis or not self.types:
            raise ValueError("wikis and types must each name at least one value")
        if self.first_boot_lookback_s > self.retention_s:
            raise ValueError("first_boot_lookback_s can't exceed retention_s")
        if any(not _WIKI.fullmatch(w) for w in self.wikis):
            raise ValueError("wikis must look like 'enwiki'")
        return self

    @property
    def max_silence_s(self) -> float:
        """How long the consumer may go without a heartbeat before the health check fails:
        an idle socket's timeout and the longest backoff, twice over, for a slow connect
        or insert on top."""
        return 2 * (self.idle_timeout_s + self.backoff_max_s)

    @property
    def user_agent(self) -> str:
        contact = self.contact.strip() or "contact not set"
        return f"livedemos/{__version__} ({contact})"


class ApiSettings(BaseSettings):
    """Settings for the HTTP API."""

    model_config = SettingsConfigDict(env_prefix="API_", extra="ignore")

    langs: str = "en,pt,de"
    tick_interval_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 1.0
    # A snapshot older than this makes /readyz fail.
    max_snapshot_age_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 10.0
    # Newest event older than this flips the widget to "Paused". Sent in the payload,
    # so the widget and the API can't disagree about it.
    stale_after_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 60.0
    live_cache_max_age_s: Annotated[int, Field(ge=0)] = 1
    activity_cache_ttl_s: Annotated[int, Field(ge=1)] = 10
    # "Query it" admission: at most this many ClickHouse queries at once and this many
    # admitted (running or queued), and no request waits longer than activity_wait_s.
    # A failure is remembered for a few seconds, so a struggling ClickHouse isn't retried
    # by every visitor.
    activity_max_concurrency: Annotated[int, Field(ge=1)] = 2
    activity_max_pending: Annotated[int, Field(ge=1)] = 4
    activity_wait_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 5.0
    activity_error_cooldown_s: Annotated[float, Field(ge=0, allow_inf_nan=False)] = 5.0
    # Set in production: CloudFront adds this value as X-Origin-Verify, and requests
    # without it are refused. Empty (local) turns the check off.
    origin_secret: SecretStr = SecretStr("")
    # Set in production: where the last good live.json goes for CloudFront's fallback.
    # Empty (local) turns the writer off.
    snapshot_bucket: str = ""
    snapshot_interval_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 60.0
    # Fire drills only: every /v1/ request gets a 503, to prove the 5xx and synthetic
    # check alerts fire (docs/runbook.md). CloudFront then serves its fallback copy.
    drill_5xx: bool = False

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        langs = self.lang_list
        if not langs:
            raise ValueError("langs must name at least one language")
        if len(set(langs)) != len(langs):
            raise ValueError("langs must not repeat")
        if any(not _LANG.fullmatch(code) for code in langs):
            raise ValueError("langs must be lowercase language codes, like 'en'")
        if self.activity_max_pending < self.activity_max_concurrency:
            raise ValueError("activity_max_pending must be at least activity_max_concurrency")
        return self

    @property
    def lang_list(self) -> list[str]:
        return [code.strip() for code in self.langs.split(",") if code.strip()]


class OpsSettings(BaseSettings):
    """The Ops tab (api/ops.py, ops/report.py). Same API_ prefix as the rest of the API."""

    model_config = SettingsConfigDict(env_prefix="API_", extra="ignore")

    # Rebuilt at most this often, shared by every viewer.
    ops_cache_ttl_s: Annotated[int, Field(ge=1)] = 60
    # A failed build is remembered this long, so ClickHouse isn't asked by every viewer.
    ops_error_cooldown_s: Annotated[float, Field(ge=0, allow_inf_nan=False)] = 5.0
    # The freshness SLO (requirements N2): this share of minutes over this many days with
    # the newest event younger than the threshold. 99.9% is the floor; more is better.
    slo_target: Annotated[float, Field(ge=0.999, lt=1)] = 0.999
    slo_threshold_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 60.0
    slo_days: Annotated[int, Field(ge=1, le=89)] = 30


class ArchiveSettings(BaseSettings):
    """Settings for the hourly Parquet archive (ADR 0004)."""

    model_config = SettingsConfigDict(env_prefix="ARCHIVE_", extra="ignore")

    # Where hour files go, as ClickHouse sees it. Production: the archive bucket over
    # HTTPS, signed with the instance role. Locally: SeaweedFS, unsigned.
    url: str = "http://s3:8333/archive/wikipedia/edits"
    # Send no credentials. Only for the local S3 stand-in.
    nosign: bool = False
    interval_s: Annotated[float, Field(gt=0, allow_inf_nan=False)] = 300.0
    # An hour is archived once ingest has committed events this far past its end, so
    # late events and a replay after an outage have landed first.
    settle_s: Annotated[int, Field(ge=0)] = 300
    # How far back to check hours against their files: all of raw retention. Raw rows
    # expire by whole days, so every hour in the last 7 days still has all its rows.
    lookback_s: Annotated[int, Field(gt=0, le=7 * 24 * 3_600)] = 7 * 24 * 3_600
    metrics_port: Annotated[int, Field(ge=1, le=65_535)] = 9102
    # After migrating, refill the rollup from the archive if raw rows are gone
    # (rollup/restore.py). Off only to start a host while the archive can't be read.
    restore: bool = True
    # Production: archive only while this host holds this public IP (the Elastic IP), so
    # when the Auto Scaling Group overlaps two hosts only the live one writes (ADR 0010).
    # Empty (local): always archive.
    only_on_ip: str = ""

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if not re.fullmatch(r"https?://\S+[^/]", self.url):
            raise ValueError("url must be an http(s) URL without a trailing slash")
        return self


class CostSettings(BaseSettings):
    """The month's AWS cost for the Ops tab (ops/cost.py), fetched by the archive service:
    the one process that runs only on the live host."""

    model_config = SettingsConfigDict(env_prefix="ARCHIVE_", extra="ignore")

    # Production: resources with this tag, once a day, claiming each day under `cost_claims`
    # (s3://bucket/prefix) first so only one caller ever asks. Either empty (local): don't.
    cost_tag: str = ""
    cost_claims: str = ""

    @property
    def enabled(self) -> bool:
        return bool(self.cost_tag and self.cost_claims)


@lru_cache
def clickhouse_settings() -> ClickHouseSettings:
    return ClickHouseSettings()


@lru_cache
def ingest_settings() -> IngestSettings:
    return IngestSettings()


@lru_cache
def api_settings() -> ApiSettings:
    return ApiSettings()


@lru_cache
def archive_settings() -> ArchiveSettings:
    return ArchiveSettings()


@lru_cache
def ops_settings() -> OpsSettings:
    return OpsSettings()


@lru_cache
def cost_settings() -> CostSettings:
    return CostSettings()
