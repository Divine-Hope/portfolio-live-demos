"""Runtime settings, read from environment variables (12-factor style)."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

from livedemos import __version__

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


class ClickHouseSettings(BaseSettings):
    """Connection to ClickHouse over its HTTP interface."""

    model_config = SettingsConfigDict(env_prefix="CLICKHOUSE_", extra="ignore")

    url: str = "http://localhost:8123"
    database: str = "demos"
    user: str = "default"
    password: str = ""
    timeout_s: float = 10.0


class IngestSettings(BaseSettings):
    """Settings for the stream consumer."""

    model_config = SettingsConfigDict(env_prefix="INGEST_", extra="ignore")

    stream_url: str = WIKIMEDIA_STREAM_URL
    # Wikimedia's User-Agent policy asks for a way to contact the operator.
    contact: str = ""
    # Which wikis to keep. Everything else in the stream is skipped.
    wikis: str = "enwiki,ptwiki,dewiki"
    # Which change types count as edits.
    types: str = "edit,new"
    flush_interval_s: float = 1.0
    flush_max_rows: int = 5_000
    # recentchange never goes quiet; silence this long means a half-open socket.
    idle_timeout_s: float = 30.0
    backoff_initial_s: float = 1.0
    backoff_max_s: float = 30.0
    # On first boot (no bookmark yet) start this far back so charts are full.
    first_boot_lookback_s: int = 3_600
    # How many of the most recently ingested event ids to remember, to drop the
    # events a resume sends twice. Roughly the last few minutes of traffic.
    seam_ids: int = 20_000
    # How far back a bookmark can resume. Wikimedia kept about 11 days (measured
    # 2026-10-04), but raw rows, and the bookmarks on them, only live 7 days.
    retention_s: int = 7 * 24 * 3_600
    metrics_port: int = 9101

    @property
    def wiki_set(self) -> frozenset[str]:
        return frozenset(w.strip() for w in self.wikis.split(",") if w.strip())

    @property
    def type_set(self) -> frozenset[str]:
        return frozenset(t.strip() for t in self.types.split(",") if t.strip())

    @property
    def user_agent(self) -> str:
        contact = self.contact.strip() or "contact not set"
        return f"livedemos/{__version__} ({contact})"


class ApiSettings(BaseSettings):
    """Settings for the HTTP API."""

    model_config = SettingsConfigDict(env_prefix="API_", extra="ignore")

    langs: str = "en,pt,de"
    tick_interval_s: float = 1.0
    # A snapshot older than this makes /readyz fail.
    max_snapshot_age_s: float = 10.0
    # Newest event older than this flips the widget to "Paused".
    stale_after_s: float = 60.0
    live_cache_max_age_s: int = 1
    activity_cache_ttl_s: int = 10


@lru_cache
def clickhouse_settings() -> ClickHouseSettings:
    return ClickHouseSettings()


@lru_cache
def ingest_settings() -> IngestSettings:
    return IngestSettings()


@lru_cache
def api_settings() -> ApiSettings:
    return ApiSettings()
