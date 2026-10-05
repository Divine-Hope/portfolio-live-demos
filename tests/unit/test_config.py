import pytest
from pydantic import ValidationError

from livedemos.config import ApiSettings, IngestSettings


@pytest.mark.parametrize(
    "overrides",
    [
        {"flush_max_rows": 0},  # asyncio.Queue(maxsize=0) is unbounded: no memory guard
        {"flush_interval_s": 0},
        {"backoff_initial_s": -1},
        {"backoff_initial_s": 60, "backoff_max_s": 30},
        {"seam_ids": 0},
        {"wikis": " , "},
        {"metrics_port": 70_000},
        {"first_boot_lookback_s": 10, "retention_s": 5},  # would record a gap backwards
        {"idle_timeout_s": float("inf")},
        {"flush_interval_s": float("nan")},
        {"seam_ids": 10_000_000},
        {"wikis": "enwiki,ENWIKI"},
    ],
)
def test_ingest_settings_refuse_values_that_break_guarantees(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        IngestSettings(**overrides)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "overrides",
    [
        {"stale_after_s": 0},
        {"activity_max_concurrency": 0},
        {"langs": ""},
        {"langs": "en,en,pt"},  # would draw 120 one-minute buckets for en
        {"langs": "en,Pt"},
        {"activity_wait_s": float("inf")},
        {"activity_max_concurrency": 4, "activity_max_pending": 2},
    ],
)
def test_api_settings_refuse_nonsense(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ApiSettings(**overrides)  # type: ignore[arg-type]


def test_defaults_are_valid() -> None:
    assert IngestSettings().flush_max_rows > 0
    assert ApiSettings().lang_list == ["en", "pt", "de"]
