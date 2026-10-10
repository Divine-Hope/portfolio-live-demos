import pytest
from pydantic import SecretStr, ValidationError

from livedemos.config import ApiSettings, ClickHouseSettings, IngestSettings, OpsSettings


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


def test_the_slo_target_cant_go_below_the_requirement() -> None:
    # Requirement N2: 99.9% at minimum. A lower target would shrink what the page promises.
    with pytest.raises(ValidationError):
        OpsSettings(slo_target=0.99)
    assert OpsSettings().slo_target == 0.999
    assert OpsSettings(slo_target=0.9999).slo_target == 0.9999


def test_comma_lists_are_parsed_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INGEST_WIKIS", "enwiki, dewiki")
    monkeypatch.setenv("INGEST_TYPES", "edit")
    settings = IngestSettings()
    assert settings.wikis == frozenset({"enwiki", "dewiki"})
    assert settings.types == frozenset({"edit"})


def test_secrets_stay_out_of_reprs() -> None:
    settings = ClickHouseSettings(password=SecretStr("hunter2"))
    assert "hunter2" not in repr(settings)
    assert settings.password.get_secret_value() == "hunter2"
