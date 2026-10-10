from livedemos.config import IngestSettings
from livedemos.ingest.health import heartbeat_age

MAX_SILENCE_S = IngestSettings().max_silence_s

METRICS = """# HELP ingest_loop_heartbeat_timestamp_seconds Last time ...
# TYPE ingest_loop_heartbeat_timestamp_seconds gauge
ingest_loop_heartbeat_timestamp_seconds 1.0e+03
"""


def test_reads_the_heartbeat_age() -> None:
    assert heartbeat_age(METRICS, now=1_030.0) == 30.0
    age = heartbeat_age(METRICS, now=1_000.0 + MAX_SILENCE_S + 1)
    assert age is not None
    assert age > MAX_SILENCE_S


def test_missing_heartbeat_is_unknown() -> None:
    assert heartbeat_age("other_metric 1\n", now=0.0) is None
