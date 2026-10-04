from livedemos.ingest.health import MAX_SILENCE_S, heartbeat_age

METRICS = """# HELP ingest_loop_heartbeat_timestamp_seconds Last time ...
# TYPE ingest_loop_heartbeat_timestamp_seconds gauge
ingest_loop_heartbeat_timestamp_seconds 1.0e+03
"""


def test_reads_the_heartbeat_age() -> None:
    assert heartbeat_age(METRICS, now=1_030.0) == 30.0
    assert heartbeat_age(METRICS, now=1_000.0 + MAX_SILENCE_S + 1) > MAX_SILENCE_S


def test_missing_heartbeat_is_unknown() -> None:
    assert heartbeat_age("other_metric 1\n", now=0.0) is None
