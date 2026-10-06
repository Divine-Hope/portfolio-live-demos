"""ClickHouse refuses to start on a config file it can't parse, so parse them all here."""

import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

CONFIG = sorted((Path(__file__).parents[2] / "clickhouse").rglob("*.xml"))


def test_there_are_config_files() -> None:
    assert len(CONFIG) >= 3


@pytest.mark.parametrize("path", CONFIG, ids=lambda p: p.name)
def test_config_file_is_well_formed_xml(path: Path) -> None:
    # "--" inside a comment is the classic: XML forbids it, and ClickHouse won't start.
    assert ET.parse(path).getroot().tag == "clickhouse"  # noqa: S314 (our own files)
