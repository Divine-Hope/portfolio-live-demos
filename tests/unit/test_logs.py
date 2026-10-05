import ast
import logging
from pathlib import Path

import livedemos

RESERVED = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


def test_log_extras_never_use_a_reserved_logrecord_key() -> None:
    """`extra={"name": ...}` raises KeyError, but only when that level is enabled: tests
    run at WARNING and never see it. Production runs at INFO. Checked statically instead."""
    clashes = []
    for path in Path(livedemos.__file__).parent.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Call):
                continue
            for keyword in node.keywords:
                if keyword.arg == "extra" and isinstance(keyword.value, ast.Dict):
                    for key in keyword.value.keys:
                        if isinstance(key, ast.Constant) and key.value in RESERVED:
                            clashes.append(f"{path.name}:{node.lineno} {key.value!r}")
    assert not clashes, clashes
