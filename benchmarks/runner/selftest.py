"""Deliberately invalid outputs: these must fail the same verification/report path."""
import hashlib
import json
import tempfile
from pathlib import Path
from . import report, validation


def run(output, kind):
    expected = b"deterministic benchmark payload"
    cases = {"wrong-byte": b"Deterministic benchmark payload", "truncated": expected[:-1], "missing": None}
    selected = cases if kind == "all" else {kind: cases[kind]}
    runs = []
    for name, payload in selected.items():
        with tempfile.TemporaryDirectory(dir=output) as temporary:
            directory = Path(temporary)
            if payload is not None:
                (directory / "0.bin").write_bytes(payload)
            checks = validation.verify(directory, [len(expected)], [hashlib.sha256(expected).hexdigest()])
            result = {"scenario": "corruption-self-test", "cohort": name, "profile": "verification",
                      "warmup": False, **validation.outcome(0, checks)}
            runs.append(result)
            (output / f"{name}.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
            print(f'{name}: {result["status"]} (deliberate corruption)')
    report.write(output, runs, {"self_test": True}, True, {}, [])
    return validation.exit_code(runs)
