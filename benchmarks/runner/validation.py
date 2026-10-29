"""Hash validation is mandatory, including when a client exits successfully."""
import hashlib


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def verify(directory, sizes, hashes):
    checks = []
    for index, (size, expected) in enumerate(zip(sizes, hashes, strict=True)):
        path = directory / f"{index}.bin"
        actual_size = path.stat().st_size if path.is_file() else None
        actual_hash = sha256(path) if actual_size is not None else None
        checks.append({"file": index, "expected_size": size, "actual_size": actual_size,
                       "expected_sha256": expected, "actual_sha256": actual_hash,
                       "valid": actual_size == size and actual_hash == expected})
    return checks


def outcome(returncode, checks):
    return {"status": "ok" if returncode == 0 and checks and all(c["valid"] for c in checks) else "failed",
            "returncode": returncode, "verification": checks}


def eligible(run):
    return run["status"] == "ok" and not run.get("warmup", False)


def exit_code(runs):
    return int(any(run["status"] == "failed" for run in runs))
