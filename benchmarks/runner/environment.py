"""Allowlisted machine metadata; never dump environment variables or mount options."""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess

from .adapters import environment


def version(command, home):
    try:
        result = subprocess.run(command, env=environment(home), capture_output=True, timeout=15,
                                check=True, text=True)
        return result.stdout.splitlines()[0][:200]
    except (OSError, subprocess.SubprocessError, IndexError):
        return "unavailable"


def filesystem(path):
    if platform.system() == "Linux":
        try:
            result = subprocess.run(["findmnt", "-J", "-T", str(path), "-o", "FSTYPE"],
                                    capture_output=True, text=True, check=False)
            return json.loads(result.stdout)["filesystems"][0]["fstype"]
        except (OSError, ValueError, KeyError, IndexError):
            return "unknown"
    if platform.system() == "Darwin":
        # stat reports filesystem type only, not devices or potentially sensitive mounts.
        try:
            return subprocess.check_output(["stat", "-f", "%T", str(path)], text=True).strip()
        except (OSError, subprocess.SubprocessError):
            return "unknown"
    return "unknown (supply --storage description)"


def collect(root, output, executables, storage):
    env = environment(output)
    def git(*args):
        try:
            return subprocess.check_output(["git", "-C", str(root), *args], env=env, text=True).strip()
        except (OSError, subprocess.SubprocessError):
            return "unknown"
    cpu = platform.processor() or platform.machine()
    if platform.system() == "Linux":
        try:
            with open("/proc/cpuinfo", encoding="utf-8") as stream:
                cpu = next(line.split(":", 1)[1].strip() for line in stream if line.startswith("model name"))
        except (OSError, StopIteration):
            pass
    fs = filesystem(output)
    if storage == "auto":
        storage = "ramdisk" if fs in ("tmpfs", "ramfs") else "unknown; declare real-disk or ramdisk with --storage"
    metadata = {"os": platform.platform(), "cpu": cpu, "logical_cpus": os.cpu_count(),
                # rustup needs its real toolchain home for this read-only version
                # query. No Vise process ever receives that home.
                "python": platform.python_version(), "rust": version(["rustc", "--version"], Path.home()),
                "vise_commit": git("rev-parse", "HEAD"), "vise_dirty": bool(git("status", "--porcelain")),
                "filesystem": fs, "storage": storage, "tools": {}}
    for name, executable in executables.items():
        with open(executable, "rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        if name == "fixture":
            import tomllib
            with (root / "benchmarks/fixture/Cargo.toml").open("rb") as manifest:
                tool_version = "vise-bench-fixture " + tomllib.load(manifest)["package"]["version"]
        else:
            tool_version = version([executable, "--version"], output)
        metadata["tools"][name] = {"path": str(Path(executable).resolve()),
                                   "version": tool_version, "sha256": digest}
    if os.name == "posix":
        import resource
        metadata["file_descriptor_limit"] = list(resource.getrlimit(resource.RLIMIT_NOFILE))
    return metadata


def tools(vise, fixture, curl):
    result = {"vise": str(vise), "fixture": str(fixture)}
    aria = shutil.which("aria2c")
    if aria:
        result["aria2"] = aria
    else:
        print("aria2c unavailable: aria2 profiles skipped; Vise-only runs remain enabled.")
    if curl:
        found = shutil.which("curl")
        if found:
            result["curl"] = found
        else:
            print("curl unavailable: C=1 curl cohort skipped.")
    return {name: str(Path(executable).resolve()) for name, executable in result.items()}
