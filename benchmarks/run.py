#!/usr/bin/env python3
"""Python 3.11+ stdlib benchmark driver. Run from any directory; no system tuning."""
import argparse
import json
from pathlib import Path
import random
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmarks.runner import adapters, environment, measurement, report, selftest, validation
from benchmarks.runner.fixture import Fixture
from benchmarks.runner.metrics import server_metrics


def parameters(args):
    if args.config.stat().st_size > 1024 * 1024:
        raise ValueError("scenario configuration exceeds 1 MiB")
    config = json.loads(args.config.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("scenario configuration must be an object")
    if args.smoke:
        if not isinstance(config.get("smoke"), dict):
            raise ValueError("smoke configuration must be an object")
        config.update(config["smoke"])
    config.pop("smoke", None)
    for key in ("connections", "jobs", "seed"):
        value = getattr(args, key)
        if value is not None:
            config[key] = value
    if args.small_files is not None:
        config["small_count"] = args.small_files
    if args.scenarios:
        config["scenarios"] = args.scenarios.split(",")
    known = {"large", "capped", "heterogeneous", "small", "latency", "flaky"}
    scenarios = config.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios or any(not isinstance(s, str) for s in scenarios):
        raise ValueError("scenarios must be a nonempty string list")
    if len(set(scenarios)) != len(scenarios) or not set(scenarios) <= known:
        raise ValueError("invalid scenarios")
    bounds = {"connections": (1, 16), "jobs": (1, 64), "seed": (0, 2**63 - 1),
              "retries": (0, 20), "timeout": (1, 3600), "run_timeout": (1, 86400),
              "large_bytes": (1, 16 * 1024**3), "small_count": (1, 1000),
              "small_min_bytes": (1, 1024**3), "small_max_bytes": (1, 1024**3),
              "rate_bytes_per_second": (1, 16 * 1024**3), "slow_every": (1, 1000),
              "slow_divisor": (1, 1000), "latency_ms": (0, 60000), "faults_per_file": (0, 10)}
    if set(config) != set(bounds) | {"scenarios"}:
        raise ValueError("unknown or missing scenario parameter")
    if any(type(config[key]) is not int or not low <= config[key] <= high
           for key, (low, high) in bounds.items()):
        raise ValueError("scenario parameter outside bounds")
    if config["large_bytes"] < config["connections"] * 2 * 1024**2:
        raise ValueError("large_bytes must allow at least 2 MiB per connection for aria2 splitting")
    if config["small_min_bytes"] > config["small_max_bytes"] or config["small_max_bytes"] > 1024**2:
        raise ValueError("small files must be between 1 byte and 1 MiB")
    if config["rate_bytes_per_second"] < config["slow_divisor"]:
        raise ValueError("heterogeneous cap must remain at least one byte per second")
    return config


def fixture_config(config, scenario, seed):
    rng = random.Random(seed)
    sizes = [rng.randint(config["small_min_bytes"], config["small_max_bytes"])
             for _ in range(config["small_count"])] if scenario == "small" else [config["large_bytes"]]
    return {"seed": seed, "sizes": sizes,
            "rate": config["rate_bytes_per_second"] if scenario in ("capped", "heterogeneous") else 0,
            "latency_ms": config["latency_ms"] if scenario == "latency" else 0,
            "slow_every": config["slow_every"] if scenario == "heterogeneous" else 0,
            "slow_divisor": config["slow_divisor"],
            "faults_per_file": config["faults_per_file"] if scenario == "flaky" else 0}


def run_one(output, executable, fixture_executable, profile, scenario, cohort, config, round_index, warmup):
    seed = config["seed"] + round_index
    fixture_settings = fixture_config(config, scenario, seed)
    connections = 1 if cohort == "C1" or scenario == "small" else config["connections"]
    jobs = config["jobs"] if scenario == "small" else 1
    run = {"scenario": scenario, "cohort": cohort, "profile": profile, "round": round_index,
           "warmup": warmup, "seed": seed, "connection_budget_per_file": connections, "jobs": jobs}
    with tempfile.TemporaryDirectory(prefix="work-", dir=output) as directory:
        work = Path(directory)
        home, destination = work / "home", work / "output"
        adapters.isolate(home)
        destination.mkdir()
        env = adapters.environment(home)
        fixture = None
        try:
            fixture = Fixture(fixture_executable, fixture_settings, work, env)
            ready_hashes = fixture.ready["hashes"]
            urls = [f'http://127.0.0.1:{fixture.ready["port"]}/file/{i}.bin'
                    for i in range(len(fixture_settings["sizes"]))]
            command, stdin = adapters.command(profile, executable, urls, destination, work,
                                              connections, jobs, config["retries"], config["timeout"])
            # Store exact flags but replace local ephemeral paths/ports; no environment dump.
            run["argv"] = [part.replace(str(work), "<run>").replace(str(fixture.ready["port"]), "<port>") for part in command]
            counters = measurement.launch(command, env, work, stdin, config["run_timeout"])
            run.update(counters)
            stats = fixture.finish()
            fixture = None
            run["fixture"] = stats
            # The expected digests were generated before timing, never from downloaded data.
            checks = validation.verify(destination, fixture_settings["sizes"], ready_hashes)
            run.update(validation.outcome(counters["returncode"], checks))
            run.update(server_metrics(stats, connections * jobs))
            run["cpu_total_s"] = (run["cpu_user_s"] + run["cpu_system_s"]) if run.get("cpu_user_s") is not None and run.get("cpu_system_s") is not None else None
            correct = run["status"] == "ok"
            run["wasted_bytes"] = run["body_bytes"] - sum(fixture_settings["sizes"]) if correct else None
            if correct and scenario != "flaky":
                run["throughput_mib_s"] = sum(fixture_settings["sizes"]) / 1024**2 / counters["wall_s"]
        except (OSError, ValueError, RuntimeError, TimeoutError) as error:
            run.update(status="failed", failure=type(error).__name__)
        finally:
            if fixture:
                fixture.abort()
    return run


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--smoke", action="store_true", help="3 measured runs, NON-PUBLISHABLE")
    parser.add_argument("--self-test", choices=("wrong-byte", "truncated", "missing", "all"),
                        help="deliberate corruption checks; expected exit status 1")
    parser.add_argument("--config", type=Path, default=ROOT / "benchmarks/scenarios.json")
    parser.add_argument("--output", type=Path, required=True, help="new directory under target/benchmarks")
    parser.add_argument("--vise", type=Path, default=ROOT / "target/release" / ("vise.exe" if sys.platform == "win32" else "vise"))
    parser.add_argument("--fixture", type=Path, default=ROOT / "benchmarks/fixture/target/release" / ("vise-bench-fixture.exe" if sys.platform == "win32" else "vise-bench-fixture"))
    parser.add_argument("--profiles", default=",".join(adapters.PROFILES))
    parser.add_argument("--curl", action="store_true", help="add a separate C=1 cohort, including curl")
    parser.add_argument("--scenarios", help="comma-separated subset")
    parser.add_argument("--runs", type=int, default=6, help="full profile measured repetitions, minimum 5")
    parser.add_argument("--connections", type=int)
    parser.add_argument("--jobs", type=int)
    parser.add_argument("--small-files", type=int, help="small file count, 1–1000 (full default: 1000)")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--storage", default="auto", choices=("auto", "real-disk", "ramdisk"))
    args = parser.parse_args(argv)
    if args.self_test:
        output = args.output.resolve()
        if not output.is_relative_to(ROOT / "target/benchmarks") or output == ROOT / "target/benchmarks":
            parser.error("output must be a new subdirectory of target/benchmarks")
        output.mkdir(parents=True, exist_ok=False)
        return selftest.run(output, args.self_test)
    try:
        config = parameters(args)
        profiles = args.profiles.split(",")
        if len(set(profiles)) != len(profiles) or not set(profiles) <= set(adapters.PROFILES) or "vise" not in profiles:
            raise ValueError("profiles must include vise and contain only documented profiles")
        if args.runs < 5:
            raise ValueError("full profile requires at least 5 measured repetitions")
        output = args.output.resolve()
        if not output.is_relative_to(ROOT / "target/benchmarks") or output == ROOT / "target/benchmarks":
            raise ValueError("output must be a new subdirectory of target/benchmarks")
        for path in (args.vise, args.fixture):
            if not path.is_file():
                raise ValueError("build the Vise and fixture release binaries first")
        output.mkdir(parents=True, exist_ok=False)
    except (OSError, ValueError) as error:
        parser.error(str(error) if isinstance(error, ValueError) else "cannot read configuration or create output directory")
    executables = environment.tools(args.vise.resolve(), args.fixture.resolve(), args.curl)
    skips = [p for p in profiles if p.startswith("aria2-") and "aria2" not in executables]
    profiles = [p for p in profiles if p not in skips]
    env = environment.collect(ROOT, output, executables, args.storage)
    measured = 3 if args.smoke else args.runs
    print("NON-PUBLISHABLE SMOKE" if args.smoke else "FULL PROFILE")
    runs = []
    cohorts = [("matched-C", profiles)]
    if args.curl and "curl" in executables:
        cohorts.append(("C1", profiles + ["curl"]))
    for scenario in config["scenarios"]:
        for cohort, selected in cohorts:
            for repetition in range(measured + 1):
                for profile in adapters.order(selected, repetition):
                    key = "aria2" if profile.startswith("aria2-") else profile
                    run = run_one(output, executables[key], executables["fixture"], profile,
                                  scenario, cohort, config, repetition, repetition == 0)
                    runs.append(run)
                    filename = f'{scenario}-{cohort}-{repetition}-{profile}.json'
                    (output / filename).write_text(json.dumps(run, indent=2) + "\n", encoding="utf-8")
                    print(f'{scenario} {cohort} {profile} {"warm-up" if repetition == 0 else repetition}: {run["status"]}')
    report.write(output, runs, env, args.smoke, config, skips)
    print("Reports: " + str(output.relative_to(ROOT)))
    return validation.exit_code(runs)


if __name__ == "__main__":
    sys.exit(main())
