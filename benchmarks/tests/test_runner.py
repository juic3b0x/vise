import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from benchmarks import run
from benchmarks.runner import adapters, environment, metrics, report, measurement


class RunnerTests(unittest.TestCase):
    def test_environment_records_executed_absolute_paths_and_hashes(self):
        with tempfile.TemporaryDirectory(dir="target") as temporary:
            directory = Path(os.path.relpath(temporary))
            paths = {name: directory / name for name in ("vise", "fixture", "aria2", "curl")}
            for name, path in paths.items():
                path.write_bytes(name.encode())
            with patch("shutil.which", side_effect=lambda name: str(paths["aria2" if name == "aria2c" else name])):
                tools = environment.tools(paths["vise"], paths["fixture"], True)
            with patch.object(environment, "version", return_value="test version"), \
                    patch.object(environment, "filesystem", return_value="testfs"), \
                    patch("subprocess.check_output", return_value="test"):
                metadata = environment.collect(Path.cwd(), directory, tools, "real-disk")
            for name, path in paths.items():
                expected = str(path.resolve())
                self.assertEqual(tools[name], expected)
                entry = metadata["tools"][name]
                self.assertEqual(entry["path"], expected)
                self.assertTrue(Path(entry["path"]).is_absolute())
                self.assertEqual(entry["sha256"], hashlib.sha256(name.encode()).hexdigest())
                if name != "fixture":
                    profile = "aria2-matched" if name == "aria2" else name
                    command, _ = adapters.command(profile, tools[name],
                                                  ["http://127.0.0.1:1/file/0.bin"],
                                                  directory, directory, 1, 1, 5, 30, False)
                    self.assertEqual(entry["path"], command[0])

    def test_missing_comparison_tools_is_a_clear_skip(self):
        output = io.StringIO()
        with tempfile.TemporaryDirectory(dir="target") as temporary:
            directory = Path(temporary)
            paths = {name: directory / name for name in ("vise", "fixture")}
            for path in paths.values():
                path.write_bytes(b"test executable")
            with patch("shutil.which", return_value=None), contextlib.redirect_stdout(output):
                tools = environment.tools(paths["vise"], paths["fixture"], True)
            with patch.object(environment, "version", return_value="test version"), \
                    patch.object(environment, "filesystem", return_value="testfs"), \
                    patch("subprocess.check_output", return_value="test"):
                metadata = environment.collect(Path.cwd(), directory, tools, "real-disk")
        self.assertEqual(set(tools), {"vise", "fixture"})
        self.assertNotIn("aria2", metadata["tools"])
        self.assertNotIn("curl", metadata["tools"])
        self.assertTrue(all(entry["path"] for entry in metadata["tools"].values()))
        self.assertIn("aria2 profiles skipped", output.getvalue())
        self.assertIn("curl cohort skipped", output.getvalue())

    def test_invalid_configuration_is_rejected_without_values(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory(dir="target") as temporary:
            path = Path(temporary) / "scenarios.json"
            path.write_text('["not-an-object"]')
            args = SimpleNamespace(config=path, smoke=False)
            with self.assertRaisesRegex(ValueError, "must be an object"):
                run.parameters(args)

    def test_negative_selftests_reach_final_reports_and_fail_process(self):
        with tempfile.TemporaryDirectory(dir="target/benchmarks") as temporary:
            output = Path(temporary) / "reports"
            with contextlib.redirect_stdout(io.StringIO()):
                code = run.main(["--self-test", "all", "--output", str(output)])
            result = json.loads((output / "results.json").read_text())
            self.assertEqual(code, 1)
            self.assertEqual(len(result["runs"]), 3)
            for row in result["summary"]:
                self.assertEqual(row["successes"], 0)
                self.assertTrue(all(value is None for value in row["metrics"].values()))

    def test_profiles_leave_cache_and_mmap_at_defaults(self):
        with tempfile.TemporaryDirectory(dir="target") as temporary:
            root = Path(temporary)
            for profile in ("aria2-matched", "aria2-defaults"):
                command, stdin = adapters.command(profile, "aria2c", ["http://127.0.0.1:1/file/0.bin"],
                                                  root, root, 4, 5, 3, 30, False)
                self.assertNotIn("--disk-cache=0", command)
                self.assertNotIn("--enable-mmap=false", command)
                self.assertIn("--max-tries=4", command)
                self.assertIn("--split=4", command)
                self.assertEqual("--file-allocation=none" in command, profile == "aria2-matched")
                self.assertEqual("--no-conf" in command, profile == "aria2-matched")
                if profile == "aria2-defaults":
                    self.assertFalse(any(arg.startswith(("--allow-overwrite", "--continue", "--auto-file-renaming", "--no-netrc")) for arg in command))
                self.assertIn(b"/file/0.bin", stdin)

    def test_small_default_uses_1000_urls_over_interactive_eof(self):
        config = json.loads((run.ROOT / "benchmarks/scenarios.json").read_text(encoding="utf-8"))
        sizes = run.fixture_config(config, "small", config["seed"])["sizes"]
        self.assertEqual(len(sizes), 1000)
        urls = [f"http://127.0.0.1:1/file/{i}.bin" for i in range(len(sizes))]
        command, stdin = adapters.command("vise", "vise", urls, Path("target"), Path("target"), 1, 5, 5, 30, True)
        self.assertIn("--interactive", command)
        self.assertNotIn("--input", command)
        self.assertEqual(len(stdin.splitlines()), 1000)
        self.assertNotIn(b"quit", stdin)

    def test_isolation_drops_credentials_proxy_and_logs(self):
        with tempfile.TemporaryDirectory(dir="target") as temporary:
            home = Path(temporary) / "home"
            adapters.isolate(home)
            with patch.dict(os.environ, {"VISE_API_TOKEN": "sentinel", "HTTPS_PROXY": "sentinel", "RUST_LOG": "trace"}):
                env = adapters.environment(home)
            self.assertNotIn("sentinel", json.dumps(env))
            self.assertEqual(env["HOME"], str(home))
            self.assertEqual(env["USERPROFILE"], str(home))
            self.assertIn('VISE_API_TOKEN = ""', (home / ".vise/config.toml").read_text())

    def test_order_rotates_and_reverses(self):
        profiles = ["vise", "aria2-matched", "aria2-defaults"]
        observed = [adapters.order(profiles, i) for i in range(6)]
        self.assertEqual(len({tuple(order) for order in observed}), 6)

    def test_per_process_measurement_and_timeout(self):
        import sys
        result = measurement.launch([sys.executable, "-c", "sum(range(100000))"], os.environ.copy(), Path.cwd())
        self.assertEqual(result["returncode"], 0)
        self.assertGreater(result["wall_s"], 0)
        self.assertIsNotNone(result["cpu_user_s"])
        result = measurement.launch([sys.executable, "-c", "import time;time.sleep(20)"], os.environ.copy(), Path.cwd(), timeout=0.05)
        self.assertTrue(result["timed_out"])
        self.assertNotEqual(result["returncode"], 0)

    def test_overlapping_ranges_have_no_clear_difference(self):
        result = metrics.comparison(metrics.span([2, 4, 6]), metrics.span([3, 5, 8]))
        self.assertEqual(result["label"], "no clear difference")
        self.assertEqual(result["reference_over_candidate_median"], 1.25)

    def test_flaky_is_excluded_from_throughput_comparisons(self):
        runs = [dict(status="ok", warmup=False, scenario="flaky", cohort="C1", profile=profile,
                     wall_s=1, throughput_mib_s=100) for profile in ("vise", "aria2-matched") for _ in range(5)]
        rows = report.aggregate(runs)
        self.assertTrue(all("throughput_mib_s" not in row["metrics"] for row in rows))
        self.assertEqual(report.comparisons(rows, False), [])

    def test_failures_and_warmups_do_not_enter_aggregates(self):
        runs = [dict(status=status, warmup=warm, scenario="large", cohort="C1", profile="vise", wall_s=time)
                for status, warm, time in [("ok", True, 100), ("failed", False, 200), ("ok", False, 1)]]
        row, = report.aggregate(runs)
        self.assertEqual(row["metrics"]["wall_s"]["median"], 1)
        self.assertEqual(row["success_rate"], 0.5)

    def test_tail_wasted_bytes_and_slow_share(self):
        def request(connection, start, count, first, last, slow=False, reset=False, started=0):
            return dict(connection=connection, file=0, start=start, end=start + count - 1,
                        status=206, head=False, slow=slow, body_bytes=count,
                        started_s=started, first_byte_s=first, last_byte_s=last, reset=reset)
        requests = [request(1, 0, 10, 0, 2), request(2, 10, 10, 1, 4, True)]
        result = metrics.server_metrics(dict(requests=requests, connections=2, peak_open_connections=2), 2)
        self.assertEqual(result["tail_s"], 2)
        self.assertEqual(result["slow_connections_with_body"], 1)
        self.assertEqual(result["slow_body_share"], 0.5)
        requests += [request(3, 0, 5, 5, 6)]
        result = metrics.server_metrics(dict(requests=requests, connections=3, peak_open_connections=2), 4)
        self.assertEqual(result["duplicate_body_bytes"], 5)
        self.assertIsNone(result["tail_s"])


if __name__ == "__main__":
    unittest.main()
