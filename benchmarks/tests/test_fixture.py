"""Real local HTTP integration; build the independent release fixture first."""
import hashlib
import http.client
import json
import os
from pathlib import Path
import tempfile
import time
import unittest

from benchmarks.runner import adapters
from benchmarks.runner.fixture import Fixture

ROOT = Path(__file__).resolve().parents[2]
EXE = ROOT / "benchmarks/fixture/target/release/vise-bench-fixture"
if os.name == "nt":
    EXE = EXE.with_suffix(".exe")


@unittest.skipUnless(EXE.is_file(), "build benchmarks/fixture release binary first")
class FixtureTests(unittest.TestCase):
    def start(self, **changes):
        temporary = tempfile.TemporaryDirectory(dir=ROOT / "target")
        self.addCleanup(temporary.cleanup)
        work = Path(temporary.name)
        adapters.isolate(work / "home")
        config = dict(seed=42, sizes=[65536], rate=0, latency_ms=0,
                      slow_every=0, slow_divisor=10, faults_per_file=0)
        config.update(changes)
        fixture = Fixture(EXE, config, work, adapters.environment(work / "home"))
        self.addCleanup(fixture.abort)
        return fixture

    def connect(self, fixture):
        connection = http.client.HTTPConnection("127.0.0.1", fixture.ready["port"], timeout=10)
        self.addCleanup(connection.close)
        return connection

    def test_hash_ranges_if_range_keepalive_and_redacted_logs(self):
        fixture = self.start()
        client = self.connect(fixture)
        client.request("GET", "/file/0.bin")
        response = client.getresponse()
        etag = response.getheader("ETag")
        whole = response.read()
        self.assertEqual(hashlib.sha256(whole).hexdigest(), fixture.ready["hashes"][0])
        client.request("GET", "/file/0.bin", headers={"Range": "bytes=7-101", "If-Range": etag})
        response = client.getresponse()
        self.assertEqual(response.status, 206)
        self.assertEqual(response.read(), whole[7:102])
        client.request("GET", "/file/0.bin", headers={"Range": "bytes=7-101", "If-Range": '"stale"'})
        response = client.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.read(), whole)
        client.request("GET", "/file/0.bin", headers={"Cookie": "sentinel-fixture-must-not-log"})
        response = client.getresponse()
        self.assertEqual(response.status, 400)
        response.read()
        client.close()
        stats = fixture.finish()
        self.assertEqual(stats["connections"], 1)
        self.assertEqual(len(stats["requests"]), 4)
        self.assertNotIn("sentinel-fixture-must-not-log", json.dumps(stats))

    def test_suffix_head_and_unsatisfiable_range(self):
        fixture = self.start()
        client = self.connect(fixture)
        client.request("HEAD", "/file/0.bin")
        response = client.getresponse()
        self.assertEqual(response.getheader("Content-Length"), "65536")
        self.assertEqual(response.read(), b"")
        client.request("GET", "/file/0.bin", headers={"Range": "bytes=-20"})
        response = client.getresponse()
        self.assertEqual(response.status, 206)
        self.assertEqual(len(response.read()), 20)
        client.request("GET", "/file/0.bin", headers={"Range": "bytes=999999-"})
        response = client.getresponse()
        self.assertEqual(response.status, 416)
        response.read()
        client.close()
        stats = fixture.finish()
        self.assertEqual(stats["requests"][0]["body_bytes"], 0)

    def test_seeded_reset_is_finite_and_recovery_hashes_correctly(self):
        fixture = self.start(faults_per_file=1)
        client = self.connect(fixture)
        client.request("GET", "/file/0.bin")
        response = client.getresponse()
        with self.assertRaises((http.client.IncompleteRead, ConnectionResetError)):
            response.read()
        client.close()
        client = self.connect(fixture)
        client.request("GET", "/file/0.bin")
        response = client.getresponse()
        data = response.read()
        self.assertEqual(hashlib.sha256(data).hexdigest(), fixture.ready["hashes"][0])
        client.close()
        stats = fixture.finish()
        reset, complete = stats["requests"]
        self.assertTrue(reset["reset"])
        self.assertFalse(complete["reset"])
        self.assertLess(reset["body_bytes"], 65536)

    def test_latency_and_per_connection_heterogeneity(self):
        fixture = self.start(rate=655360, slow_every=2, slow_divisor=10, latency_ms=40)
        durations = []
        for _ in range(2):
            client = self.connect(fixture)
            start = time.monotonic()
            client.request("GET", "/file/0.bin")
            response = client.getresponse()
            self.assertEqual(len(response.read()), 65536)
            durations.append(time.monotonic() - start)
            client.close()
        stats = fixture.finish()
        self.assertGreaterEqual(durations[0], 0.04)
        self.assertGreaterEqual(durations[1], 1.0)
        self.assertEqual([request["slow"] for request in stats["requests"]], [False, True])


if __name__ == "__main__":
    unittest.main()
