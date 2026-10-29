import hashlib
from pathlib import Path
import tempfile
import unittest

from benchmarks.runner import validation


class CorruptionTests(unittest.TestCase):
    def check_corruption(self, data):
        with tempfile.TemporaryDirectory(dir="target") as directory:
            output = Path(directory)
            expected = b"deterministic benchmark payload"
            if data is not None:
                (output / "0.bin").write_bytes(data)
            result = validation.verify(output, [len(expected)], [hashlib.sha256(expected).hexdigest()])
            run = validation.outcome(0, result)
            self.assertEqual(run["status"], "failed")
            self.assertFalse(validation.eligible(run))
            self.assertEqual(validation.exit_code([run]), 1)

    def test_wrong_byte_is_failed_excluded_and_nonzero(self):
        self.check_corruption(b"Deterministic benchmark payload")

    def test_truncated_is_failed_excluded_and_nonzero(self):
        self.check_corruption(b"deterministic")

    def test_missing_is_failed_excluded_and_nonzero(self):
        self.check_corruption(None)

    def test_correct_output_is_eligible(self):
        with tempfile.TemporaryDirectory(dir="target") as directory:
            path = Path(directory)
            (path / "0.bin").write_bytes(b"abc")
            checks = validation.verify(path, [3], [hashlib.sha256(b"abc").hexdigest()])
            run = validation.outcome(0, checks)
            self.assertTrue(validation.eligible(run))
            self.assertEqual(validation.exit_code([run]), 0)


if __name__ == "__main__":
    unittest.main()
