import unittest
from benchmarks.tail_simulation import simulate


class TailSimulationTests(unittest.TestCase):
    def test_reference_and_uniform_scenarios(self):
        static = simulate("static", [8, 8, 8, .8])
        weighted = simulate("weighted", [8, 8, 8, .8])
        half = simulate("half", [8, 8, 8, .8])
        self.assertEqual(static["finish_s"], 160.001)
        self.assertEqual(weighted["finish_s"], 20.78225)
        self.assertEqual(weighted["splits"], 3)
        self.assertEqual(weighted["duplicate_bytes"], 65536)
        self.assertLess(weighted["tail_s"], 10)
        self.assertLess(weighted["finish_s"], half["finish_s"])
        for policy in ("static", "half", "weighted"):
            uniform = simulate(policy, [8] * 4)
            self.assertEqual(uniform["finish_s"], 16.001)
            self.assertEqual(uniform["splits"], 0)

    def test_fine_grained_requests_pay_latency_and_first_drop_tail(self):
        fine = simulate("fine", [8] * 4, setup=.08)
        self.assertEqual(fine["body_requests"], 512)
        self.assertEqual(fine["finish_s"], 26.24)
        self.assertGreater(fine["tail_s"], 10)

    def test_overrun_is_an_explicit_assumption(self):
        for overrun in (0, 65536, 262144, 1048576):
            result = simulate("weighted", [8, 8, 8, .8], overrun=overrun)
            self.assertEqual(result["duplicate_bytes"], overrun)
            self.assertEqual(result, simulate("weighted", [8, 8, 8, .8], overrun=overrun))
