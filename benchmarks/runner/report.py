import json
from collections import defaultdict
from .metrics import comparison, span
from .validation import eligible

METRICS = ("wall_s", "throughput_mib_s", "cpu_user_s", "cpu_system_s", "cpu_total_s",
           "fixture_cpu_user_s", "fixture_cpu_system_s", "fixture_cpu_total_s",
           "peak_rss_bytes", "requests", "connections", "wasted_bytes", "tail_s",
           "retry_requests", "slow_connections_with_body", "slow_body_share")


def aggregate(runs):
    groups = defaultdict(list)
    for run in runs:
        if not run.get("warmup", False):
            groups[(run["scenario"], run["cohort"], run["profile"])].append(run)
    rows = []
    for (scenario, cohort, profile), attempts in groups.items():
        good = [r for r in attempts if eligible(r)]
        metrics = {key: span([r.get(key) for r in good]) for key in METRICS}
        if scenario == "flaky":
            metrics.pop("throughput_mib_s", None)
        rows.append({"scenario": scenario, "cohort": cohort, "profile": profile,
                     "successes": len(good), "attempts": len(attempts),
                     "success_rate": len(good) / len(attempts), "metrics": metrics})
    return rows


def comparisons(rows, smoke):
    results = []
    for row in rows:
        if row["profile"] == "vise" or row["scenario"] == "flaky":
            continue
        vise = next((r for r in rows if r["scenario"] == row["scenario"]
                     and r["cohort"] == row["cohort"] and r["profile"] == "vise"), None)
        minimum = 3 if smoke else 5
        if not vise or min(vise["successes"], row["successes"]) < minimum:
            continue
        results.append({"scenario": row["scenario"], "cohort": row["cohort"],
                        "reference": row["profile"],
                        **comparison(vise["metrics"]["wall_s"], row["metrics"]["wall_s"])})
    return results


def display(metric, scale=1):
    if metric is None:
        return "n/a"
    return f'{metric["median"] / scale:.3f} [{metric["min"] / scale:.3f}, {metric["max"] / scale:.3f}]; Δ {metric["spread"] / scale:.3f}'


def write(output, runs, environment, smoke, config, skips):
    rows = aggregate(runs)
    comparisons_out = comparisons(rows, smoke)
    payload = {"schema": 1, "non_publishable": smoke, "environment": environment,
               "configuration": config, "skipped_profiles": skips, "summary": rows,
               "comparisons": comparisons_out, "runs": runs}
    (output / "results.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    lines = ["# Vise benchmark results", "",
             "**NON-PUBLISHABLE SMOKE RUN — correctness and harness validation only.**" if smoke
             else "Full profile. Check failures, storage, environment and limits before interpreting ratios.",
             "", "Values: median [min, max]; Δ = max − min. Warm-ups and failed runs excluded.",
             "", "| Scenario / cohort | Profile | Correct | Wall s | MiB/s | CPU s | Peak MiB | Requests | Connections | Wasted B | Tail s | Slow conns / body share |",
             "|---|---|---:|---|---|---|---|---|---|---|---|---|"]
    for row in rows:
        m = row["metrics"]
        lines.append(f'| {row["scenario"]} / {row["cohort"]} | {row["profile"]} | {row["successes"]}/{row["attempts"]} | '
                     + " | ".join([display(m["wall_s"]), display(m.get("throughput_mib_s")),
                                   display(m["cpu_total_s"]), display(m["peak_rss_bytes"], 1024 ** 2),
                                   display(m["requests"]), display(m["connections"]), display(m["wasted_bytes"]),
                                   display(m["tail_s"]), display(m["slow_connections_with_body"]) + " / " + display(m["slow_body_share"])]) + " |")
    lines += ["", "Flaky runs are excluded from throughput comparisons and rankings: clients have different native retry schedules.",
              "Their wall time is time to correct completion for successful runs only; success rate includes all attempts.", "",
              "| Flaky cohort / profile | Success rate | Time to correct completion s | Observed retry requests |",
              "|---|---:|---|---|"]
    for row in rows:
        if row["scenario"] == "flaky":
            lines.append(f'| {row["cohort"]} / {row["profile"]} | {row["success_rate"]:.1%} | {display(row["metrics"]["wall_s"])} | {display(row["metrics"]["retry_requests"])} |')
    lines += ["", "## Ratios of median wall times", "",
              "Reference / Vise. Ratios are descriptive, not statistical significance. Overlapping min–max ranges mean **no clear difference**."]
    for result in comparisons_out:
        lines.append(f'- {result["scenario"]} / {result["cohort"]}: {result["reference"]} / Vise = '
                     f'{result["reference_over_candidate_median"]:.3f}; {result["label"]}.')
    lines += ["", "## Environment", "", "```json", json.dumps(environment, indent=2), "```", "",
              "Loopback is not the internet. Client CPU excludes runner/fixture. Linux fixture CPU around the timed client interval and separate lifetime CPU (including hash preparation) are recorded per run; shared CPU/cache contention can dominate.",
              "Socket write counts exclude headers/TCP retransmissions and are not confirmed delivery counts. See the benchmarking guide for definitions.", ""]
    (output / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    return rows
