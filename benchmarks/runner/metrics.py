"""Server-observed metrics use body bytes accepted by write(), not wire traffic."""
from collections import defaultdict
import statistics


def span(values):
    values = [value for value in values if value is not None]
    if not values:
        return None
    return {"median": statistics.median(values), "min": min(values), "max": max(values),
            "spread": max(values) - min(values)}


def server_metrics(stats, budget):
    requests = stats["requests"]
    by_file = defaultdict(list)
    slow_connections = set()
    slow_bytes = total = retries = 0
    intervals = defaultdict(list)
    for request in requests:
        count = request["body_bytes"]
        total += count
        if request["slow"] and count:
            slow_connections.add(request["connection"])
            slow_bytes += count
        if count:
            by_file[request["file"]].append((request["start"], request["start"] + count))
        # Exclude one-byte probes from serving occupancy and retry attribution.
        if count > 1:
            intervals[request["connection"]].append((request["first_byte_s"], request["last_byte_s"]))
    # An observed recovery request starts after a reset and intersects that failed
    # response's advertised interval. This includes non-duplicating byte resumes.
    resets = [r for r in requests if r["reset"]]
    retries = sum(r["body_bytes"] > 1 and any(
        r["file"] == failed["file"] and r["started_s"] > failed["last_byte_s"]
        and r["start"] <= failed["end"] and r["end"] >= failed["start"]
        for failed in resets) for r in requests)
    unique = 0
    for ranges in by_file.values():
        end = -1
        for start, stop in sorted(ranges):
            unique += max(0, stop - max(start, end))
            end = max(end, stop)
    events = []
    for ranges in intervals.values():
        for start, end in ranges:
            events.extend([(start, 1), (end, -1)])
    active = peak = 0
    armed = False
    tail_start = None
    for timestamp, change in sorted(events, key=lambda event: (event[0], -event[1])):
        old = active
        active += change
        peak = max(peak, active)
        armed |= active >= budget
        if armed and old >= budget > active and tail_start is None:
            tail_start = timestamp
    last = max((r["last_byte_s"] for r in requests if r["last_byte_s"] is not None and r["body_bytes"] > 1), default=None)
    tail = max(0, last - tail_start) if tail_start is not None and last is not None else None
    return {"requests": len(requests), "connections": stats["connections"],
            "peak_open_connections": stats["peak_open_connections"], "peak_serving_connections": peak,
            "head_requests": sum(r["head"] for r in requests),
            "probe_requests": sum(r["body_bytes"] == 1 for r in requests),
            "range_requests": sum(r["status"] == 206 for r in requests),
            "error_requests": sum(r["status"] >= 400 for r in requests),
            "resets": sum(r["reset"] for r in requests), "retry_requests": retries,
            "body_bytes": total, "unique_body_bytes": unique, "duplicate_body_bytes": total - unique,
            "slow_connections_with_body": len(slow_connections),
            "slow_body_share": slow_bytes / total if total else 0,
            "tail_s": tail, "tail_budget_reached": armed}


def comparison(candidate, reference):
    overlap = candidate["min"] <= reference["max"] and reference["min"] <= candidate["max"]
    return {"reference_over_candidate_median": reference["median"] / candidate["median"],
            "label": "no clear difference" if overlap else "non-overlapping observed ranges"}
