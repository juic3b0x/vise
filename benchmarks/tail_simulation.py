"""Deterministic tail-policy model, not a measured performance benchmark.

512 MiB, fixed worker rates, equal initial quarters (or queued 1 MiB pieces).
A shortened response consumes one configurable duplicate-byte overrun at its
worker rate before closing. Multiple boundary changes to it still close it once.
Setup, disk/journal costs, TCP reassignment and hidden buffering are assumptions;
real output hashes and server-measured waste must be checked with run.py.
"""
import json
import math

MIB = 1024 * 1024
MINIMUM = MIB
MARGIN = 256 * 1024
ALIGNMENT = 64 * 1024


def simulate(policy, speeds, setup=0.001, overrun=65536):
    if policy not in ("static", "half", "weighted", "fine") or len(speeds) != 4:
        raise ValueError("four workers and a known policy required")
    if any(not math.isfinite(speed) or speed <= 0 for speed in speeds) or setup < 0 or overrun < 0:
        raise ValueError("invalid model parameters")
    now = 0.0
    tail_start = None
    waste = splits = requests = 0
    slots = [None] * 4
    queue = [MIB] * 512 if policy == "fine" else [128 * MIB] * 4

    def assign(worker, size):
        nonlocal requests
        requests += 1
        slots[worker] = dict(remaining=float(size), start=now + setup,
                             shortened=False, duplicate=0.0)

    for worker in range(4):
        assign(worker, queue.pop(0))
    while any(slot is not None for slot in slots) or queue:
        delta = min(max(0.0, slot["start"] - now)
                    + (slot["remaining"] + slot["duplicate"]) / (speeds[i] * MIB)
                    for i, slot in enumerate(slots) if slot is not None)
        for i, slot in enumerate(slots):
            if slot is None:
                continue
            amount = max(0.0, now + delta - max(now, slot["start"])) * speeds[i] * MIB
            useful = min(slot["remaining"], amount)
            slot["remaining"] -= useful
            slot["duplicate"] = max(0.0, slot["duplicate"] - (amount - useful))
        now += delta
        done = [i for i, slot in enumerate(slots)
                if slot is not None and slot["remaining"] + slot["duplicate"] < 0.01]
        if tail_start is None:
            tail_start = now
        for i in done:
            waste += overrun if slots[i]["shortened"] else 0
            slots[i] = None
        idle = [i for i, slot in enumerate(slots) if slot is None]
        for i in idle[:]:
            if queue:
                assign(i, queue.pop(0))
                idle.remove(i)
        if policy not in ("half", "weighted"):
            continue
        for i in idle[:]:
            eligible = [j for j, slot in enumerate(slots)
                        if slot is not None and slot["remaining"] >= 2 * MINIMUM + MARGIN
                        and slot["start"] <= now]
            if not eligible:
                continue
            donor = max(eligible, key=lambda j: (slots[j]["remaining"], -j))
            slot = slots[donor]
            remaining = slot["remaining"]
            share = 0.5 if policy == "half" else speeds[i] / (speeds[donor] + sum(speeds[k] for k in idle))
            child = math.floor(remaining * share / ALIGNMENT) * ALIGNMENT
            child = max(MINIMUM, min(child, remaining - MINIMUM - MARGIN))
            slot["remaining"] -= child
            slot["shortened"] = True
            slot["duplicate"] = float(overrun)
            assign(i, child)
            idle.remove(i)
            splits += 1
    return dict(finish_s=round(now, 6), tail_s=round(now - tail_start, 6),
                splits=splits, body_requests=requests, duplicate_bytes=waste,
                duplicate_bytes_per_split=waste / splits if splits else 0)


def main():
    rows = []
    for setup in (0.001, 0.080):
        for scenario, speeds in (("heterogeneous", [8, 8, 8, 0.8]), ("uniform", [8] * 4)):
            for policy in ("static", "half", "weighted", "fine"):
                rows.append(dict(scenario=scenario, policy=policy, response_setup_s=setup,
                                 **simulate(policy, speeds, setup)))
    print(json.dumps(dict(model_only=True, overrun_per_shortened_response=65536, results=rows), indent=2))


if __name__ == "__main__":
    main()
