"""
Timing aggregation — pure stdlib, no pandas.

compute_stats(values)         → {mean, median, p95, stdev, n}
split_cold_warm(run_records)  → (cold_stats_by_phase, warm_stats_by_phase)
"""
from __future__ import annotations

import statistics


def compute_stats(values: list[float]) -> dict:
    """
    Descriptive stats for a list of float timings (seconds).
    p95 is the value at the 95th percentile using nearest-rank.
    stdev is None when n < 2.
    """
    n = len(values)
    if n == 0:
        return {"mean": None, "median": None, "p95": None, "stdev": None, "n": 0}

    mean = statistics.mean(values)
    median = statistics.median(values)
    stdev = statistics.stdev(values) if n >= 2 else None

    sorted_v = sorted(values)
    p95_idx = max(0, int(round(0.95 * n)) - 1)
    p95 = sorted_v[p95_idx]

    return {
        "mean": round(mean, 6),
        "median": round(median, 6),
        "p95": round(p95, 6),
        "stdev": round(stdev, 6) if stdev is not None else None,
        "n": n,
    }


def split_cold_warm(
    run_records: list[dict],
) -> tuple[dict[str, dict], dict[str, dict]]:
    """
    Split run records into cold (index 0) and warm (indices 1-N).

    Each record is a dict of {phase_name: float, ...} — the 'type' key is ignored here.

    Returns:
        cold_stats  — {phase: compute_stats([single cold value])}
        warm_stats  — {phase: compute_stats([warm values])}
    """
    if not run_records:
        return {}, {}

    numeric_keys = {
        k for rec in run_records for k, v in rec.items()
        if isinstance(v, (int, float)) and k != "run"
    }

    cold_record = {k: v for k, v in run_records[0].items() if k in numeric_keys}
    warm_records = [
        {k: v for k, v in rec.items() if k in numeric_keys}
        for rec in run_records[1:]
    ]

    cold_stats = {
        phase: compute_stats([cold_record[phase]] if phase in cold_record else [])
        for phase in numeric_keys
    }
    warm_stats = {
        phase: compute_stats([r[phase] for r in warm_records if phase in r])
        for phase in numeric_keys
    }
    return cold_stats, warm_stats
