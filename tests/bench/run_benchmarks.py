"""
Benchmark runner for spatial-ETL mappers.

Usage:
    python3 tests/bench/run_benchmarks.py weather --runs 5
    python3 tests/bench/run_benchmarks.py weather tree --runs 3
    python3 tests/bench/run_benchmarks.py --all --runs 5
    python3 tests/bench/run_benchmarks.py weather --runs 0 --no-flush   # static metrics only

Flags:
    --runs N        total runs per mapper (default 5); run 0=cold, 1..N-1=warm
    --no-flush      skip TRUNCATE on cold run (use for read-only / fetch-only mappers)
    --all           run every datasource class_name found in config.yaml
    --out-dir PATH  results directory (default: tests/bench/results/)
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.bench.aggregates import split_cold_warm
from tests.bench.benchmark_wrapper import make_benchmark_class
from tests.bench.cold_flush import flush_tables
from tests.bench.mapper_loader import _strip_mapper_suffix, load_mapper
from tests.bench.static_analysis import analyze

_PHASES = [
    "extract",
    "transform",
    "load",
    "sync_raw_to_staging",
    "sync_staging_to_enrichment",
    "execute_on_staging",
    "execute_on_enrichment",
    "map_to_base",
    "total",
]

_CSV_COLUMNS = (
    ["mapper", "timestamp", "loc_total", "loc_code", "import_count", "method_count", "override_count"]
    + [f"{p}_cold_mean" for p in _PHASES]
    + [f"{p}_warm_mean" for p in _PHASES]
    + [f"{p}_warm_p95" for p in _PHASES]
    + ["total_warm_stdev"]
)


# ── helpers ───────────────────────────────────────────────────────────────────

def _mapper_path(normalised: str) -> Path:
    return ROOT / "data_mappers" / f"{normalised}Mapper.py"


def _all_class_names() -> list[str]:
    from main_core.core_config import CoreConfig
    conf = CoreConfig().get_config()
    names = []
    for ds in conf.get("datasources", []):
        cn = str(ds.get("class_name") or "").strip()
        if cn:
            names.append(_strip_mapper_suffix(cn))
    return names


def _silence_logs():
    logging.getLogger().setLevel(logging.CRITICAL)
    for name in logging.root.manager.loggerDict:
        logging.getLogger(name).setLevel(logging.CRITICAL)


def _safe_stat(stats: dict, phase: str, key: str):
    return (stats.get(phase) or {}).get(key)


# ── per-mapper benchmark ──────────────────────────────────────────────────────

def run_mapper_bench(
    class_name: str,
    n_runs: int,
    no_flush: bool,
    out_dir: Path,
) -> dict:
    normalised = _strip_mapper_suffix(class_name)
    print(f"\n{'='*60}")
    print(f"  Mapper : {normalised}")
    print(f"  Runs   : {n_runs}  (cold=1, warm={max(0, n_runs-1)})")
    print(f"  Flush  : {'no' if no_flush else 'yes (cold run)'}")
    print(f"{'='*60}")

    # static analysis (no DB needed)
    mp = _mapper_path(normalised)
    static = analyze(mp) if mp.exists() else {
        "loc_total": 0, "loc_code": 0,
        "import_count": 0, "method_count": 0, "override_count": 0,
    }
    print(f"  Static : LOC={static['loc_total']} code={static['loc_code']} "
          f"imports={static['import_count']} methods={static['method_count']} "
          f"overrides={static['override_count']}")

    if n_runs == 0:
        result = _assemble_result(normalised, static, [], {}, {})
        _write_outputs(result, out_dir)
        return result

    # load mapper (real DB connection)
    instance, dto, mapper_class, db, base_conf, metadata_svc = load_mapper(normalised)

    BenchCls = make_benchmark_class(mapper_class)
    bench = BenchCls(dto, db, None, base_conf, metadata_svc)

    run_records: list[dict] = []

    for i in range(n_runs):
        run_type = "cold" if i == 0 else "warm"
        print(f"\n  Run {i+1}/{n_runs}  [{run_type}]", end="", flush=True)

        if i == 0 and not no_flush:
            flushed = flush_tables(db, dto, bench)
            if flushed:
                print(f"  → flushed: {flushed}", end="", flush=True)

        bench._reset_timings()

        t_total_start = time.perf_counter()
        try:
            bench.run()
            error = None
        except Exception as exc:
            error = str(exc)
            print(f"  ✗ ERROR: {error}", end="", flush=True)
        t_total = time.perf_counter() - t_total_start

        timings = dict(bench._bench_timings)
        timings["total"] = t_total

        record: dict = {"run": i, "type": run_type, **timings}
        if error:
            record["error"] = error
        run_records.append(record)

        _print_run_summary(timings)

    cold_stats, warm_stats = split_cold_warm(run_records)
    result = _assemble_result(normalised, static, run_records, cold_stats, warm_stats)

    _write_outputs(result, out_dir)
    _print_aggregate_summary(normalised, cold_stats, warm_stats)
    return result


# ── output helpers ────────────────────────────────────────────────────────────

def _assemble_result(
    normalised: str,
    static: dict,
    run_records: list[dict],
    cold_stats: dict,
    warm_stats: dict,
) -> dict:
    mapper_cls_name = normalised[0].upper() + normalised[1:] + "Mapper"
    return {
        "mapper": normalised,
        "mapper_class": mapper_cls_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_runs": len(run_records),
        "transform_timing_note": "cumulative_thread_seconds",
        "static": static,
        "runs": [
            {"run": r["run"], "type": r["type"],
             "timings": {k: v for k, v in r.items() if k not in ("run", "type", "error")},
             **({"error": r["error"]} if "error" in r else {})}
            for r in run_records
        ],
        "cold_stats": cold_stats,
        "warm_stats": warm_stats,
    }


def _write_outputs(result: dict, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    json_path = out_dir / f"{result['mapper']}_{ts}.json"
    json_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(f"\n  → JSON : {json_path.relative_to(ROOT)}")

    _append_csv_row(result, out_dir)


def _append_csv_row(result: dict, out_dir: Path):
    csv_path = out_dir / "summary.csv"
    cold = result.get("cold_stats", {})
    warm = result.get("warm_stats", {})
    static = result.get("static", {})

    row = {
        "mapper": result["mapper"],
        "timestamp": result["timestamp"],
        "loc_total": static.get("loc_total"),
        "loc_code": static.get("loc_code"),
        "import_count": static.get("import_count"),
        "method_count": static.get("method_count"),
        "override_count": static.get("override_count"),
        **{f"{p}_cold_mean": _safe_stat(cold, p, "mean") for p in _PHASES},
        **{f"{p}_warm_mean": _safe_stat(warm, p, "mean") for p in _PHASES},
        **{f"{p}_warm_p95": _safe_stat(warm, p, "p95") for p in _PHASES},
        "total_warm_stdev": _safe_stat(warm, "total", "stdev"),
    }

    write_header = not csv_path.exists()
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_CSV_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    print(f"  → CSV  : {csv_path.relative_to(ROOT)}")


def _print_run_summary(timings: dict):
    parts = []
    for phase in _PHASES:
        v = timings.get(phase)
        if v is not None:
            parts.append(f"{phase}={v:.3f}s")
    print("  " + "  ".join(parts))


def _print_aggregate_summary(mapper: str, cold_stats: dict, warm_stats: dict):
    print(f"\n  Aggregates for [{mapper}]")
    print(f"  {'Phase':<32} {'cold':>10}  {'warm_mean':>10}  {'warm_p95':>10}  {'warm_stdev':>10}")
    print(f"  {'-'*32} {'-'*10}  {'-'*10}  {'-'*10}  {'-'*10}")
    for phase in _PHASES:
        c_mean = (cold_stats.get(phase) or {}).get("mean")
        w_mean = (warm_stats.get(phase) or {}).get("mean")
        w_p95 = (warm_stats.get(phase) or {}).get("p95")
        w_std = (warm_stats.get(phase) or {}).get("stdev")
        fmt = lambda v: f"{v:.4f}s" if v is not None else "     —"
        print(f"  {phase:<32} {fmt(c_mean):>10}  {fmt(w_mean):>10}  {fmt(w_p95):>10}  {fmt(w_std):>10}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args():
    p = argparse.ArgumentParser(
        description="Benchmark spatial-ETL mappers with per-phase timing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("mappers", nargs="*", metavar="MAPPER",
                   help="One or more mapper class_names (e.g. weather tree)")
    p.add_argument("--runs", type=int, default=5,
                   help="Total runs per mapper; run 0=cold, rest=warm (default 5)")
    p.add_argument("--no-flush", action="store_true",
                   help="Skip TRUNCATE on cold run")
    p.add_argument("--all", dest="run_all", action="store_true",
                   help="Run every datasource found in config.yaml")
    p.add_argument("--out-dir", default=str(ROOT / "tests" / "bench" / "results"),
                   help="Directory for JSON/CSV output")
    return p.parse_args()


def main():
    args = _parse_args()
    _silence_logs()

    out_dir = Path(args.out_dir)

    if args.run_all:
        targets = _all_class_names()
        if not targets:
            print("No datasources found in config.yaml.", file=sys.stderr)
            sys.exit(1)
    elif args.mappers:
        targets = [_strip_mapper_suffix(m) for m in args.mappers]
    else:
        print("Specify one or more mapper names, or use --all.", file=sys.stderr)
        sys.exit(1)

    print(f"Benchmark run — {len(targets)} mapper(s), {args.runs} run(s) each")

    all_results = []
    for name in targets:
        try:
            r = run_mapper_bench(name, args.runs, args.no_flush, out_dir)
            all_results.append(r)
        except Exception as exc:
            print(f"\n  [ERROR] {name}: {exc}", file=sys.stderr)

    print(f"\nDone. Results in {out_dir.relative_to(ROOT)}/")


if __name__ == "__main__":
    main()
