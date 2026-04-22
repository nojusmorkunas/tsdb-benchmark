#!/usr/bin/env python3
"""Automated benchmark runner — single-database, all 14 preset queries.

Usage:
    python3 benchmark.py --database PostgreSQL --runs 3
    python3 benchmark.py --database ClickHouse --runs 1          # validate only
    python3 benchmark.py --database TimescaleDB --skip 4,6,7,8,9,10,11  # skip OOM queries
    python3 benchmark.py --database TimescaleDB --scope quarter  # 3-month scope, no OOM
"""
import argparse
import json
import statistics
import sys
from datetime import datetime
from pathlib import Path

import os

import time

import requests

RESULTS_FILE = Path("/data/benchmark_results.json")
API_BASE = "http://localhost:8000"
AUTH = (os.environ.get("APP_USER", "admin"), os.environ.get("APP_PASS", ""))

# (tier, preset_name)  — must match PRESET_QUERIES keys in app.py exactly
QUERIES = [
    (1, "Single meter: 1 day hourly"),
    (1, "Single meter: 1 month daily"),
    (1, "Single meter: 1 year monthly"),
    (2, "Monthly E17 vs E18 balance"),
    (2, "Hourly aggregation (first 3 months)"),
    (2, "Peak consumption hours"),
    (2, "Daily aggregation by direction"),
    (3, "Total consumption per meter (top 20)"),
    (3, "Net energy balance per meter"),
    (3, "Prosumer detection (ratio)"),
    (3, "Active meters per day"),
    (4, "Hierarchy: supplier total daily"),
    (4, "Hierarchy: by category (PRF/SMA) daily"),
    (4, "Hierarchy: sub-category monthly (PRF only)"),
    (5, "Hierarchy: supplier total (all time)"),
    (5, "Hierarchy: by category all time (PRF/SMA)"),
    (5, "Hierarchy: sub-category all time (PRF only)"),
]


def run_query(db, preset, scope="full"):
    r = requests.post(
        f"{API_BASE}/api/query",
        json={"databases": [db], "preset": preset, "max_rows": 1000, "scope": scope},
        auth=AUTH,
        timeout=30,
    )
    r.raise_for_status()
    job_id = r.json()["job_id"]
    while True:
        time.sleep(1)
        poll = requests.get(f"{API_BASE}/api/query/{job_id}", auth=AUTH, timeout=30)
        poll.raise_for_status()
        data = poll.json()
        if data["status"] == "done":
            return data["result"].get(db, {})
        if data["status"] == "error":
            raise RuntimeError(data.get("error", "Query failed"))


def fmt(ms):
    if ms is None:
        return "—"
    return f"{ms/1000:.2f}s" if ms >= 1000 else f"{ms:.1f}ms"


def _update_best(best, run):
    """Update the best table with results from a run (in-place)."""
    key = str(run["n_runs"])
    db = run["database"]
    for preset, q in run.get("queries", {}).items():
        if q.get("error") or q.get("min_ms") is None:
            continue
        best.setdefault(key, {}).setdefault(db, {})
        existing = best[key][db].get(preset)
        if existing is None or q["min_ms"] < existing["min_ms"]:
            best[key][db][preset] = {
                "min_ms": q["min_ms"],
                "median_ms": q["median_ms"],
                "max_ms": q["max_ms"],
                "times_ms": q["times_ms"],
                "rows": q["rows"],
                "error": None,
                "run_id": run["started_at"],
            }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--database", required=True, help="Database name as shown in the UI")
    ap.add_argument("--runs", type=int, default=3, help="Number of timed runs per query")
    ap.add_argument("--skip", default="", help="Comma-separated query numbers to skip, e.g. --skip 4 or --skip 4,5,11")
    ap.add_argument("--scope", default="full", choices=["full", "quarter"],
        help="Dataset scope: full (all data) or quarter (first 3 months, ~350M rows)")
    args = ap.parse_args()
    db, n, scope = args.database, args.runs, args.scope
    skip_nums = {int(x.strip()) for x in args.skip.split(",") if x.strip()}

    scope_label = f"  |  scope: {scope}" if scope != "full" else ""
    print(f"{'='*60}", flush=True)
    print(f"Benchmark: {db}  |  {n} run(s) per query{scope_label}", flush=True)
    print(f"Started:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"{'='*60}", flush=True)

    run = {
        "id": datetime.now().isoformat(),
        "database": db,
        "n_runs": n,
        "scope": scope,
        "started_at": datetime.now().isoformat(),
        "completed_at": None,
        "queries": {},
    }

    for query_num, (tier, preset) in enumerate(QUERIES, 1):
        if query_num in skip_nums:
            print(f"\n[Tier {tier}] {preset}  →  SKIPPED (--skip {query_num})", flush=True)
            run["queries"][preset] = {
                "tier": tier, "times_ms": None, "min_ms": None,
                "median_ms": None, "max_ms": None, "rows": None,
                "error": f"skipped (--skip {query_num})",
            }
            continue

        print(f"\n[Tier {tier}] {preset}", flush=True)
        times, error, rows = [], None, None

        for i in range(n):
            label = f"  run {i+1}/{n}"
            print(f"{label} ... ", end="", flush=True)
            try:
                res = run_query(db, preset, scope)
                if res.get("error"):
                    error = res["error"][:400]
                    print(f"ERROR: {error}", flush=True)
                    break
                t = res.get("time_ms", -1)
                rows = res.get("total_rows", 0)
                times.append(t)
                print(f"{fmt(t)}  ({rows} rows)", flush=True)
            except Exception as e:
                error = str(e)[:400]
                print(f"EXCEPTION: {error}", flush=True)
                break

        run["queries"][preset] = {
            "tier": tier,
            "times_ms": times if times else None,
            "min_ms": round(min(times), 1) if times else None,
            "median_ms": round(statistics.median(times), 1) if times else None,
            "max_ms": round(max(times), 1) if times else None,
            "rows": rows,
            "error": error,
        }

    run["completed_at"] = datetime.now().isoformat()

    # Load existing results
    try:
        data = json.loads(RESULTS_FILE.read_text()) if RESULTS_FILE.exists() else {}
    except Exception:
        data = {}

    # Migrate old format: "runs" key -> "history" + rebuild "best"
    if "runs" in data and "history" not in data:
        data["history"] = data.pop("runs")
        data.setdefault("best", {})
        for old_run in data["history"]:
            _update_best(data["best"], old_run)

    data.setdefault("history", [])
    data.setdefault("best", {})
    data["history"].append(run)

    # Update best table
    _update_best(data["best"], run)

    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_FILE.write_text(json.dumps(data, indent=2))

    # Also write individual backup file — never lost even if main file is cleared
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_db = db.replace(" ", "_")
    backup = RESULTS_FILE.parent / "runs" / f"{ts}_{safe_db}.json"
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.write_text(json.dumps(run, indent=2))

    # Summary
    ok  = sum(1 for q in run["queries"].values() if not q["error"])
    err = sum(1 for q in run["queries"].values() if q["error"])
    print(f"\n{'='*60}", flush=True)
    print(f"Done: {ok} ok, {err} error/unsupported  →  {RESULTS_FILE}", flush=True)
    print(f"\nSummary ({db}{', scope='+scope if scope != 'full' else ''}):", flush=True)
    for query_num, (tier, preset) in enumerate(QUERIES, 1):
        q = run["queries"][preset]
        if q["error"] and q["error"].startswith("skipped"):
            status = "SKIPPED"
        elif q["error"]:
            status = "ERROR"
        else:
            status = f"{fmt(q['min_ms'])} min / {fmt(q['median_ms'])} median"
        print(f"  [T{tier}] #{query_num:<2} {preset:<48} {status}", flush=True)



if __name__ == "__main__":
    main()
