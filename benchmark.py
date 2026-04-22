#!/usr/bin/env python3
"""Standalone benchmark runner — all 17 preset queries, direct database connections.

Runs the same query suite as the web UI benchmark but without the web UI.
Configure databases via environment variables (see .env.example).

Usage:
    python benchmark.py --database PostgreSQL --runs 3
    python benchmark.py --database ClickHouse --runs 1
    python benchmark.py --database TimescaleDB --skip 12,13,14 --scope quarter
    python benchmark.py --list-databases

Supported databases:
    PostgreSQL, TimescaleDB, ClickHouse, QuestDB, InfluxDB 2
"""

import argparse
import json
import os
import statistics
import sys
from datetime import datetime
from pathlib import Path

# Make bench/web importable without installation
sys.path.insert(0, str(Path(__file__).parent / "bench" / "web"))

from config import DB                                      # noqa: E402
from db import get_adapter                                 # noqa: E402
from queries import PRESET_QUERIES, apply_quarter_scope    # noqa: E402

RESULTS_FILE = Path("benchmark_results.json")

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


def run_query(db_name: str, preset: str, scope: str = "full") -> dict:
    cfg = DB.get(db_name)
    if not cfg:
        return {"error": f"Unknown database: {db_name}", "time_ms": -1}

    pq = PRESET_QUERIES.get(preset)
    if pq is None:
        return {"error": f"Unknown preset: {preset}", "time_ms": -1}

    if scope == "quarter":
        pq = apply_quarter_scope(pq)

    adapter = get_adapter(db_name)
    if adapter is None:
        return {"error": "Could not connect", "time_ms": -1}

    db_type = cfg["type"]
    try:
        if db_type == "influx":
            q = pq.get("flux")
            if not q:
                return {"error": "Not supported: InfluxDB 2 does not support this query type", "time_ms": -1}
            q = adapter.resolve_placeholders(q)
        elif db_type == "influx3":
            q = pq.get("influx3")
            if not q:
                return {"error": "Not supported: InfluxDB 3 does not support this query type", "time_ms": -1}
            q = adapter.resolve_placeholders(q)
        elif db_type == "ch":
            q = pq["ch"]
        elif db_type == "pg":
            q = pq["qdb"] if db_name == "QuestDB" else pq["sql"]
            q = adapter.resolve_placeholders(q)
        else:
            return {"error": f"Unknown adapter type: {db_type}", "time_ms": -1}

        return adapter.query(q, max_rows=1000)
    except Exception as e:
        return {"error": str(e)[:400], "time_ms": -1}


def fmt(ms):
    if ms is None:
        return "—"
    return f"{ms / 1000:.2f}s" if ms >= 1000 else f"{ms:.1f}ms"


def _update_best(best: dict, run: dict):
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
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--database", help="Database to benchmark")
    ap.add_argument("--runs", type=int, default=3, help="Timed runs per query (default: 3)")
    ap.add_argument("--skip", default="", help="Query numbers to skip, e.g. --skip 12,13,14")
    ap.add_argument("--scope", default="full", choices=["full", "quarter"],
                    help="full = all data; quarter = first 3 months (~350M rows)")
    ap.add_argument("--output", default=str(RESULTS_FILE),
                    help=f"Results file (default: {RESULTS_FILE})")
    ap.add_argument("--list-databases", action="store_true", help="List configured databases and exit")
    args = ap.parse_args()

    if args.list_databases:
        print("Configured databases:")
        for name, cfg in DB.items():
            adapter = get_adapter(name)
            status = "online" if (adapter and adapter.ping()) else "offline"
            print(f"  {name:<16} [{status}]  ({cfg['type']})")
        return

    if not args.database:
        ap.error("--database is required (or use --list-databases)")

    db_name = args.database
    if db_name not in DB:
        print(f"Error: '{db_name}' is not a configured database.")
        print(f"Available: {', '.join(DB.keys())}")
        sys.exit(1)

    adapter = get_adapter(db_name)
    if not adapter or not adapter.ping():
        print(f"Error: cannot connect to {db_name}. Is the container running?")
        sys.exit(1)

    n = args.runs
    scope = args.scope
    output = Path(args.output)
    skip_nums = {int(x.strip()) for x in args.skip.split(",") if x.strip()}

    scope_label = f"  |  scope: {scope}" if scope != "full" else ""
    print(f"{'=' * 60}", flush=True)
    print(f"Benchmark: {db_name}  |  {n} run(s) per query{scope_label}", flush=True)
    print(f"Started:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"{'=' * 60}", flush=True)

    run = {
        "database": db_name,
        "n_runs": n,
        "scope": scope,
        "started_at": datetime.now().isoformat(),
        "completed_at": None,
        "queries": {},
    }

    for query_num, (tier, preset) in enumerate(QUERIES, 1):
        if query_num in skip_nums:
            print(f"\n[Tier {tier}] {preset}  →  SKIPPED", flush=True)
            run["queries"][preset] = {
                "tier": tier, "times_ms": None, "min_ms": None,
                "median_ms": None, "max_ms": None, "rows": None,
                "error": f"skipped (--skip {query_num})",
            }
            continue

        print(f"\n[Tier {tier}] {preset}", flush=True)
        times, error, rows = [], None, None

        for i in range(n):
            print(f"  run {i + 1}/{n} ... ", end="", flush=True)
            res = run_query(db_name, preset, scope)
            if res.get("error"):
                error = res["error"]
                print(f"ERROR: {error}", flush=True)
                break
            t = res.get("time_ms", -1)
            rows = res.get("total_rows", 0)
            times.append(t)
            print(f"{fmt(t)}  ({rows} rows)", flush=True)

        run["queries"][preset] = {
            "tier": tier,
            "times_ms": times or None,
            "min_ms": round(min(times), 1) if times else None,
            "median_ms": round(statistics.median(times), 1) if times else None,
            "max_ms": round(max(times), 1) if times else None,
            "rows": rows,
            "error": error,
        }

    run["completed_at"] = datetime.now().isoformat()

    # Persist results
    try:
        data = json.loads(output.read_text()) if output.exists() else {}
    except Exception:
        data = {}
    data.setdefault("history", [])
    data.setdefault("best", {})
    data["history"].append(run)
    _update_best(data["best"], run)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(data, indent=2))

    # Individual backup
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = output.parent / "runs" / f"{ts}_{db_name.replace(' ', '_')}.json"
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.write_text(json.dumps(run, indent=2))

    # Summary
    ok  = sum(1 for q in run["queries"].values() if not q["error"])
    err = sum(1 for q in run["queries"].values() if q["error"])
    print(f"\n{'=' * 60}", flush=True)
    print(f"Done: {ok} ok, {err} error/skipped  →  {output}", flush=True)
    print(f"\nSummary ({db_name}{', scope=' + scope if scope != 'full' else ''}):", flush=True)
    for _, (tier, preset) in enumerate(QUERIES, 1):
        q = run["queries"][preset]
        if q["error"] and q["error"].startswith("skipped"):
            status = "SKIPPED"
        elif q["error"]:
            status = f"ERROR  ({q['error'][:60]})"
        else:
            status = f"{fmt(q['min_ms'])} min / {fmt(q['median_ms'])} median"
        print(f"  [T{tier}] {preset:<50} {status}", flush=True)


if __name__ == "__main__":
    main()
