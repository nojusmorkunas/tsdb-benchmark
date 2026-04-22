"""Web UI for the database benchmark suite.

Provides:
  - Live query runner against all 5 databases
  - Side-by-side timing comparison
  - Async job pattern to avoid gateway timeouts
  - Script runner (generate, load, benchmark) with log streaming
  - Database status monitoring
"""
import json
import os
import secrets
import subprocess
import threading
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response

from config import DB, DATA_DIR, SEED_FILE
from auth import basic_auth_middleware
from db import ADAPTERS, get_adapter
from jobs import submit_job, get_job, cleanup_jobs, _run_query_job, _run_density_job
from queries import PRESET_QUERIES

app = FastAPI(title="DB Benchmark")
app.middleware("http")(basic_auth_middleware)

LOG_FILE = Path("/data/runner.log")
PROCESS: subprocess.Popen | None = None

_TEMPLATE = Path(__file__).parent / "templates" / "index.html"


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------
@app.get("/api/status")
def status():
    result = {name: adapter.ping() for name, adapter in ADAPTERS.items()}
    blocks = len([f for f in os.listdir(DATA_DIR) if f.startswith("block_")]) if os.path.isdir(DATA_DIR) else 0
    result["_data"] = {"seed": os.path.exists(SEED_FILE), "blocks": blocks}
    return result



@app.get("/api/presets")
def presets():
    return {name: {"sql": q["sql"]} for name, q in PRESET_QUERIES.items()}


@app.post("/api/query")
async def run_query(body: dict):
    cleanup_jobs()
    job_id = secrets.token_hex(8)
    submit_job(job_id)
    databases = body.get("databases", list(DB.keys()))
    threading.Thread(
        target=_run_query_job,
        args=(job_id, databases, body.get("query", ""), body.get("preset"), body.get("max_rows", 500), body.get("scope", "full")),
        daemon=True,
    ).start()
    return {"job_id": job_id}


@app.get("/api/query/{job_id}")
def query_status(job_id: str):
    job = get_job(job_id)
    if not job:
        return Response(status_code=404, content="Job not found")
    return job


@app.post("/api/run-script")
async def run_script(body: dict):
    global PROCESS
    script = body.get("script", "benchmark.py")
    args = body.get("args", [])
    if PROCESS and PROCESS.poll() is None:
        return {"error": "A script is already running."}
    cmd = ["python3", "-u", script] + args
    with open(LOG_FILE, "w") as lf:
        lf.write(f"[{datetime.now().isoformat()}] Starting: {' '.join(cmd)}\n")
    def _run():
        global PROCESS
        with open(LOG_FILE, "a") as lf:
            PROCESS = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, text=True)
            PROCESS.wait()
            lf.write(f"\n[{datetime.now().isoformat()}] Exit code: {PROCESS.returncode}\n")
    threading.Thread(target=_run, daemon=True).start()
    return {"status": "started"}


@app.post("/api/stop-script")
async def stop_script():
    global PROCESS
    if PROCESS and PROCESS.poll() is None:
        PROCESS.terminate(); return {"status": "terminated"}
    return {"status": "idle"}


@app.get("/api/logs")
def logs():
    text = LOG_FILE.read_text() if LOG_FILE.exists() else ""
    return {"logs": text, "running": PROCESS is not None and PROCESS.poll() is None}



def _migrate_results(data: dict) -> dict:
    """Migrate old {runs: [...]} format to {history: [...], best: {...}}."""
    if "runs" not in data and "history" not in data:
        return data
    if "history" not in data:
        data["history"] = data.pop("runs")
    data.setdefault("best", {})
    if data["best"]:
        return data
    # Rebuild best from history
    for run in data["history"]:
        key = str(run.get("n_runs", 3))
        db = run.get("database", "")
        for preset, q in run.get("queries", {}).items():
            if q.get("error") or q.get("min_ms") is None:
                continue
            data["best"].setdefault(key, {}).setdefault(db, {})
            existing = data["best"][key][db].get(preset)
            if existing is None or q["min_ms"] < existing["min_ms"]:
                data["best"][key][db][preset] = {
                    "min_ms": q["min_ms"], "median_ms": q["median_ms"],
                    "max_ms": q["max_ms"], "times_ms": q["times_ms"],
                    "rows": q["rows"], "error": None,
                    "run_id": run.get("started_at"),
                }
    return data


@app.get("/api/benchmark-results")
def benchmark_results():
    path = Path("/data/benchmark_results.json")
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    data = _migrate_results(data)
    # Persist migration so benchmark.py doesn't have to redo it
    if "runs" not in data:
        path.write_text(json.dumps(data))
    return data


@app.post("/api/benchmark-results/clear")
async def clear_benchmark_results():
    return {"status": "disabled", "error": "Clearing results is disabled to prevent data loss. Delete /data/benchmark_results.json manually if needed."}


@app.delete("/api/benchmark-results/best")
async def delete_best_cell(body: dict):
    """Remove a specific cell from the best table."""
    n_runs = body.get("n_runs")
    database = body.get("database")
    preset = body.get("preset")
    if n_runs is None or not database or not preset:
        return {"error": "n_runs, database and preset are required"}
    path = Path("/data/benchmark_results.json")
    if not path.exists():
        return {"error": "No results file"}
    data = json.loads(path.read_text())
    key = str(n_runs)
    try:
        del data["best"][key][database][preset]
        path.write_text(json.dumps(data, indent=2))
        return {"status": "ok"}
    except KeyError:
        return {"error": "Cell not found"}


@app.delete("/api/benchmark-results/history")
async def delete_history_run(body: dict):
    """Remove a specific run from history by started_at + database."""
    run_id = body.get("run_id")
    database = body.get("database")
    if not run_id or not database:
        return {"error": "run_id and database are required"}
    path = Path("/data/benchmark_results.json")
    if not path.exists():
        return {"error": "No results file"}
    data = json.loads(path.read_text())
    history = data.get("history", data.get("runs", []))
    new_history = [r for r in history if not (r.get("started_at") == run_id and r.get("database") == database)]
    if len(new_history) == len(history):
        return {"error": "Run not found"}
    data["history"] = new_history
    path.write_text(json.dumps(data, indent=2))
    return {"status": "ok"}


@app.put("/api/benchmark-results/best")
async def promote_to_best(body: dict):
    """Copy a query result from history into the best table."""
    n_runs = body.get("n_runs")
    run_id = body.get("run_id")
    preset = body.get("preset")
    if n_runs is None or not run_id or not preset:
        return {"error": "n_runs, run_id and preset are required"}
    path = Path("/data/benchmark_results.json")
    if not path.exists():
        return {"error": "No results file"}
    data = json.loads(path.read_text())
    history = data.get("history", data.get("runs", []))
    run = next((r for r in history if r.get("started_at") == run_id and r.get("n_runs") == n_runs), None)
    if not run:
        return {"error": "Run not found in history"}
    q = run.get("queries", {}).get(preset)
    if not q:
        return {"error": "Preset not found in run"}
    if q.get("error") or q.get("min_ms") is None:
        return {"error": "Cannot promote an errored or empty result"}
    key = str(n_runs)
    db = run["database"]
    data.setdefault("best", {}).setdefault(key, {}).setdefault(db, {})
    data["best"][key][db][preset] = {
        "min_ms": q["min_ms"],
        "median_ms": q["median_ms"],
        "max_ms": q["max_ms"],
        "times_ms": q["times_ms"],
        "rows": q["rows"],
        "error": None,
        "run_id": run_id,
    }
    path.write_text(json.dumps(data, indent=2))
    return {"status": "ok"}


@app.post("/api/density")
async def density(body: dict):
    days = min(max(int(body.get("days", 30)), 1), 90)
    db_name = body.get("database", "")
    cfg = DB.get(db_name)
    if not cfg:
        return {"error": "Invalid database"}
    job_id = secrets.token_hex(8)
    submit_job(job_id)
    threading.Thread(target=_run_density_job, args=(job_id, days, db_name, cfg), daemon=True).start()
    return {"job_id": job_id}


@app.get("/api/density/{job_id}")
def density_status(job_id: str):
    job = get_job(job_id)
    if not job:
        return Response(status_code=404, content="Job not found")
    return job



# ---------------------------------------------------------------------------
# Frontend
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def index():
    return _TEMPLATE.read_text()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
