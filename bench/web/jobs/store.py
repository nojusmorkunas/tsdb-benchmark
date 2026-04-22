import time
import threading

_JOBS: dict = {}
_JOBS_LOCK = threading.Lock()


def submit_job(job_id: str) -> None:
    """Register a new job as running."""
    with _JOBS_LOCK:
        _JOBS[job_id] = {"status": "running", "ts": time.time()}


def get_job(job_id: str) -> dict | None:
    """Return a copy of the job dict, or None if not found."""
    with _JOBS_LOCK:
        return _JOBS.get(job_id)


def complete_job(job_id: str, result) -> None:
    """Mark a job as done and store its result."""
    with _JOBS_LOCK:
        if job_id in _JOBS:
            _JOBS[job_id].update({"status": "done", "result": result})


def fail_job(job_id: str, error: str) -> None:
    """Mark a job as failed with an error message."""
    with _JOBS_LOCK:
        if job_id in _JOBS:
            _JOBS[job_id].update({"status": "error", "error": error})


def update_job_progress(job_id: str, progress: dict) -> None:
    """Update in-progress status with live progress data."""
    with _JOBS_LOCK:
        if job_id in _JOBS:
            _JOBS[job_id].update({"progress": progress})


def cleanup_jobs() -> None:
    """Remove jobs older than 1 hour."""
    cutoff = time.time() - 3600
    with _JOBS_LOCK:
        stale = [k for k, v in _JOBS.items() if v.get("ts", 0) < cutoff]
        for k in stale:
            del _JOBS[k]
