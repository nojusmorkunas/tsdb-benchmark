from config import DB
from db import get_adapter
from queries import PRESET_QUERIES, apply_quarter_scope
from jobs.store import complete_job


def _run_query_job(job_id: str, databases, custom_query, preset, max_rows, scope="full"):
    results = {}
    for db_name in databases:
        cfg = DB.get(db_name)
        if not cfg:
            continue
        try:
            pq = PRESET_QUERIES.get(preset) if preset and preset in PRESET_QUERIES else None
            if pq and scope == "quarter":
                pq = apply_quarter_scope(pq)

            if cfg["type"] == "influx":
                adapter = get_adapter(db_name)
                q = pq.get("flux") if pq else custom_query
                if not q:
                    results[db_name] = {"error": "Not supported: InfluxDB does not support JOIN operations. Hierarchical aggregation requires pre-computed tags at ingestion time.", "time_ms": -1}
                    continue
                if adapter:
                    q = adapter.resolve_placeholders(q)
                    results[db_name] = adapter.query(q, max_rows)
                else:
                    results[db_name] = {"error": "No adapter available for InfluxDB 2", "time_ms": -1}
            elif cfg["type"] == "influx3":
                q = pq.get("influx3") if pq else custom_query
                if not q:
                    results[db_name] = {"error": "Not supported for InfluxDB 3 (no hierarchy table loaded).", "time_ms": -1}
                    continue
                adapter = get_adapter(db_name)
                if not adapter:
                    results[db_name] = {"error": "No adapter available for InfluxDB 3", "time_ms": -1}
                    continue
                q = adapter.resolve_placeholders(q)
                results[db_name] = adapter.query(q, max_rows)
            elif cfg["type"] == "ch":
                adapter = get_adapter(db_name)
                if not adapter:
                    results[db_name] = {"error": "No adapter available", "time_ms": -1}
                    continue
                q = pq["ch"] if pq else custom_query
                results[db_name] = adapter.query(q, max_rows)
            elif cfg["type"] == "pg":
                adapter = get_adapter(db_name)
                if not adapter:
                    results[db_name] = {"error": "No adapter available", "time_ms": -1}
                    continue
                if pq:
                    q = pq["qdb"] if db_name == "QuestDB" else pq["sql"]
                else:
                    q = custom_query
                q = adapter.resolve_placeholders(q)
                results[db_name] = adapter.query(q, max_rows)
        except Exception as e:
            results[db_name] = {"error": str(e), "time_ms": -1}
    complete_job(job_id, results)



def _run_density_job(job_id: str, days, db_name, cfg):
    adapter = get_adapter(db_name)
    try:
        if cfg["type"] == "ch":
            sql = f"SELECT toStartOfDay(ts) AS day, direction, SUM(value) AS total FROM energy_data WHERE ts >= (SELECT MAX(ts) FROM energy_data) - INTERVAL {days} DAY GROUP BY 1, 2 ORDER BY 1, 2"
            result = adapter.query(sql, max_rows=days * 2 + 10)
        elif cfg["type"] == "influx":
            flux = f'from(bucket: "energy") |> range(start: 0) |> aggregateWindow(every: 1d, fn: sum, createEmpty: false) |> group(columns: ["direction", "_time"]) |> sum() |> group() |> sort(columns: ["_time"], desc: true) |> limit(n: {days * 2}) |> sort(columns: ["_time"]) |> keep(columns: ["_time", "direction", "_value"])'
            result = adapter.query(flux, max_rows=days * 2 + 10)
        elif cfg["type"] == "influx3":
            sql = f"SELECT date_trunc('day', time) AS day, direction, SUM(value) AS total FROM energy WHERE time >= (SELECT MAX(time) FROM energy) - INTERVAL '{days} days' GROUP BY 1, 2 ORDER BY 1, 2"
            result = adapter.query(sql, max_rows=days * 2 + 10)
        elif cfg["type"] == "pg":
            if db_name == "QuestDB":
                sql = f"SELECT ts, direction, SUM(value) AS total FROM energy_data WHERE ts >= dateadd('d', -{days}, (SELECT max(ts) FROM energy_data)) SAMPLE BY 1d ALIGN TO CALENDAR ORDER BY ts"
            else:
                sql = f"SELECT date_trunc('day', ts) AS day, direction, SUM(value) AS total FROM energy_data WHERE ts >= (SELECT MAX(ts) FROM energy_data) - INTERVAL '{days} days' GROUP BY 1, 2 ORDER BY 1, 2"
            result = adapter.query(sql, max_rows=days * 2 + 10)
        else:
            result = {"error": "Unsupported DB type"}
    except Exception as e:
        result = {"error": str(e), "time_ms": -1}
    complete_job(job_id, result)
