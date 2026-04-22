import time
import calendar
from datetime import timedelta

import psycopg2

from .base import DbAdapter, QueryResult

# Cache for QuestDB T1 placeholder values (fetched once on first use)
_QDB_META: dict = {}


class PostgresAdapter(DbAdapter):
    def __init__(self, name: str, config: dict):
        self.name = name
        self._config = config

    def _connect(self, timeout=10):
        cfg = self._config
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
            user=cfg["user"], password=cfg["password"], connect_timeout=timeout,
        )
        conn.autocommit = True
        return conn

    def query(self, sql: str, max_rows: int = 500) -> QueryResult:
        t0 = time.perf_counter()
        conn = self._connect()
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(max_rows)
        total = cur.rowcount if cur.rowcount >= 0 else len(rows)
        elapsed = (time.perf_counter() - t0) * 1000
        cur.close()
        conn.close()
        return {
            "columns": cols,
            "rows": [[str(c) for c in r] for r in rows],
            "total_rows": total,
            "time_ms": round(elapsed, 1),
        }

    def ping(self) -> bool:
        try:
            conn = self._connect(timeout=3)
            conn.cursor().execute("SELECT 1")
            conn.close()
            return True
        except Exception:
            return False

    def row_count(self) -> int | None:
        try:
            result = self.query("SELECT COUNT(*) FROM energy_data", max_rows=1)
            return int(result["rows"][0][0])
        except Exception:
            return None

    def resolve_placeholders(self, query_text: str) -> str:
        """For QuestDB: pre-fetch min(ean) and boundary timestamps, substitute placeholders."""
        if self.name != "QuestDB":
            return query_text
        if not any(p in query_text for p in ("{QDB_EAN}", "{QDB_START}", "{QDB_END_3M}")):
            return query_text
        if not _QDB_META:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(
                "SELECT min(ean), min(timestamp), "
                "dateadd('d', 1, min(timestamp)), dateadd('M', 1, min(timestamp)), dateadd('y', 1, min(timestamp)), dateadd('M', 3, min(timestamp)) "
                "FROM energy_data"
            )
            ean, ts, end_1d, end_1m, end_1y, end_3m = cur.fetchone()
            cur.close()
            conn.close()

            def _fmt(dt):
                return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"

            _QDB_META.update({
                "ean": str(ean),
                "start": _fmt(ts),
                "end_1d": _fmt(end_1d),
                "end_1m": _fmt(end_1m),
                "end_1y": _fmt(end_1y),
                "end_3m": _fmt(end_3m),
            })
        return (query_text
            .replace("{QDB_EAN}", _QDB_META["ean"])
            .replace("{QDB_START}", _QDB_META["start"])
            .replace("{QDB_END_1D}", _QDB_META["end_1d"])
            .replace("{QDB_END_1M}", _QDB_META["end_1m"])
            .replace("{QDB_END_1Y}", _QDB_META["end_1y"])
            .replace("{QDB_END_3M}", _QDB_META["end_3m"])
        )
