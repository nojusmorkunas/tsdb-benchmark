import time

import clickhouse_connect

from .base import DbAdapter, QueryResult


class ClickHouseAdapter(DbAdapter):
    def __init__(self, name: str, config: dict):
        self.name = name
        self._config = config

    def _client(self):
        cfg = self._config
        return clickhouse_connect.get_client(
            host=cfg["host"],
            port=cfg["port"],
            username=cfg.get("user", "default"),
            password=cfg.get("password", ""),
        )

    def query(self, sql: str, max_rows: int = 500) -> QueryResult:
        t0 = time.perf_counter()
        client = self._client()
        result = client.query(sql)
        all_rows = result.result_rows
        elapsed = (time.perf_counter() - t0) * 1000
        client.close()
        return {
            "columns": result.column_names,
            "rows": [[str(c) for c in r] for r in all_rows[:max_rows]],
            "total_rows": len(all_rows),
            "time_ms": round(elapsed, 1),
        }

    def ping(self) -> bool:
        try:
            client = self._client()
            client.query("SELECT 1")
            client.close()
            return True
        except Exception:
            return False

    def row_count(self) -> int | None:
        try:
            result = self.query("SELECT COUNT(*) FROM energy_data", max_rows=1)
            return int(result["rows"][0][0])
        except Exception:
            return None
