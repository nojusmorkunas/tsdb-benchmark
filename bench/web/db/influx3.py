import time

import influxdb_client_3 as influxdb3

from .base import DbAdapter, QueryResult

_INFLUX3_META: dict = {}


class InfluxDB3Adapter(DbAdapter):
    def __init__(self, name: str, config: dict):
        self.name = name
        self._config = config

    def _client(self):
        cfg = self._config
        return influxdb3.InfluxDBClient3(
            host=cfg["url"],
            database=cfg.get("database", "energy"),
            token=cfg.get("token", ""),
        )

    def query(self, sql: str, max_rows: int = 500) -> QueryResult:
        t0 = time.perf_counter()
        client = self._client()
        table = client.query(sql)
        elapsed = (time.perf_counter() - t0) * 1000
        df = table.to_pandas()
        columns = list(df.columns)
        rows = [[str(v) for v in row] for row in df.values.tolist()]
        return {
            "columns": columns,
            "rows": rows[:max_rows],
            "total_rows": len(rows),
            "time_ms": round(elapsed, 1),
        }

    def resolve_placeholders(self, query_text: str) -> str:
        if not any(p in query_text for p in ("{INFLUX3_EAN}", "{INFLUX3_START}")):
            return query_text
        if not _INFLUX3_META:
            client = self._client()
            result = client.query(
                "SELECT MIN(ean) AS min_ean, MIN(time) AS min_time,"
                " MIN(time) + INTERVAL '1 day' AS end_1d,"
                " MIN(time) + INTERVAL '1 month' AS end_1m,"
                " MIN(time) + INTERVAL '1 year' AS end_1y"
                " FROM energy"
            ).to_pandas()
            row = result.iloc[0]

            def _fmt(dt):
                return dt.strftime("%Y-%m-%dT%H:%M:%S") + "Z"

            _INFLUX3_META.update({
                "ean": str(row["min_ean"]),
                "start": _fmt(row["min_time"]),
                "end_1d": _fmt(row["end_1d"]),
                "end_1m": _fmt(row["end_1m"]),
                "end_1y": _fmt(row["end_1y"]),
            })
        return (query_text
            .replace("{INFLUX3_EAN}", _INFLUX3_META["ean"])
            .replace("{INFLUX3_START}", _INFLUX3_META["start"])
            .replace("{INFLUX3_END_1D}", _INFLUX3_META["end_1d"])
            .replace("{INFLUX3_END_1M}", _INFLUX3_META["end_1m"])
            .replace("{INFLUX3_END_1Y}", _INFLUX3_META["end_1y"])
        )

    def ping(self) -> bool:
        try:
            client = self._client()
            client.query("SELECT 1")
            return True
        except Exception:
            return False

    def row_count(self) -> int | None:
        try:
            result = self.query("SELECT COUNT(*) AS n FROM energy", max_rows=1)
            if result["rows"]:
                return int(result["rows"][0][0])
        except Exception:
            pass
        return None
