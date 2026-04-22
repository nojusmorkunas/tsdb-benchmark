import time
import calendar
from datetime import timedelta

from influxdb_client import InfluxDBClient

from .base import DbAdapter, QueryResult

# Cache for InfluxDB 2 T1 placeholder values (fetched once on first use)
_INFLUX_META: dict = {}


class InfluxDB2Adapter(DbAdapter):
    def __init__(self, name: str, config: dict):
        self.name = name
        self._config = config

    def _client(self, timeout=86_400_000):
        cfg = self._config
        return InfluxDBClient(url=cfg["url"], token=cfg["token"], org=cfg["org"], timeout=timeout)

    def query(self, flux_query: str, max_rows: int = 500) -> QueryResult:
        t0 = time.perf_counter()
        client = self._client()
        tables = client.query_api().query(flux_query)
        FLUX_META = {'_start', '_stop', '_field', '_measurement', 'result', 'table'}
        rows, all_keys = [], set()
        for table in tables:
            for rec in table.records:
                all_keys.update(rec.values.keys())
                rows.append(rec.values)
        elapsed = (time.perf_counter() - t0) * 1000
        client.close()
        data_keys = {k for k in all_keys if k not in FLUX_META}
        time_key = '_time' if '_time' in data_keys else None
        val_key = '_value' if '_value' in data_keys else None
        tag_keys = sorted(k for k in data_keys if k not in ('_time', '_value'))
        ordered = ([time_key] if time_key else []) + tag_keys + ([val_key] if val_key else [])
        display = ['time' if k == '_time' else ('total' if k == '_value' else k) for k in ordered]
        return {
            "columns": display,
            "rows": [[str(r.get(k, "")) for k in ordered] for r in rows[:max_rows]],
            "total_rows": len(rows),
            "time_ms": round(elapsed, 1),
        }

    def ping(self) -> bool:
        try:
            client = self._client(timeout=5000)
            result = client.health()
            client.close()
            return result.status in ("pass", "ok")
        except Exception:
            return False

    def row_count(self) -> int | None:
        # Full count is too slow for large datasets; return None
        return None

    def resolve_placeholders(self, query_text: str) -> str:
        """Pre-fetch min(ean) and boundary timestamps from InfluxDB 2, substitute placeholders."""
        if not any(p in query_text for p in ("{INFLUX_EAN}", "{INFLUX_START}")):
            return query_text
        if not _INFLUX_META:
            cfg = self._config
            client = InfluxDBClient(url=cfg["url"], token=cfg["token"], org=cfg["org"], timeout=30_000)
            api = client.query_api()
            # Min EAN from tag index — fast, no data scan
            ean_tables = api.query(
                'import "influxdata/influxdb/schema"\n'
                'schema.tagValues(bucket: "energy", tag: "ean") |> sort() |> limit(n: 1)'
            )
            min_ean = ean_tables[0].records[0]["_value"]
            # Min timestamp — first() seeks directly to earliest point
            ts_tables = api.query(
                'from(bucket: "energy") |> range(start: 0) |> first() |> keep(columns: ["_time"])'
            )
            min_ts = ts_tables[0].records[0]["_time"]
            client.close()

            def _add_months(dt, n):
                m = dt.month - 1 + n
                year = dt.year + m // 12
                month = m % 12 + 1
                day = min(dt.day, calendar.monthrange(year, month)[1])
                return dt.replace(year=year, month=month, day=day)

            def _fmt(dt):
                return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            _INFLUX_META.update({
                "ean": min_ean,
                "start": _fmt(min_ts),
                "end_1d": _fmt(min_ts + timedelta(days=1)),
                "end_1m": _fmt(_add_months(min_ts, 1)),
                "end_3m": _fmt(_add_months(min_ts, 3)),
                "end_1y": _fmt(_add_months(min_ts, 12)),
            })
        return (query_text
            .replace("{INFLUX_EAN}", _INFLUX_META["ean"])
            .replace("{INFLUX_START}", _INFLUX_META["start"])
            .replace("{INFLUX_END_1D}", _INFLUX_META["end_1d"])
            .replace("{INFLUX_END_1M}", _INFLUX_META["end_1m"])
            .replace("{INFLUX_END_3M}", _INFLUX_META["end_3m"])
            .replace("{INFLUX_END_1Y}", _INFLUX_META["end_1y"])
        )
