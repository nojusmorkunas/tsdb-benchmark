from config import DB
from .postgres import PostgresAdapter
from .clickhouse import ClickHouseAdapter
from .influx2 import InfluxDB2Adapter
from .influx3 import InfluxDB3Adapter
from .base import DbAdapter

ADAPTERS: dict[str, DbAdapter] = {}

for _name, _cfg in DB.items():
    if _cfg["type"] == "influx3":
        ADAPTERS[_name] = InfluxDB3Adapter(name=_name, config=_cfg)
    elif _cfg["type"] == "pg":
        ADAPTERS[_name] = PostgresAdapter(name=_name, config=_cfg)
    elif _cfg["type"] == "ch":
        ADAPTERS[_name] = ClickHouseAdapter(name=_name, config=_cfg)
    elif _cfg["type"] == "influx":
        ADAPTERS[_name] = InfluxDB2Adapter(name=_name, config=_cfg)


def get_adapter(db_name: str) -> DbAdapter | None:
    return ADAPTERS.get(db_name)
