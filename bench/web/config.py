"""Centralized config — reads from environment variables set by docker-compose."""
import os

try:
    from pydantic_settings import BaseSettings
    from pydantic import Field

    class _Settings(BaseSettings):
        DATA_DIR: str = Field(default="/data")
        PG_HOST: str = Field(default="postgres")
        PG_PORT: int = Field(default=5432)
        PG_PASSWORD: str = Field(default="")
        TS_HOST: str = Field(default="timescaledb")
        TS_PORT: int = Field(default=5432)
        TS_PASSWORD: str = Field(default="")
        CH_HOST: str = Field(default="clickhouse")
        CH_PORT: int = Field(default=8123)
        CH_USER: str = Field(default="default")
        CH_PASSWORD: str = Field(default="")
        QDB_HOST: str = Field(default="questdb")
        QDB_PORT: int = Field(default=8812)
        QDB_PASSWORD: str = Field(default="")
        INFLUX_URL: str = Field(default="http://influxdb:8086")
        INFLUX_TOKEN: str = Field(default="")
        INFLUX_ORG: str = Field(default="bench")
        INFLUX3_URL: str = Field(default="http://influxdb3:8181")
        INFLUX3_DATABASE: str = Field(default="energy")
        INFLUX3_TOKEN: str = Field(default="")

    _s = _Settings()
    DATA_DIR = _s.DATA_DIR
    DATA_GLOB = f"{DATA_DIR}/*.parquet"
    SEED_FILE = f"{DATA_DIR}/seed.parquet"

    DB = {
        "PostgreSQL": {
            "type": "pg",
            "host": _s.PG_HOST,
            "port": _s.PG_PORT,
            "dbname": "energy",
            "user": "postgres",
            "password": _s.PG_PASSWORD,
        },
        "TimescaleDB": {
            "type": "pg",
            "host": _s.TS_HOST,
            "port": _s.TS_PORT,
            "dbname": "energy",
            "user": "postgres",
            "password": _s.TS_PASSWORD,
        },
        "ClickHouse": {
            "type": "ch",
            "host": _s.CH_HOST,
            "port": _s.CH_PORT,
            "user": _s.CH_USER,
            "password": _s.CH_PASSWORD,
        },
        "QuestDB": {
            "type": "pg",
            "host": _s.QDB_HOST,
            "port": _s.QDB_PORT,
            "dbname": "qdb",
            "user": "admin",
            "password": _s.QDB_PASSWORD,
        },
        "InfluxDB 2": {
            "type": "influx",
            "url": _s.INFLUX_URL,
            "token": _s.INFLUX_TOKEN,
            "org": _s.INFLUX_ORG,
        },
        # InfluxDB 3 is disabled — experienced persistent OOM crashes at scale.
        # Uncomment to re-enable (requires a valid Enterprise license).
        # "InfluxDB 3": {
        #     "type": "influx3",
        #     "url": _s.INFLUX3_URL,
        #     "database": _s.INFLUX3_DATABASE,
        #     "token": _s.INFLUX3_TOKEN,
        # },
    }

except ImportError:
    # Fallback: pydantic-settings not installed, use plain os.environ
    DATA_DIR = os.environ.get("DATA_DIR", "/data")
    DATA_GLOB = f"{DATA_DIR}/*.parquet"
    SEED_FILE = f"{DATA_DIR}/seed.parquet"

    DB = {
        "PostgreSQL": {
            "type": "pg",
            "host": os.environ.get("PG_HOST", "postgres"),
            "port": int(os.environ.get("PG_PORT", 5432)),
            "dbname": "energy",
            "user": "postgres",
            "password": os.environ.get("PG_PASSWORD", ""),
        },
        "TimescaleDB": {
            "type": "pg",
            "host": os.environ.get("TS_HOST", "timescaledb"),
            "port": int(os.environ.get("TS_PORT", 5432)),
            "dbname": "energy",
            "user": "postgres",
            "password": os.environ.get("TS_PASSWORD", ""),
        },
        "ClickHouse": {
            "type": "ch",
            "host": os.environ.get("CH_HOST", "clickhouse"),
            "port": int(os.environ.get("CH_PORT", 8123)),
            "user": os.environ.get("CH_USER", "default"),
            "password": os.environ.get("CH_PASSWORD", ""),
        },
        "QuestDB": {
            "type": "pg",
            "host": os.environ.get("QDB_HOST", "questdb"),
            "port": int(os.environ.get("QDB_PORT", 8812)),
            "dbname": "qdb",
            "user": "admin",
            "password": os.environ.get("QDB_PASSWORD", ""),
        },
        "InfluxDB 2": {
            "type": "influx",
            "url": os.environ.get("INFLUX_URL", "http://influxdb:8086"),
            "token": os.environ.get("INFLUX_TOKEN", ""),
            "org": os.environ.get("INFLUX_ORG", "bench"),
        },
        # InfluxDB 3 is disabled — experienced persistent OOM crashes at scale.
        # Uncomment to re-enable (requires a valid Enterprise license).
        # "InfluxDB 3": {
        #     "type": "influx3",
        #     "url": os.environ.get("INFLUX3_URL", "http://influxdb3:8181"),
        #     "database": os.environ.get("INFLUX3_DATABASE", "energy"),
        #     "token": os.environ.get("INFLUX3_TOKEN", ""),
        # },
    }
