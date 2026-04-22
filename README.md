# tsdb-benchmark

A self-hosted benchmarking suite for comparing time-series databases on large-scale energy meter data. Supports PostgreSQL, TimescaleDB, ClickHouse, QuestDB and InfluxDB 2 with a browser-based query runner and benchmark dashboard.

## What's included

| Component | Description |
|-----------|-------------|
| `bench/web/` | FastAPI web UI - query runner, data generator, benchmark dashboard |
| `bench/postgres/` | PostgreSQL 16 |
| `bench/timescaledb/` | TimescaleDB (PostgreSQL 16 extension) |
| `bench/clickhouse/` | ClickHouse |
| `bench/questdb/` | QuestDB |
| `bench/influxdb/` | InfluxDB 2 |

> **InfluxDB 3 note:** the `bench/influxdb3/` stack and all supporting code are present but disabled by default. During testing it ran successfully once, then consistently crashed with OOM errors on subsequent runs. Enable it at your own risk by uncommenting the `InfluxDB 3` entry in `bench/web/config.py` and the corresponding vars in `.env`.

Each database stack is independent. **For accurate results, benchmark one database at a time** - start only the database you are currently testing alongside the web UI.

## Prerequisites

- Docker + Docker Compose
- Python 3.10+ with `pandas`, `pyarrow`, `numpy` (for data generation)
- Disk space per database (see storage table below)
- **16 GB RAM or more recommended** - each database container is allocated 20 GB by default (configurable via `MEM_LIMIT` in `.env`). Less than 16 GB may cause OOM kills during heavy queries.

## Quickstart

### 1. Configure credentials

```bash
cp .env.example .env
# Edit .env and fill in passwords/tokens
```

### 2. Start the web UI

```bash
cd bench/web
docker compose --env-file ../../.env up -d --build
```

This also creates the shared `benchnet` Docker network that database containers join.

### 3. Start the database you want to benchmark

```bash
# Pick one at a time for accurate results:
cd bench/postgres      && docker compose --env-file ../../.env up -d
# cd bench/timescaledb && docker compose --env-file ../../.env up -d
# cd bench/clickhouse  && docker compose --env-file ../../.env up -d
# cd bench/questdb     && docker compose --env-file ../../.env up -d
# cd bench/influxdb    && docker compose --env-file ../../.env up -d
```

### 4. Generate synthetic data

```bash
pip install pandas pyarrow numpy
python generate_synthetic_data.py
# Outputs: transformedData.parquet (~29M rows, 15-day window)
```

Then copy the seed file into the web container:

```bash
docker cp transformedData.parquet $(docker ps -qf name=web):/data/seed.parquet
```

### 5. Open the UI and prepare the database

Navigate to `http://localhost:8400` and log in with the credentials you set in `.env`.

Use the **Setup** tab and follow these steps **in order**:

**Step 1 - Generate Data**

Expands the 15-day seed parquet into a full ~1.4B row dataset by replaying it 48 times with shifted timestamps (~2 years of data). Takes 45-90 min on modest hardware. This only needs to be done once - the generated files are reused for every database you benchmark.

**Step 2 - Load**

Pushes the generated dataset into the currently running database. Each database has its own schema and bulk-insert strategy. This step can take several hours depending on the database. Data is persisted in a Docker volume, so **you only need to load each database once** - stopping and restarting the container does not lose the data.

**Step 3 - Load Hierarchy**

Generates and loads a `meter_hierarchy` table that maps each EAN (meter) to a supplier, category (PRF/SMA) and sub-category. About 10% of meters simulate a real-world supplier switch at the dataset midpoint, giving the table a time-valid structure (`valid_from` / `valid_to`).

The hierarchy is required for the **T4 and T5 query tiers** - these join energy readings against the hierarchy to produce aggregations like "total consumption per supplier per day" or "breakdown by category across all time". Without the hierarchy loaded, T4/T5 queries will fail.

**Step 4 - Run Benchmark**

Executes the full preset query suite and records results. The benchmark tab shows min/median/max timing per query and persists results across runs so you can compare databases side by side.

Repeat steps 3-4 (start DB -> load -> load hierarchy -> run benchmark) for each database you want to compare.

### Running the benchmark without the web UI

The standalone `benchmark.py` at the repo root runs the same query suite directly against the database - no web UI or HTTP layer involved. It requires the same environment variables as the web UI (set via `.env` or exported in your shell).

```bash
# Install direct database drivers
pip install psycopg2-binary clickhouse-connect influxdb-client pyarrow

# Check which databases are reachable
python benchmark.py --list-databases

# Run the full benchmark suite (3 timed runs per query)
python benchmark.py --database PostgreSQL --runs 3

# Lighter run - first 3 months of data only (useful for slow databases)
python benchmark.py --database TimescaleDB --scope quarter

# Save results to a custom file
python benchmark.py --database ClickHouse --output results/clickhouse.json
```

Results are saved to `benchmark_results.json` in the same format as the web UI, with per-run backups in `runs/`. The file is cumulative - each run appends to the history so you can collect results for multiple databases over time.

## Utility scripts

| Script | Purpose |
|--------|---------|
| `generate_synthetic_data.py` | Generate a synthetic seed parquet file (`transformedData.parquet`) with ~29M rows of energy meter data. Run this first before loading any database. |
| `build_dashboard.py` | Generate a standalone `dashboard.html` from the seed parquet with interactive charts of hourly aggregates and per-meter daily totals. Requires `duckdb`. |
| `convert_to_parquet.py` | Convert a real JSON dataset (same schema) to parquet format. This was used in the original project with real energy meter data. If you are trying to replicate the benchmark results, use `generate_synthetic_data.py` instead - this script is only relevant if you have your own dataset in the same JSON format. |

## Web UI tabs

| Tab | Description |
|-----|-------------|
| **Benchmark** | Automated benchmark runner with min/median/max results table |
| **Graph** | Visualize query results as a chart |
| **Density** | Daily energy heatmap for one database |
| **Setup** | Data generation, loading, hierarchy creation |
| **DB Status** | Which databases are online and their row counts |
| **Logs** | Live output from running scripts |

## Preset queries

The suite includes 17 preset queries organized into 5 tiers:

| Tier | Type | Examples |
|------|------|---------|
| T1 | Point lookups | Single meter, 1-day / 1-month / 1-year aggregation |
| T2 | Range scans | Monthly E17 vs E18 balance, peak hours |
| T3 | Full-table scans | Top 20 meters by consumption, prosumer detection |
| T4 | Cross-table joins | Hierarchy: supplier totals, category breakdowns |
| T5 | All-time totals | No time windowing, full dataset aggregation |

Each query is automatically translated into every supported dialect: standard SQL, ClickHouse SQL, QuestDB SQL and InfluxDB Flux.

## Storage requirements

| Database | ~1.4B rows |
|----------|-----------|
| PostgreSQL | ~140-150 GB |
| TimescaleDB | ~25-40 GB (compressed) |
| ClickHouse | ~3-5 GB |
| QuestDB | ~15-25 GB |
| InfluxDB 2 | ~20-35 GB |
| Parquet files | ~1.3 GB |

## Tear down

```bash
cd bench/web && docker compose down
cd bench/postgres && docker compose down  # or whichever DB you ran

# Remove data volumes (irreversible)
docker volume rm web_bench_data postgres_pg_data  # adjust for your DB

# Remove shared network
docker network rm benchnet
```

## License

MIT - see [LICENSE](LICENSE).
