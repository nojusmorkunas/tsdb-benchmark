"""Load the generated Parquet data into all 5 databases.

Usage: python3 load_all.py [--only postgres,clickhouse,...]
"""
import duckdb
import psycopg2
import psycopg2.extras
import clickhouse_connect
import io
import time
import sys
import os
from influxdb_client import InfluxDBClient, WriteOptions
from config import DB, DATA_DIR

DATA_GLOB = f"{DATA_DIR}/block_*.parquet"
BATCH_SIZE = 200_000

targets = None
for i, arg in enumerate(sys.argv[1:], 1):
    if arg == "--only":
        targets = sys.argv[i + 1].split(",")


def should_run(name):
    return targets is None or name in targets


ROW_WARN_THRESHOLD = 1_300_000_000

def confirm_drop(db_name, row_count):
    """Prompt user to confirm if table already has significant data."""
    print(f"\n  WARNING: {db_name} energy_data already has {row_count:,} rows.")
    print(f"  This will DROP the table and reload from scratch.")
    answer = input("  Are you sure? (yes/no): ").strip().lower()
    if answer != "yes":
        print(f"  Skipping {db_name}.")
        return False
    return True


def iter_batches(batch_size=BATCH_SIZE):
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    total = con.sql(f"SELECT COUNT(*) FROM '{DATA_GLOB}'").fetchone()[0]
    print(f"  Total rows to load: {total:,}", flush=True)
    result = con.sql(f"""
        SELECT Ean, EnergyFlowDirection, Timestamp::TIMESTAMP AS ts, Value
        FROM '{DATA_GLOB}'
    """)
    loaded = 0
    while True:
        rows = result.fetchmany(batch_size)
        if not rows:
            break
        batch = [{"ean": r[0], "dir": r[1], "ts": r[2], "val": r[3]} for r in rows]
        loaded += len(batch)
        yield batch, loaded, total


_last_log_time = 0

def progress(loaded, total, t0, prefix=""):
    global _last_log_time
    now = time.perf_counter()
    if now - _last_log_time < 10:
        return
    _last_log_time = now
    pct = loaded / total * 100
    elapsed = now - t0
    rate = loaded / elapsed if elapsed > 0 else 0
    eta = (total - loaded) / rate / 60 if rate > 0 else 0
    print(f"  {prefix}{loaded:>13,} / {total:,} ({pct:.1f}%) | {rate:,.0f} rows/s | ETA {eta:.1f}m", flush=True)


# --- PostgreSQL ---
def load_postgres():
    cfg = DB["PostgreSQL"]
    print("\n[PostgreSQL] Loading ...", flush=True)
    conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
                            user=cfg["user"], password=cfg["password"])
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET synchronous_commit TO off")
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='energy_data'")
    if cur.fetchone()[0]:
        cur.execute("SELECT COUNT(*) FROM energy_data")
        n = cur.fetchone()[0]
        if n >= ROW_WARN_THRESHOLD and not confirm_drop("PostgreSQL", n):
            cur.close(); conn.close(); return
    cur.execute("DROP TABLE IF EXISTS energy_data;")
    cur.execute("""CREATE TABLE energy_data (
        ean TEXT NOT NULL, direction TEXT NOT NULL,
        ts TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NOT NULL);""")
    t0 = time.perf_counter()
    for batch, loaded, total in iter_batches():
        buf = io.StringIO()
        for r in batch:
            buf.write(f"{r['ean']}\t{r['dir']}\t{r['ts'].isoformat()}\t{r['val']}\n")
        buf.seek(0)
        cur.copy_from(buf, 'energy_data', columns=('ean', 'direction', 'ts', 'value'))
        progress(loaded, total, t0)
    print(f"\n  Creating indexes ...", flush=True)
    cur.execute("CREATE INDEX idx_energy_ts ON energy_data (ts);")
    cur.execute("CREATE INDEX idx_energy_ean_ts ON energy_data (ean, ts);")
    cur.execute("ANALYZE energy_data;")
    print(f"  Done: {loaded:,} rows in {(time.perf_counter()-t0)/60:.1f} min", flush=True)
    cur.close(); conn.close()


# --- TimescaleDB ---
def load_timescaledb():
    cfg = DB["TimescaleDB"]
    print("\n[TimescaleDB] Loading ...", flush=True)
    conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
                            user=cfg["user"], password=cfg["password"])
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SET synchronous_commit TO off")
    cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='energy_data'")
    if cur.fetchone()[0]:
        cur.execute("SELECT COUNT(*) FROM energy_data")
        n = cur.fetchone()[0]
        if n >= ROW_WARN_THRESHOLD and not confirm_drop("TimescaleDB", n):
            cur.close(); conn.close(); return
    cur.execute("DROP TABLE IF EXISTS energy_data CASCADE;")
    cur.execute("""CREATE TABLE energy_data (
        ean TEXT NOT NULL, direction TEXT NOT NULL,
        ts TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NOT NULL);""")
    cur.execute("SELECT create_hypertable('energy_data', 'ts', chunk_time_interval => INTERVAL '7 days');")
    t0 = time.perf_counter()
    for batch, loaded, total in iter_batches():
        buf = io.StringIO()
        for r in batch:
            buf.write(f"{r['ean']}\t{r['dir']}\t{r['ts'].isoformat()}\t{r['val']}\n")
        buf.seek(0)
        cur.copy_from(buf, 'energy_data', columns=('ean', 'direction', 'ts', 'value'))
        progress(loaded, total, t0)
    print(f"\n  Creating index + enabling compression ...", flush=True)
    cur.execute("CREATE INDEX idx_energy_ean ON energy_data (ean, ts DESC);")
    cur.execute("""ALTER TABLE energy_data SET (
        timescaledb.compress, timescaledb.compress_segmentby = 'ean, direction',
        timescaledb.compress_orderby = 'ts');""")
    cur.execute("SELECT compress_chunk(c) FROM show_chunks('energy_data') c;")
    print(f"  Done: {loaded:,} rows in {(time.perf_counter()-t0)/60:.1f} min", flush=True)
    cur.close(); conn.close()


# --- ClickHouse ---
def load_clickhouse():
    from concurrent.futures import ThreadPoolExecutor, as_completed

    cfg = DB["ClickHouse"]
    print("\n[ClickHouse] Loading ...", flush=True)

    def make_client():
        return clickhouse_connect.get_client(
            host=cfg["host"], port=cfg["port"],
            username=cfg.get("user", "default"),
            password=cfg.get("password", ""),
            compress=True,  # LZ4 on the wire
        )

    client = make_client()
    n = client.query("SELECT COUNT(*) FROM energy_data").result_rows[0][0] if client.query("EXISTS TABLE energy_data").result_rows[0][0] else 0
    if n >= ROW_WARN_THRESHOLD and not confirm_drop("ClickHouse", n):
        client.close(); return
    client.command("DROP TABLE IF EXISTS energy_data")
    client.command("""CREATE TABLE energy_data (
        ean String, direction String, ts DateTime64(0), value Float64
    ) ENGINE = MergeTree() ORDER BY (ean, direction, ts)""")
    client.close()

    CHUNK = 50_000   # 10k-100k is optimal per docs
    WORKERS = 4

    def _send(rows):
        c = make_client()
        try:
            data = [[r[0], r[1], r[2], r[3]] for r in rows]
            c.insert("energy_data", data, column_names=["ean", "direction", "ts", "value"])
        finally:
            c.close()

    t0 = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    total = con.sql(f"SELECT COUNT(*) FROM '{DATA_GLOB}'").fetchone()[0]
    print(f"  Total rows to load: {total:,}", flush=True)
    result = con.sql(f"SELECT Ean, EnergyFlowDirection, Timestamp::TIMESTAMP AS ts, Value FROM '{DATA_GLOB}'")
    loaded = 0
    fetch_batch = CHUNK * WORKERS * 2

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        while True:
            rows = result.fetchmany(fetch_batch)
            if not rows:
                break
            chunks = [rows[i:i+CHUNK] for i in range(0, len(rows), CHUNK)]
            futures = [pool.submit(_send, chunk) for chunk in chunks]
            for f in as_completed(futures):
                f.result()
            loaded += len(rows)
            progress(loaded, total, t0)

    print(f"\n  Done: {loaded:,} rows in {(time.perf_counter()-t0)/60:.1f} min", flush=True)


# --- QuestDB ---
def load_questdb():
    cfg = DB["QuestDB"]
    print("\n[QuestDB] Loading ...", flush=True)
    from questdb.ingress import Sender
    t0 = time.perf_counter()
    loaded = 0
    conf = f"http::addr={cfg['host']}:9000;auto_flush_rows=75000;auto_flush_interval=1000;"
    with Sender.from_conf(conf) as sender:
        for batch, loaded, total in iter_batches():
            for r in batch:
                sender.row(
                    "energy_data",
                    symbols={"ean": r["ean"], "direction": r["dir"]},
                    columns={"value": float(r["val"])},
                    at=r["ts"],
                )
            sender.flush()
            progress(loaded, total, t0)
    print(f"\n  Done: {loaded:,} rows in {(time.perf_counter()-t0)/60:.1f} min", flush=True)


# --- InfluxDB 2 ---
def load_influxdb():
    import gzip
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed

    cfg = DB["InfluxDB 2"]
    print("\n[InfluxDB 2] Loading ...", flush=True)

    base_url = cfg["url"].rstrip("/")
    write_url = f"{base_url}/api/v2/write?org={cfg['org']}&bucket=energy&precision=s"
    headers = {
        "Authorization": f"Token {cfg['token']}",
        "Content-Encoding": "gzip",
        "Content-Type": "text/plain; charset=utf-8",
    }

    CHUNK = 5_000    # optimal per v2 docs
    WORKERS = 4      # parallel concurrent requests

    def _send(lines):
        body = gzip.compress("\n".join(lines).encode(), compresslevel=1)
        requests.post(write_url, data=body, headers=headers, timeout=60).raise_for_status()

    t0 = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    total = con.sql(f"SELECT COUNT(*) FROM '{DATA_GLOB}'").fetchone()[0]
    print(f"  Total rows to load: {total:,}", flush=True)
    result = con.sql(f"SELECT Ean, EnergyFlowDirection, Timestamp::TIMESTAMP AS ts, Value FROM '{DATA_GLOB}'")
    loaded = 0
    fetch_batch = CHUNK * WORKERS * 2

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        while True:
            rows = result.fetchmany(fetch_batch)
            if not rows:
                break
            # sorted tags: direction before ean (d < e), seconds precision
            all_lines = [
                f"energy,direction={r[1]},ean={r[0]} value={r[3]} {int(r[2].timestamp())}"
                for r in rows
            ]
            chunks = [all_lines[i:i+CHUNK] for i in range(0, len(all_lines), CHUNK)]
            futures = [pool.submit(_send, chunk) for chunk in chunks]
            for f in as_completed(futures):
                f.result()
            loaded += len(rows)
            progress(loaded, total, t0)

    print(f"\n  Done: {loaded:,} rows in {(time.perf_counter()-t0)/60:.1f} min", flush=True)


# --- InfluxDB 3 ---
def load_influxdb3():
    import gzip
    import requests
    from concurrent.futures import ThreadPoolExecutor, as_completed

    cfg = DB["InfluxDB 3"]
    print("\n[InfluxDB 3] Loading ...", flush=True)

    base_url = cfg["url"].rstrip("/")
    write_url = f"{base_url}/api/v3/write_lp?db={cfg['database']}&precision=second&no_sync=true"
    headers = {"Content-Encoding": "gzip", "Content-Type": "text/plain; charset=utf-8"}

    CHUNK = 10_000   # optimal per docs: 10k lines or 10MB
    WORKERS = 4      # parallel concurrent requests

    def _send(lines):
        body = gzip.compress("\n".join(lines).encode(), compresslevel=1)
        requests.post(write_url, data=body, headers=headers, timeout=60).raise_for_status()

    t0 = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    total = con.sql(f"SELECT COUNT(*) FROM '{DATA_GLOB}'").fetchone()[0]
    print(f"  Total rows to load: {total:,}", flush=True)
    result = con.sql(f"SELECT Ean, EnergyFlowDirection, Timestamp::TIMESTAMP AS ts, Value FROM '{DATA_GLOB}'")
    loaded = 0
    fetch_batch = CHUNK * WORKERS * 2  # read ahead enough to keep all workers busy

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        while True:
            rows = result.fetchmany(fetch_batch)
            if not rows:
                break
            # sorted tags: direction before ean (d < e), seconds precision
            all_lines = [
                f"energy,direction={r[1]},ean={r[0]} value={r[3]} {int(r[2].timestamp())}"
                for r in rows
            ]
            chunks = [all_lines[i:i+CHUNK] for i in range(0, len(all_lines), CHUNK)]
            futures = [pool.submit(_send, chunk) for chunk in chunks]
            for f in as_completed(futures):
                f.result()  # raises on HTTP error
            loaded += len(rows)
            progress(loaded, total, t0)

    print(f"\n  Done: {loaded:,} rows in {(time.perf_counter()-t0)/60:.1f} min", flush=True)


# --- Hierarchy loaders ---
HIER_FILE = f"{DATA_DIR}/hierarchy.parquet"

def _read_hierarchy():
    con = duckdb.connect()
    return con.sql(
        f"SELECT ean, supplier, category, sub_category, valid_from, valid_to FROM '{HIER_FILE}'"
    ).fetchall()

def load_hierarchy_postgres(cfg):
    conn = psycopg2.connect(host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
                            user=cfg["user"], password=cfg["password"])
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS meter_hierarchy;")
    cur.execute("""CREATE TABLE meter_hierarchy (
        ean TEXT NOT NULL, supplier TEXT NOT NULL, category TEXT NOT NULL,
        sub_category TEXT, valid_from TIMESTAMPTZ NOT NULL, valid_to TIMESTAMPTZ
    );""")
    rows = _read_hierarchy()
    args = ",".join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", r).decode() for r in rows)
    cur.execute(f"INSERT INTO meter_hierarchy VALUES {args}")
    cur.execute("CREATE INDEX idx_hier_ean_time ON meter_hierarchy (ean, valid_from, valid_to);")
    print(f"  Loaded {len(rows):,} hierarchy rows", flush=True)
    cur.close(); conn.close()

def load_hierarchy_clickhouse(cfg):
    client = clickhouse_connect.get_client(host=cfg["host"], port=cfg["port"],
                                           username=cfg.get("user", "default"),
                                           password=cfg.get("password", ""))
    client.command("DROP TABLE IF EXISTS meter_hierarchy")
    client.command("""CREATE TABLE meter_hierarchy (
        ean String, supplier String, category String,
        sub_category Nullable(String),
        valid_from DateTime64(0), valid_to Nullable(DateTime64(0))
    ) ENGINE = MergeTree() ORDER BY (ean, valid_from)""")
    rows = _read_hierarchy()
    data = [list(r) for r in rows]
    client.insert("meter_hierarchy", data,
                  column_names=["ean", "supplier", "category", "sub_category", "valid_from", "valid_to"])
    print(f"  Loaded {len(rows):,} hierarchy rows", flush=True)
    client.close()

def load_hierarchy_questdb(cfg):
    from questdb.ingress import Sender
    rows = _read_hierarchy()
    with Sender.from_conf(f"tcp::addr={cfg['host']}:9009;") as sender:
        for r in rows:
            ean, supplier, category, sub_category, valid_from, valid_to = r
            symbols = {"ean": ean, "supplier": supplier, "category": category}
            columns = {}
            if sub_category:
                columns["sub_category"] = sub_category
            if valid_to:
                columns["valid_to"] = int(valid_to.timestamp() * 1e9)
            sender.row("meter_hierarchy", symbols=symbols, columns=columns, at=valid_from)
        sender.flush()
    print(f"  Loaded {len(rows):,} hierarchy rows", flush=True)

def _load_hierarchy_for(db_name, fn):
    import os
    if not os.path.exists(HIER_FILE):
        print(f"\n[{db_name}] hierarchy.parquet not found. Run generate_hierarchy.py first.", flush=True)
        return
    print(f"\n[{db_name}] Loading hierarchy ...", flush=True)
    try:
        fn()
    except Exception as e:
        print(f"  ERROR: {e}", flush=True)

def load_hierarchy_influxdb2(cfg):
    client = InfluxDBClient(url=cfg["url"], token=cfg["token"], org=cfg["org"])
    write_api = client.write_api(write_options=WriteOptions(batch_size=5000, flush_interval=1000))
    rows = _read_hierarchy()
    for ean, supplier, category, sub_category, valid_from, valid_to in rows:
        tags = f"ean={ean},supplier={supplier},category={category}"
        if sub_category:
            tags += f",sub_category={sub_category}"
        valid_to_ns = int(valid_to.timestamp() * 1e9) if valid_to else 0
        ts_ns = int(valid_from.timestamp() * 1e9)
        line = f"meter_hierarchy,{tags} valid_to_ns={valid_to_ns}i {ts_ns}"
        write_api.write(bucket="energy", record=line)
    write_api.close()
    print(f"  Loaded {len(rows):,} hierarchy rows", flush=True)
    client.close()


def load_hierarchy_influxdb3(cfg):
    from influxdb_client_3 import InfluxDBClient3
    client = InfluxDBClient3(host=cfg["url"], database=cfg["database"], token=cfg.get("token"), write_timeout=600_000)
    rows = _read_hierarchy()
    lines = []
    for ean, supplier, category, sub_category, valid_from, valid_to in rows:
        tags = f"ean={ean},supplier={supplier},category={category}"
        if sub_category:
            tags += f",sub_category={sub_category}"
        # Store valid_to as integer nanoseconds (0 = open-ended). valid_from is
        # encoded as the line protocol timestamp so no separate field is needed.
        valid_to_ns = int(valid_to.timestamp() * 1e9) if valid_to else 0
        fields = f"valid_to_ns={valid_to_ns}i"
        ts_ns = int(valid_from.timestamp() * 1e9)
        lines.append(f"meter_hierarchy,{tags} {fields} {ts_ns}")
    batch_size = 5000
    for i in range(0, len(lines), batch_size):
        client.write(record="\n".join(lines[i:i+batch_size]), write_precision="ns")
    print(f"  Loaded {len(rows):,} hierarchy rows", flush=True)
    client.close()


def load_hierarchy():
    _load_hierarchy_for("PostgreSQL",  lambda: load_hierarchy_postgres(DB["PostgreSQL"]))
    _load_hierarchy_for("TimescaleDB", lambda: load_hierarchy_postgres(DB["TimescaleDB"]))
    _load_hierarchy_for("ClickHouse",  lambda: load_hierarchy_clickhouse(DB["ClickHouse"]))
    _load_hierarchy_for("QuestDB",     lambda: load_hierarchy_questdb(DB["QuestDB"]))
    _load_hierarchy_for("InfluxDB 3",  lambda: load_hierarchy_influxdb3(DB["InfluxDB 3"]))
    _load_hierarchy_for("InfluxDB 2",  lambda: load_hierarchy_influxdb2(DB["InfluxDB 2"]))


if __name__ == "__main__":
    loaders = {
        "postgres": load_postgres, "timescaledb": load_timescaledb,
        "clickhouse": load_clickhouse, "questdb": load_questdb, "influxdb": load_influxdb,
        "influxdb3": load_influxdb3, "hierarchy": load_hierarchy,
        "hierarchy-postgres":    lambda: _load_hierarchy_for("PostgreSQL",  lambda: load_hierarchy_postgres(DB["PostgreSQL"])),
        "hierarchy-timescaledb": lambda: _load_hierarchy_for("TimescaleDB", lambda: load_hierarchy_postgres(DB["TimescaleDB"])),
        "hierarchy-clickhouse":  lambda: _load_hierarchy_for("ClickHouse",  lambda: load_hierarchy_clickhouse(DB["ClickHouse"])),
        "hierarchy-questdb":     lambda: _load_hierarchy_for("QuestDB",     lambda: load_hierarchy_questdb(DB["QuestDB"])),
        "hierarchy-influxdb3":   lambda: _load_hierarchy_for("InfluxDB 3",  lambda: load_hierarchy_influxdb3(DB["InfluxDB 3"])),
        "hierarchy-influxdb2":   lambda: _load_hierarchy_for("InfluxDB 2",  lambda: load_hierarchy_influxdb2(DB["InfluxDB 2"])),
    }
    for name, fn in loaders.items():
        if should_run(name):
            try:
                fn()
            except Exception as e:
                print(f"\n  ERROR loading {name}: {e}", flush=True)
