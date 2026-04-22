"""Generate a 2-year dataset by replaying the seed Parquet 48 times with shifted timestamps."""
import duckdb
import os
import time
from config import DATA_DIR, SEED_FILE

OUTPUT_DIR = DATA_DIR
REPETITIONS = 48  # 48 blocks x 15-day seed = ~2 years, ~1.4B rows

os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()

count = con.sql(f"SELECT COUNT(*) FROM '{SEED_FILE}'").fetchone()[0]
span = con.sql(f"""
    SELECT DATEDIFF('second', MIN(Timestamp::TIMESTAMP), MAX(Timestamp::TIMESTAMP))
    FROM '{SEED_FILE}'
""").fetchone()[0]

print(f"Seed: {count:,} rows, {span/86400:.1f} days")
print(f"Generating {REPETITIONS} blocks -> ~{count * REPETITIONS / 1e9:.2f}B rows")
print(flush=True)

t_total = time.perf_counter()

for i in range(REPETITIONS):
    t0 = time.perf_counter()
    outfile = f"{OUTPUT_DIR}/block_{i:03d}.parquet"

    if os.path.exists(outfile):
        print(f"  Block {i+1:>2}/{REPETITIONS}: already exists, skipping")
        continue

    con.sql(f"""
        COPY (
            SELECT Ean, EnergyFlowDirection,
                (Timestamp::TIMESTAMP + INTERVAL '{i * span} seconds')::VARCHAR AS Timestamp,
                ReceivedAt,
                Value * (0.85 + RANDOM() * 0.30) AS Value
            FROM '{SEED_FILE}'
        ) TO '{outfile}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    elapsed = time.perf_counter() - t0
    total_elapsed = time.perf_counter() - t_total
    rows_so_far = (i + 1) * count
    eta = total_elapsed / (i + 1) * (REPETITIONS - i - 1)
    print(f"  Block {i+1:>2}/{REPETITIONS}: {elapsed:.1f}s | "
          f"{rows_so_far/1e6:.0f}M rows | ETA {eta/60:.1f} min", flush=True)

total_time = time.perf_counter() - t_total
total_rows = REPETITIONS * count
total_size = sum(
    os.path.getsize(os.path.join(OUTPUT_DIR, f))
    for f in os.listdir(OUTPUT_DIR)
    if f.startswith("block_") and f.endswith(".parquet")
)

print(f"\nDone! {total_rows:,} rows in {total_time/60:.1f} min")
print(f"Total size: {total_size/1024/1024/1024:.2f} GB")

verified = con.sql(f"SELECT COUNT(*) FROM '{OUTPUT_DIR}/block_*.parquet'").fetchone()[0]
print(f"Verified: {verified:,} rows", flush=True)
