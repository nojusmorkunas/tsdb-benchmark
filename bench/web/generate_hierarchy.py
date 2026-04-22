"""Generate synthetic meter hierarchy data from existing EANs in the dataset.

Hierarchy structure:
    Supplier → Category (PRF/SMA) → Sub-category (AZI/AMI, PRF only) → EAN

~10% of EANs switch supplier at the dataset midpoint to simulate real-world changes.
Output: /data/hierarchy.parquet
"""
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import random
from config import DATA_DIR

DATA_GLOB = f"{DATA_DIR}/block_*.parquet"
OUTPUT = f"{DATA_DIR}/hierarchy.parquet"

random.seed(42)  # reproducible

SUPPLIERS = ["SupplierA", "SupplierB", "SupplierC"]

def rand_assignment():
    supplier = random.choice(SUPPLIERS)
    category = random.choices(["PRF", "SMA"], weights=[2, 1])[0]
    sub_category = random.choice(["AZI", "AMI"]) if category == "PRF" else None
    return supplier, category, sub_category

print("Reading distinct EANs and time range from parquet files...")
con = duckdb.connect()
eans = [r[0] for r in con.sql(f"SELECT DISTINCT Ean FROM '{DATA_GLOB}' ORDER BY Ean").fetchall()]
min_ts, max_ts = con.sql(
    f"SELECT MIN(Timestamp::TIMESTAMP), MAX(Timestamp::TIMESTAMP) FROM '{DATA_GLOB}'"
).fetchone()
mid_ts = min_ts + (max_ts - min_ts) / 2

print(f"  {len(eans):,} distinct EANs")
print(f"  Time range: {min_ts} → {max_ts}")
print(f"  Midpoint:   {mid_ts}")

rows = []
switches = 0
for ean in eans:
    supplier, category, sub_category = rand_assignment()

    if random.random() < 0.10:  # 10% switch supplier at midpoint
        rows.append({
            "ean": ean, "supplier": supplier, "category": category,
            "sub_category": sub_category, "valid_from": min_ts, "valid_to": mid_ts,
        })
        new_supplier, new_category, new_sub = rand_assignment()
        while new_supplier == supplier:  # ensure it actually switches
            new_supplier, new_category, new_sub = rand_assignment()
        rows.append({
            "ean": ean, "supplier": new_supplier, "category": new_category,
            "sub_category": new_sub, "valid_from": mid_ts, "valid_to": None,
        })
        switches += 1
    else:
        rows.append({
            "ean": ean, "supplier": supplier, "category": category,
            "sub_category": sub_category, "valid_from": min_ts, "valid_to": None,
        })

schema = pa.schema([
    ("ean",          pa.string()),
    ("supplier",     pa.string()),
    ("category",     pa.string()),
    ("sub_category", pa.string()),
    ("valid_from",   pa.timestamp("us")),
    ("valid_to",     pa.timestamp("us")),
])

table = pa.Table.from_pylist(rows, schema=schema)
pq.write_table(table, OUTPUT, compression="zstd")

print(f"\nDone! {len(rows):,} rows ({len(eans):,} EANs, {switches:,} with supplier switch)")
print(f"Output: {OUTPUT}")
