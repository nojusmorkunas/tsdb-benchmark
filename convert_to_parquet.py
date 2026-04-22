"""Stream a large JSON array to Parquet in chunks using ijson + pyarrow."""
import ijson
import pyarrow as pa
import pyarrow.parquet as pq
import os

INPUT = "transformedData.json"
OUTPUT = "transformedData.parquet"
CHUNK_SIZE = 100_000  # rows per batch — keeps memory low (~20-50 MB)

schema = pa.schema([
    ("Ean", pa.string()),
    ("EnergyFlowDirection", pa.string()),
    ("Timestamp", pa.string()),
    ("ReceivedAt", pa.string()),
    ("Value", pa.float64()),
])

writer = None
total_rows = 0
chunk = []

print(f"Converting {INPUT} -> {OUTPUT} (streaming, {CHUNK_SIZE} rows/batch) ...")

with open(INPUT, "rb") as f:
    for record in ijson.items(f, "item"):
        record["Value"] = float(record["Value"])
        chunk.append(record)
        if len(chunk) >= CHUNK_SIZE:
            batch = pa.RecordBatch.from_pylist(chunk, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(OUTPUT, schema, compression="zstd")
            writer.write_batch(batch)
            total_rows += len(chunk)
            print(f"  {total_rows:>12,} rows written ...")
            chunk = []

# flush remaining rows
if chunk:
    batch = pa.RecordBatch.from_pylist(chunk, schema=schema)
    if writer is None:
        writer = pq.ParquetWriter(OUTPUT, schema, compression="zstd")
    writer.write_batch(batch)
    total_rows += len(chunk)

if writer:
    writer.close()

size_mb = os.path.getsize(OUTPUT) / (1024 * 1024)
print(f"\nDone! {total_rows:,} rows written to {OUTPUT}")
print(f"Parquet file size: {size_mb:.1f} MB  (was {os.path.getsize(INPUT)/1024/1024:.0f} MB as JSON)")
