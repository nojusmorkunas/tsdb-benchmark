"""
Generate a synthetic transformedData.parquet file with the same schema and scale
as a real energy meter dataset (~29M rows, 11572 EANs, 15-day window, 15-min intervals).

No real data is included — all EAN codes and values are randomly generated.

Output: transformedData.parquet (schema matches the loader in bench/web/load_all.py)
  - Ean                  (str)  18-digit EAN code
  - EnergyFlowDirection  (str)  E17 (consumption) or E18 (injection)
  - Timestamp            (str)  ISO-8601, 15-minute intervals
  - Value                (float) kWh per interval

Usage:
  pip install pandas pyarrow numpy
  python generate_synthetic_data.py
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import time
import sys

# ── Parameters ────────────────────────────────────────────────────────────────

NUM_EANS           = 11_572
INTERVAL_MINUTES   = 15
DAYS               = 15
PROSUMER_FRACTION  = 0.75   # fraction of EANs that also have E18 (injection)
START_DATE         = datetime(2024, 1, 1)
SEED               = 42
OUTPUT_FILE        = "transformedData.parquet"

# Consumption (E17): log-normal to capture low/zero + occasional spikes
E17_MEAN_KWH       = 0.35   # mean per 15-min interval
E17_SIGMA          = 0.8    # log-normal sigma (higher → more variance)

# Injection (E18): typically lower than consumption; zero at night
E18_MEAN_KWH       = 0.15
E18_SIGMA          = 1.0

CHUNK_EANS         = 500    # EANs per chunk (memory management)

# ── Setup ─────────────────────────────────────────────────────────────────────

rng = np.random.default_rng(SEED)
intervals = DAYS * 24 * (60 // INTERVAL_MINUTES)   # 1440

timestamps_dt = [START_DATE + timedelta(minutes=INTERVAL_MINUTES * i) for i in range(intervals)]
timestamps_str = np.array([ts.strftime("%Y-%m-%dT%H:%M:%S") for ts in timestamps_dt])

# Hour-of-day array for shaping the load curve (0-23 repeated)
hours = np.array([ts.hour for ts in timestamps_dt], dtype=np.float32)

# E17 load curve: higher during day (6-22), lower at night
e17_curve = np.where((hours >= 6) & (hours < 22), 1.4, 0.4).astype(np.float32)

# E18 (solar) curve: parabolic peak around noon, zero at night
solar = np.where(
    (hours >= 7) & (hours < 20),
    np.sin(np.pi * (hours - 7) / 13) ** 2,
    0.0,
).astype(np.float32)

# Generate 18-digit EAN codes (country-code prefix 541 = Belgium, rest random)
ean_numbers = rng.integers(10**14, 10**15, size=NUM_EANS)
eans = np.array([f"541{n:015d}" for n in ean_numbers])

# Which EANs have injection (prosumers)
is_prosumer = rng.random(NUM_EANS) < PROSUMER_FRACTION

print(f"Generating {NUM_EANS:,} EANs × {intervals} intervals "
      f"({PROSUMER_FRACTION:.0%} prosumers) = ~{NUM_EANS * (1 + PROSUMER_FRACTION) * intervals / 1e6:.1f}M rows")

# ── Generate ──────────────────────────────────────────────────────────────────

t0 = time.perf_counter()
chunks = []

for start in range(0, NUM_EANS, CHUNK_EANS):
    end = min(start + CHUNK_EANS, NUM_EANS)
    batch_size = end - start
    batch_eans = eans[start:end]
    batch_prosumer = is_prosumer[start:end]

    # E17 rows for entire batch
    # Shape: (batch_size, intervals)
    noise_e17 = rng.lognormal(mean=0.0, sigma=E17_SIGMA, size=(batch_size, intervals)).astype(np.float32)
    values_e17 = (noise_e17 * e17_curve * E17_MEAN_KWH).clip(0)

    # Flatten: repeat each EAN `intervals` times
    ean_col_e17   = np.repeat(batch_eans, intervals)
    dir_col_e17   = np.full(batch_size * intervals, "E17")
    ts_col_e17    = np.tile(timestamps_str, batch_size)
    val_col_e17   = values_e17.ravel()

    chunk_e17 = pd.DataFrame({
        "Ean": ean_col_e17,
        "EnergyFlowDirection": dir_col_e17,
        "Timestamp": ts_col_e17,
        "Value": val_col_e17,
    })
    chunks.append(chunk_e17)

    # E18 rows only for prosumer EANs in this batch
    prosumer_mask = batch_prosumer
    if prosumer_mask.any():
        prosumer_eans = batch_eans[prosumer_mask]
        n_pro = prosumer_mask.sum()

        noise_e18 = rng.lognormal(mean=0.0, sigma=E18_SIGMA, size=(n_pro, intervals)).astype(np.float32)
        values_e18 = (noise_e18 * solar * E18_MEAN_KWH).clip(0)

        chunk_e18 = pd.DataFrame({
            "Ean": np.repeat(prosumer_eans, intervals),
            "EnergyFlowDirection": np.full(n_pro * intervals, "E18"),
            "Timestamp": np.tile(timestamps_str, n_pro),
            "Value": values_e18.ravel(),
        })
        chunks.append(chunk_e18)

    done = end
    elapsed = time.perf_counter() - t0
    print(f"  {done:>6}/{NUM_EANS} EANs  {elapsed:.1f}s", end="\r", flush=True)

print()

# ── Write ─────────────────────────────────────────────────────────────────────

print("Concatenating…", flush=True)
df = pd.concat(chunks, ignore_index=True)
total_rows = len(df)

print(f"Writing {total_rows:,} rows to {OUTPUT_FILE}…", flush=True)
df.to_parquet(OUTPUT_FILE, index=False, compression="zstd")

elapsed = time.perf_counter() - t0
size_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
import os
file_mb = os.path.getsize(OUTPUT_FILE) / 1024 / 1024

print(f"\nDone in {elapsed:.1f}s")
print(f"  Rows:        {total_rows:,}")
print(f"  EANs:        {NUM_EANS:,}  ({is_prosumer.sum():,} prosumers)")
print(f"  File size:   {file_mb:.1f} MB  ({OUTPUT_FILE})")
