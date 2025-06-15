# split_data.py
# Script to split the full csv into 3 files: history, incremental batch, and streaming_demo

SOURCE_FILE = "data/fraud_data.csv"
OUTPUT_DIR = "data/splits"

import pandas as pd
import os

# Ensure the output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Read the full file and sort by Timestamp
print(f"Reading file {SOURCE_FILE}...")
full_file = pd.read_csv(SOURCE_FILE).sort_values(by="Timestamp", ascending=True)
full_file.sort_values(by="Timestamp", inplace=True)

# Compute the split indices
total_rows = len(full_file)
history_end = int(total_rows * 0.60)
batch_end = history_end + int(total_rows * 0.20)

# Slice the dataframes
history_df = full_file.iloc[:history_end]
batch_df = full_file.iloc[history_end:batch_end]
streaming_df = full_file.iloc[batch_end:]

# Write the dataframes to csv
print(f"Writing history slice ({len(history_df)} rows)...")
history_df.to_csv(os.path.join(OUTPUT_DIR, "history.csv"), index=False)

print(f"Writing batch slice ({len(batch_df)} rows)...")
batch_df.to_csv(os.path.join(OUTPUT_DIR, "incremental_batch.csv"), index=False)

print(f"Writing streaming slice ({len(streaming_df)} rows)...")
streaming_df.to_csv(os.path.join(OUTPUT_DIR, "streaming_demo.csv"), index=False)

print("Data split complete!")