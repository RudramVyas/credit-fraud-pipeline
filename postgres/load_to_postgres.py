# load_to_postgres.py
# Script to load a CSV slice into Postgres using SQLAlchemy

import pandas as pd
from sqlalchemy import create_engine
import os

# Configuration - update these with your Postgres credentials
DB_USERNAME = os.getenv("DB_USERNAME", "your_username")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_password")
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "frauddb")
TABLE_NAME  = "fraud_transactions"

# CSV to load - point to the history slice first
CSV_PATH    = "data/splits/history.csv"

# Create the SQLAlchemy engine
engine = create_engine(f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Load DataFrame
print(f"Loading CSV from {CSV_PATH}...")
df = pd.read_csv(CSV_PATH)
print(f"Loaded {len(df)} rows. Writing to Postgres table '{TABLE_NAME}'...")

# Write to Postgres (replace existing data in the table)
df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False, method='multi', chunksize=1000)

print("Data load complete.")
