# load_to_postgres.py
# Script to load a CSV slice into Postgres using SQLAlchemy, with credentials from a .env file

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from urllib.parse import quote_plus

# Uncomment the following line to load environment variables from a .env file

load_dotenv()

DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))
DB_HOST = os.getenv("DB_HOST")
DB_HOST_INT = os.getenv("DB_HOST_INT")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

CSV_PATH    = "data/splits/history.csv"


# password = quote_plus("WelcomeItc@2022")
try:
    engine = create_engine(
        # f"postgresql://consultants:{password}@172.31.14.3:5432/testdb"
        f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
except Exception as e:
    print(f"An error occurred while creating the Postgres engine: {e}")
    exit(1)

# Load the CSV file into a DataFrame
df = pd.read_csv("data/splits/history.csv")

# df = pd.read_csv("history.csv")

# Load the DataFrame into the Postgres table
try:
    df.to_sql(
        "rdv_history",        # Name of the table in Postgres
        con=engine,           # Use the engine
        if_exists="replace",  # Options: 'fail', 'replace', 'append'
        index=False,          # Do not write row indices
        method="multi"        # Use multi-row insert for efficiency
    )
    print("Data loaded successfully into Postgres table 'rdv_history'.")
except Exception as e:
    print(f"An error occurred while loading data into Postgres: {e}")
