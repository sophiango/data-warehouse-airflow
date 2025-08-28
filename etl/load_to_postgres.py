import os
import pandas as pd
from sqlalchemy import create_engine, text

DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME = "de_user","de_pass","localhost","5432","de"
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

BASE = os.path.dirname(os.path.dirname(__file__))
RAW = os.path.join(BASE, "data", "raw")

def load_csv_to_table(filename, table_name):
    path = os.path.join(RAW, filename)
    df = pd.read_csv(path)

    # Drop rows with null IDs
    for col in ["customer_id", "order_id"]:
        if col in df.columns:
            df = df.dropna(subset=[col])
            df[col] = df[col].astype(str)

    # Create schema once
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))

    # Use engine instead of connection for pandas
    df.to_sql(
        table_name,
        con=engine,
        schema="staging",
        if_exists="replace",
        index=False
    )

    print(f"✅ Loaded {filename} -> staging.{table_name} ({len(df)} rows)")

if __name__ == "__main__":
    load_csv_to_table("customers.csv", "customers")
    load_csv_to_table("orders.csv", "orders")
