import os
from sqlalchemy import create_engine
import pandas as pd

DB_USER=os.getenv("POSTGRES_USER", "de_user")
DB_PASS=os.getenv("POSTGRES_PASSWORD", "de_pass")
DB_NAME=os.getenv("POSTGRES_DB", "de")
DB_HOST=os.getenv("POSTGRES_HOST", "localhost")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}")
print(f"ENGINE", engine)

ROOT = os.path.dirname(os.path.dirname(__file__))
RAW = os.path.join(ROOT, "data", "raw")

def load_csv_to_table(file_name, table_name):
    path = os.path.join(RAW, file_name)
    df = pd.read_csv(path)
    df.to_sql(table_name, engine, schema="staging", if_exists="replace", index=False)
    print(f"Loaded {file_name} -> staging.{table_name} {len(df)} rows")

if __name__ == "__main__":
    load_csv_to_table("customers.csv", "customers")
    load_csv_to_table("orders.csv", "orders")