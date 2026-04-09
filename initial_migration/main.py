import os
import pandas as pd
import psycopg2
from urllib.parse import urlparse

# ---------- CONFIG ----------
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "",
)
if not DATABASE_URL:
    raise SystemExit("Set DATABASE_URL to your PostgreSQL connection string (e.g. in .env).")

CSV_FOLDER = "csvs"

# ---------- PARSE DB URL ----------
result = urlparse(DATABASE_URL)

conn = psycopg2.connect(
    database=result.path[1:],
    user=result.username,
    password=result.password,
    host=result.hostname,
    port=result.port,
    sslmode="require"
)

cursor = conn.cursor()

# ---------- CREATE TABLE ----------
def create_table(cursor, table_name, df):
    columns = [f"{col} TEXT" for col in df.columns]  # TEXT is safer in Postgres
    columns_sql = ", ".join(columns)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns_sql}
        )
    """)

# ---------- INSERT DATA ----------
def insert_data(cursor, table_name, df):
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    data = list(df.itertuples(index=False, name=None))

    cursor.executemany(query, data)
    print(f"{cursor.rowcount} rows inserted into {table_name}")

# ---------- PROCESS CSV FILES ----------
for file in os.listdir(CSV_FOLDER):
    if file.endswith(".csv"):
        file_path = os.path.join(CSV_FOLDER, file)
        table_name = file.replace(".csv", "")

        print(f"Processing {file} → {table_name}")

        df = pd.read_csv(file_path)
        df = df.fillna("")

        create_table(cursor, table_name, df)
        insert_data(cursor, table_name, df)

# ---------- FINALIZE ----------
conn.commit()
cursor.close()
conn.close()

print("All CSV files imported successfully into PostgreSQL.")