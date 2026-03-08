import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

RAW_DATA_DIR = "data/raw"
RAW_FILE = os.path.join(RAW_DATA_DIR, "employee_data.csv")

DB_NAME = "csds397"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

def main():
    df = pd.read_csv(RAW_FILE, dtype=str)

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE raw_data RESTART IDENTITY;")

    records = [tuple(row) for row in df.itertuples(index=False)]
    execute_values(cur, """
        INSERT INTO raw_data (
            employee_id, name, age, department, date_of_joining,
            years_of_experience, country, salary, performance_rating,
            total_sales, support_rating
        ) VALUES %s
    """, records)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(records)} rows into raw_data.")

if __name__ == "__main__":
    main()