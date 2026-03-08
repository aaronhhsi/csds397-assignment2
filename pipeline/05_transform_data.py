import psycopg2
import pandas as pd
from dateutil import parser as dateparser

DB_NAME = "csds397"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

VALID_DEPARTMENTS = {"Sales", "Support", "Marketing"}

def normalize_date(date_str):
    """Parse dates in any format and return YYYY-MM-DD, or None if unparseable."""
    if pd.isna(date_str) or str(date_str).strip() == "":
        return None
    try:
        return dateparser.parse(str(date_str).strip()).strftime("%Y-%m-%d")
    except Exception:
        return None

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    df = pd.read_sql("SELECT * FROM sources.raw_data;", conn)
    print(f"Loaded {len(df)} rows from sources.raw_data.")

    # --- Step 1: Remove fully duplicate rows ---
    df = df.drop_duplicates()
    print(f"After removing duplicate rows: {len(df)}")

    # --- Step 2: Remove duplicate employee IDs (keep first occurrence) ---
    df = df.drop_duplicates(subset=["employee_id"])
    print(f"After removing duplicate employee IDs: {len(df)}")

    # --- Step 3: Drop rows with invalid departments ---
    invalid_dept = ~df["department"].isin(VALID_DEPARTMENTS)
    print(f"Rows with invalid department dropped: {invalid_dept.sum()}")
    df = df[~invalid_dept]

    # --- Step 4: Drop rows missing critical fields ---
    df = df.dropna(subset=["employee_id", "name", "department"])
    print(f"After dropping rows missing critical fields: {len(df)}")

    # --- Step 5: Normalize date formats ---
    df["date_of_joining"] = df["date_of_joining"].apply(normalize_date)

    # --- Step 6: Cast numeric columns, coerce invalid values to NaN ---
    df["employee_id"] = pd.to_numeric(df["employee_id"], errors="coerce")
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df["years_of_experience"] = pd.to_numeric(df["years_of_experience"], errors="coerce")
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")
    df["performance_rating"] = pd.to_numeric(df["performance_rating"], errors="coerce")
    df["total_sales"] = pd.to_numeric(df["total_sales"], errors="coerce")
    df["support_rating"] = pd.to_numeric(df["support_rating"], errors="coerce")

    # --- Step 7: Validate ranges, NULL out invalid values ---
    df.loc[~df["performance_rating"].between(1, 5), "performance_rating"] = None
    df.loc[~df["support_rating"].between(1, 5), "support_rating"] = None
    df.loc[df["age"] <= 0, "age"] = None
    df.loc[df["salary"] < 0, "salary"] = None

    # --- Step 8: Normalize country casing ---
    df["country"] = df["country"].str.strip().str.title()

    # --- Populate staging.departments ---
    cur.execute("TRUNCATE TABLE staging.support_metrics, staging.sales_metrics, staging.employees, staging.departments RESTART IDENTITY CASCADE;")

    for dept in df["department"].unique():
        cur.execute(
            "INSERT INTO staging.departments (department_name) VALUES (%s) ON CONFLICT (department_name) DO NOTHING;",
            (dept,)
        )

    cur.execute("SELECT department_id, department_name FROM staging.departments;")
    dept_map = {name: did for did, name in cur.fetchall()}

    # --- Populate staging.employees ---
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO staging.employees (
                employee_id, name, age, department_id, date_of_joining,
                years_of_experience, country, salary, performance_rating
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (employee_id) DO NOTHING;
        """, (
            int(row["employee_id"]),
            row["name"],
            int(row["age"]) if pd.notna(row["age"]) else None,
            dept_map[row["department"]],
            row["date_of_joining"],
            int(row["years_of_experience"]) if pd.notna(row["years_of_experience"]) else None,
            row["country"] if pd.notna(row["country"]) else None,
            float(row["salary"]) if pd.notna(row["salary"]) else None,
            int(row["performance_rating"]) if pd.notna(row["performance_rating"]) else None,
        ))

    # --- Populate staging.sales_metrics (Sales only) ---
    for _, row in df[df["department"] == "Sales"].iterrows():
        cur.execute("""
            INSERT INTO staging.sales_metrics (employee_id, total_sales)
            VALUES (%s, %s)
            ON CONFLICT (employee_id) DO NOTHING;
        """, (
            int(row["employee_id"]),
            float(row["total_sales"]) if pd.notna(row["total_sales"]) else None,
        ))

    # --- Populate staging.support_metrics (Support only) ---
    for _, row in df[df["department"] == "Support"].iterrows():
        cur.execute("""
            INSERT INTO staging.support_metrics (employee_id, support_rating)
            VALUES (%s, %s)
            ON CONFLICT (employee_id) DO NOTHING;
        """, (
            int(row["employee_id"]),
            int(row["support_rating"]) if pd.notna(row["support_rating"]) else None,
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("Transformation complete. staging tables populated.")

if __name__ == "__main__":
    main()