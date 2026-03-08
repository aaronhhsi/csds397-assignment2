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

    df = pd.read_sql("SELECT * FROM raw_data;", conn)
    print(f"Loaded {len(df)} rows from raw_data.")

    df = df.drop_duplicates()
    print(f"After removing duplicate rows: {len(df)}")

    df = df.drop_duplicates(subset=["employee_id"])
    print(f"After removing duplicate employee IDs: {len(df)}")

    invalid_dept = ~df["department"].isin(VALID_DEPARTMENTS)
    print(f"Rows with invalid department dropped: {invalid_dept.sum()}")
    df = df[~invalid_dept]

    df = df.dropna(subset=["employee_id", "name", "department"])
    print(f"After dropping rows missing critical fields: {len(df)}")

    df["date_of_joining"] = df["date_of_joining"].apply(normalize_date)

    df["employee_id"] = pd.to_numeric(df["employee_id"], errors="coerce")
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df["years_of_experience"] = pd.to_numeric(df["years_of_experience"], errors="coerce")
    df["salary"] = pd.to_numeric(df["salary"], errors="coerce")
    df["performance_rating"] = pd.to_numeric(df["performance_rating"], errors="coerce")
    df["total_sales"] = pd.to_numeric(df["total_sales"], errors="coerce")
    df["support_rating"] = pd.to_numeric(df["support_rating"], errors="coerce")

    df.loc[~df["performance_rating"].between(1, 5), "performance_rating"] = None
    df.loc[~df["support_rating"].between(1, 5), "support_rating"] = None
    df.loc[df["age"] <= 0, "age"] = None
    df.loc[df["salary"] < 0, "salary"] = None

    df["country"] = df["country"].str.strip().str.title()
    cur.execute("TRUNCATE TABLE support_metrics, sales_metrics, employees, departments RESTART IDENTITY CASCADE;")

    for dept in df["department"].unique():
        cur.execute(
            "INSERT INTO departments (department_name) VALUES (%s) ON CONFLICT (department_name) DO NOTHING;",
            (dept,)
        )

    cur.execute("SELECT department_id, department_name FROM departments;")
    dept_map = {name: did for did, name in cur.fetchall()}

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO employees (
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

    for _, row in df[df["department"] == "Sales"].iterrows():
        cur.execute("""
            INSERT INTO sales_metrics (employee_id, total_sales)
            VALUES (%s, %s)
            ON CONFLICT (employee_id) DO NOTHING;
        """, (
            int(row["employee_id"]),
            float(row["total_sales"]) if pd.notna(row["total_sales"]) else None,
        ))

    for _, row in df[df["department"] == "Support"].iterrows():
        cur.execute("""
            INSERT INTO support_metrics (employee_id, support_rating)
            VALUES (%s, %s)
            ON CONFLICT (employee_id) DO NOTHING;
        """, (
            int(row["employee_id"]),
            int(row["support_rating"]) if pd.notna(row["support_rating"]) else None,
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("Transformation complete. Staging tables populated.")

if __name__ == "__main__":
    main()