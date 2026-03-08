import psycopg2
import pandas as pd

DB_NAME = "csds397"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

    df = pd.read_sql("SELECT * FROM raw_data;", conn)

    print("=== Shape ===")
    print(df.shape)

    print("\n=== Missing Values ===")
    print(df.isna().sum())

    print("\n=== Duplicate Rows (all columns) ===")
    print(df.duplicated().sum())

    print("\n=== Duplicate Employee IDs ===")
    print(df.duplicated(subset=["employee_id"]).sum())

    print("\n=== Column Types ===")
    print(df.dtypes)

    print("\n=== Sample Data ===")
    print(df.head())

    print("\n=== Unique Departments ===")
    print(df["department"].unique())

    print("\n=== Unique Countries ===")
    print(df["country"].unique())

    print("\n=== Date of Joining Formats (sample) ===")
    print(df["date_of_joining"].unique()[:20])

    print("\n=== Performance Rating Values ===")
    print(df["performance_rating"].value_counts())

    print("\n=== Support Rating Values ===")
    print(df["support_rating"].value_counts())

    print("\n=== Salary Stats ===")
    df["salary_numeric"] = pd.to_numeric(df["salary"], errors="coerce")
    print(df["salary_numeric"].describe())
    print(f"Non-numeric salary values: {df['salary_numeric'].isna().sum()}")

    print("\n=== Total Sales for non-Sales employees ===")
    non_sales = df[df["department"] != "Sales"]
    print(non_sales["total_sales"].value_counts())

    print("\n=== Support Rating for non-Support employees ===")
    non_support = df[df["department"] != "Support"]
    print(non_support["support_rating"].value_counts())

    conn.close()

if __name__ == "__main__":
    main()