import os
import pandas as pd
from sqlalchemy import create_engine

EXPORT_DIR = "data/final"
os.makedirs(EXPORT_DIR, exist_ok=True)

DB_NAME = "csds397"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

def main():
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    # Export each staging table individually
    tables = {
        "departments": "SELECT * FROM staging.departments ORDER BY department_id;",
        "employees": "SELECT * FROM staging.employees ORDER BY employee_id;",
        "sales_metrics": "SELECT * FROM staging.sales_metrics ORDER BY employee_id;",
        "support_metrics": "SELECT * FROM staging.support_metrics ORDER BY employee_id;",
    }

    with engine.connect() as conn:
        for table_name, query in tables.items():
            df = pd.read_sql(query, conn)
            filepath = os.path.join(EXPORT_DIR, f"{table_name}.csv")
            df.to_csv(filepath, index=False)
            print(f"Exported {len(df)} rows to {filepath}")

        # Also export a single joined view for convenience
        joined_query = """
            SELECT
                e.employee_id,
                e.name,
                e.age,
                d.department_name AS department,
                e.date_of_joining,
                e.years_of_experience,
                e.country,
                e.salary,
                e.performance_rating,
                sm.total_sales,
                su.support_rating
            FROM staging.employees e
            JOIN staging.departments d ON e.department_id = d.department_id
            LEFT JOIN staging.sales_metrics sm ON e.employee_id = sm.employee_id
            LEFT JOIN staging.support_metrics su ON e.employee_id = su.employee_id
            ORDER BY e.employee_id;
        """
        df_joined = pd.read_sql(joined_query, conn)
        joined_path = os.path.join(EXPORT_DIR, "final_dataset.csv")
        df_joined.to_csv(joined_path, index=False)
        print(f"\nExported joined dataset ({len(df_joined)} rows) to {joined_path}")

if __name__ == "__main__":
    main()