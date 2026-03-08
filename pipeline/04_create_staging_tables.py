import psycopg2

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
    cur = conn.cursor()

    # Create staging schema
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")

    # Drop staging tables in reverse dependency order
    cur.execute("DROP TABLE IF EXISTS staging.support_metrics CASCADE;")
    cur.execute("DROP TABLE IF EXISTS staging.sales_metrics CASCADE;")
    cur.execute("DROP TABLE IF EXISTS staging.employees CASCADE;")
    cur.execute("DROP TABLE IF EXISTS staging.departments CASCADE;")

    # Departments lookup table
    cur.execute("""
        CREATE TABLE staging.departments (
            department_id SERIAL PRIMARY KEY,
            department_name TEXT NOT NULL UNIQUE
        );
    """)

    # Employees — core attributes, typed and constrained
    cur.execute("""
        CREATE TABLE staging.employees (
            employee_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER CHECK (age > 0 AND age < 100),
            department_id INTEGER REFERENCES staging.departments(department_id),
            date_of_joining DATE,
            years_of_experience INTEGER CHECK (years_of_experience >= 0),
            country TEXT,
            salary NUMERIC(12, 2) CHECK (salary >= 0),
            performance_rating INTEGER CHECK (performance_rating BETWEEN 1 AND 5)
        );
    """)

    # Sales metrics — Sales employees only
    cur.execute("""
        CREATE TABLE staging.sales_metrics (
            employee_id INTEGER PRIMARY KEY REFERENCES staging.employees(employee_id),
            total_sales NUMERIC(12, 2) CHECK (total_sales >= 0)
        );
    """)

    # Support metrics — Support employees only
    cur.execute("""
        CREATE TABLE staging.support_metrics (
            employee_id INTEGER PRIMARY KEY REFERENCES staging.employees(employee_id),
            support_rating INTEGER CHECK (support_rating BETWEEN 1 AND 5)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Staging tables created: staging.departments, staging.employees, staging.sales_metrics, staging.support_metrics.")

if __name__ == "__main__":
    main()