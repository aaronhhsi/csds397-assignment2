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

    cur.execute("DROP TABLE IF EXISTS support_metrics CASCADE;")
    cur.execute("DROP TABLE IF EXISTS sales_metrics CASCADE;")
    cur.execute("DROP TABLE IF EXISTS employees CASCADE;")
    cur.execute("DROP TABLE IF EXISTS departments CASCADE;")

    cur.execute("""
        CREATE TABLE departments (
            department_id SERIAL PRIMARY KEY,
            department_name TEXT NOT NULL UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE employees (
            employee_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER CHECK (age > 0 AND age < 100),
            department_id INTEGER REFERENCES departments(department_id),
            date_of_joining DATE,
            years_of_experience INTEGER CHECK (years_of_experience >= 0),
            country TEXT,
            salary NUMERIC(12, 2) CHECK (salary >= 0),
            performance_rating INTEGER CHECK (performance_rating BETWEEN 1 AND 5)
        );
    """)

    cur.execute("""
        CREATE TABLE sales_metrics (
            employee_id INTEGER PRIMARY KEY REFERENCES employees(employee_id),
            total_sales NUMERIC(12, 2) CHECK (total_sales >= 0)
        );
    """)

    cur.execute("""
        CREATE TABLE support_metrics (
            employee_id INTEGER PRIMARY KEY REFERENCES employees(employee_id),
            support_rating INTEGER CHECK (support_rating BETWEEN 1 AND 5)
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Staging tables created: departments, employees, sales_metrics, support_metrics.")

if __name__ == "__main__":
    main()