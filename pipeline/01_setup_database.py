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

    cur.execute("DROP TABLE IF EXISTS raw_data CASCADE;")

    cur.execute("""
        CREATE TABLE raw_data (
            id SERIAL PRIMARY KEY,
            employee_id TEXT,
            name TEXT,
            age TEXT,
            department TEXT,
            date_of_joining TEXT,
            years_of_experience TEXT,
            country TEXT,
            salary TEXT,
            performance_rating TEXT,
            total_sales TEXT,
            support_rating TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("raw_data table created.")

if __name__ == "__main__":
    main()