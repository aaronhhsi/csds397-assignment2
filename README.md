# CSDS397 Assignment 2 — Data Ingestion & Cleaning Pipeline

This code implements an ELT (Extract, Load, Transform) pipeline that ingests raw employee data into a PostgreSQL database, identifies and resolves data quality issues, normalizes the data to 3rd Normal Form (3NF), and exports the cleaned data for analysis.

---

## Dependencies

The following Python packages are required:

| Package | Purpose |
|---|---|
| `pandas` | Data loading, inspection, and transformation |
| `sqlalchemy` | Database connection interface for pandas |
| `psycopg2-binary` | PostgreSQL adapter for Python |

Install all dependencies with:

```bash
pip install -r requirements.txt
```

---

## Setup

Before running any Python scripts, run the setup shell script to install PostgreSQL, start the service, configure the database user, and install Python dependencies:

```bash
bash setup.sh
```

This script will:
- Install PostgreSQL and start the service
- Create the `postgres` database user with password `postgres`
- Create the `csds397` database
- Install all Python dependencies from `requirements.txt`

---

## Pipeline Execution Order

Run the scripts in the following order:

### 1. `pipeline/01_setup_database.py` — Database Setup
Creates the `raw_data` table in PostgreSQL with all columns defined as `TEXT`. No type enforcement is applied at this stage — the table is intentionally schema-less to accept the source data exactly as it arrives.

```bash
python pipeline/01_setup_database.py
```

### 2. `pipeline/02_load_raw_data.py` — Data Ingestion
Reads `data/raw/employee_data.csv` and loads all rows into the `raw_data` table as-is, with no cleaning or transformation. This is the **Extract/Load** stage of the ELT pipeline — raw data is preserved in its original form before any changes are made.

```bash
python pipeline/02_load_raw_data.py
```

### 3. `pipeline/03_quality_checks.py` — Data Quality Inspection
Queries `raw_data` and produces a report of data quality issues including missing values, duplicate rows, duplicate employee IDs, mixed date formats, invalid rating values, and country name inconsistencies. 

**Review the output of this script before proceeding.** If additional invalid values are found (e.g. new invalid country names), update the relevant sections in `05_transform_data.py` accordingly.

```bash
python pipeline/03_quality_checks.py
```

### 4. `pipeline/04_create_staging_tables.py` — Staging Table Creation
Creates the normalized 3NF staging tables:

- `departments` — lookup table for department names
- `employees` — core employee attributes with correct data types and constraints
- `sales_metrics` — total sales figures for Sales department employees only
- `support_metrics` — support ratings for Support department employees only

```bash
python pipeline/04_create_staging_tables.py
```

### 5. `pipeline/05_transform_data.py` — Data Cleaning & Transformation
Reads from `raw_data`, applies all cleaning and transformation steps, and populates the staging tables. This is the **Transform** stage of the ELT pipeline. The following operations are performed:

- Removes fully duplicate rows
- Removes duplicate employee IDs (keeping first occurrence)
- Drops rows with invalid or missing departments
- Drops rows missing critical fields (`employee_id`, `name`, `department`)
- Normalizes mixed date formats (e.g. `01/01/2017` and `2015-01-01`) to `YYYY-MM-DD`
- Casts all columns to their correct data types
- NULLs out invalid performance and support ratings (outside 1–5 range)
- NULLs out invalid age and salary values
- Normalizes country name casing (e.g. `kaldora` → `Kaldora`)
- Routes `total_sales` only to Sales employees and `support_rating` only to Support employees

```bash
python pipeline/05_transform_data.py
```

### 6. `pipeline/06_export_clean_data.py` — Export
Exports each staging table to a CSV file in `data/final/`, plus a single joined `final_dataset.csv` that combines all four tables for convenience.

```bash
python pipeline/06_export_clean_data.py
```

Output files:
- `data/final/departments.csv`
- `data/final/employees.csv`
- `data/final/sales_metrics.csv`
- `data/final/support_metrics.csv`
- `data/final/final_dataset.csv`

---

## Database Schema (3NF)

```
departments     (department_id, department_name)
employees       (employee_id, name, age, department_id, date_of_joining,
                 years_of_experience, country, salary, performance_rating)
sales_metrics   (employee_id, total_sales)
support_metrics (employee_id, support_rating)
```

- `employees.department_id` references `departments.department_id`
- `sales_metrics.employee_id` references `employees.employee_id`
- `support_metrics.employee_id` references `employees.employee_id`

---

## Project Structure

```
csds397-assignment2/
├── data/
│   ├── raw/
│   │   └── employee_data.csv
│   └── final/
│       ├── departments.csv
│       ├── employees.csv
│       ├── sales_metrics.csv
│       ├── support_metrics.csv
│       └── final_dataset.csv
├── pipeline/
│   ├── 01_setup_database.py
│   ├── 02_load_raw_data.py
│   ├── 03_quality_checks.py
│   ├── 04_create_staging_tables.py
│   ├── 05_transform_data.py
│   └── 06_export_clean_data.py
├── requirements.txt
├── setup.sh
└── README.md
```
