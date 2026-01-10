Data Engineering Assignment 1
Employee Data Pipeline (Spark + PostgreSQL + Docker)

Overview:
• This project implements an end-to-end employee data pipeline using Apache Spark (PySpark) and PostgreSQL, fully containerized with Docker Compose.

The pipeline demonstrates:
• realistic dirty data generation
• robust data cleaning and transformation using Spark
• strict data integrity enforcement using PostgreSQL constraints
• professional configuration and secret management using environment variables
• The final output is a clean, analytics-ready employees_clean table in PostgreSQL.

Architecture:
Raw CSV (employees_raw.csv) --> Apache Spark (PySpark ETL) --> PostgreSQL (employees_clean table)
• All components run locally inside Docker containers.

Tech Stack:
• Apache Spark 3.5 (PySpark)
• PostgreSQL 15
• Docker & Docker Compose
• Python (Faker for data generation)

Project Structure:
assignment_one/
│
├── docker-compose.yml
├── .env.example
├── README.md
│
├── data/
│   └── employees_raw.csv
│
├── jars/
│   └── postgresql-42.7.3.jar
│
├── scripts/
│   └── generate_employees_data.py
│
├── spark/
│   └── employee_etl.py
│
└── sql/
    └── init.sql

Data Generation:
• A Python script generates 1200+ employee records with intentional data quality issues:

Included issues:
• duplicate employee_id
• duplicate and invalid emails
• future hire_date
• salaries with currency symbols and commas
• mixed case text
• null values in non-critical fields

Script:
`python scripts/generate_employees_data.py`

Output:
`data/employees_raw.csv`

Environment Configuration:
• Sensitive configuration is handled via environment variables.

.env.example (committed)
`DB_HOST=postgres
DB_PORT=5432
DB_NAME=employees
DB_USER=admin
DB_PASSWORD=admin
SPARK_APP_NAME=EmployeeETL`

Setup:
cp .env.example .env

• The .env file is intentionally not committed to version control.

Database Schema:
• PostgreSQL table is created automatically on first startup:

CREATE TABLE employees_clean (
    employee_id INTEGER PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    email_domain VARCHAR(50),
    hire_date DATE NOT NULL,
    job_title VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2),
    salary_band VARCHAR(20),
    manager_id INTEGER,
    address TEXT,
    city VARCHAR(50),
    state VARCHAR(2),
    zip_code VARCHAR(10),
    birth_date DATE,
    age INTEGER,
    tenure_years DECIMAL(3,1),
    status VARCHAR(20) DEFAULT 'Active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

• The database enforces NOT NULL, PRIMARY KEY, and UNIQUE constraints.


Spark ETL Logic:-

The PySpark job performs:
Data Quality Handling
• drops rows with null critical fields
• removes duplicate employee_id
• validates email format
• removes future hire_date
• enforces email uniqueness (to satisfy DB constraint)

Transformations
• name standardization
• salary cleaning and casting
• salary band classification
• age and tenure calculation

Enrichment
• full_name
• email_domain

Design Choice:
• Invalid records are intentionally dropped rather than defaulting to preserve data integrity.

Running the Pipeline:
1. Start services
docker compose up -d

2. Run Spark ETL
docker exec -it spark /opt/spark/bin/spark-submit --jars /opt/jars/postgresql-42.7.7.jar --driver-class-path /opt/jars/postgresql-42.7.7.jar /opt/spark-apps/employee_etl.py

3. Verification:-
• Connect to PostgreSQL:
docker exec -it postgres_db psql -U admin -d employees

• Run:
SELECT COUNT(*) FROM employees_clean;

SELECT employee_id, full_name, salary
FROM employees_clean
LIMIT 5;

Expected outcome:
• Final row count < raw count (by design)
• No nulls in critical columns
• Clean, query-ready data

4. Notes on Data Loss:-
The reduction from raw records to final records is intentional and due to:
• invalid emails
• duplicate business keys
• future hire dates
• null critical fields
This demonstrates defensive data engineering, not pipeline failure.

5. Sample Data:-
• The repository includes a sample raw input file (employees_raw.csv) and a sample cleaned output (employees_clean_sample.csv) as required by the assignment.
• These files are representative examples; the pipeline can regenerate data using the provided scripts.

6. How to Re-run Cleanly
• To restart containers (data preserved):
docker compose down
docker compose up -d

• To reset everything including data:
docker compose down -v

## Demo Video
A short demo video showing environment setup, Spark ETL execution, and PostgreSQL verification:
[Demo video](https://drive.google.com/file/d/1akP4JvsKNhmSdPmXQubn3Yd3H8Z08gJy/view?usp=sharing)
