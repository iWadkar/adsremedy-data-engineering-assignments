import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, initcap, regexp_replace,
    when, current_date, datediff, floor, split
)
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import concat_ws, trim


# Environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
APP_NAME = os.getenv("SPARK_APP_NAME", "EmployeeETL")

missing = [k for k, v in {
    "DB_HOST": DB_HOST,
    "DB_NAME": DB_NAME,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASSWORD
}.items() if not v]

if missing:
    raise RuntimeError(f"Missing required env vars: {missing}")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"


spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .config("spark.jars", "/opt/jars/postgresql-42.7.3.jar")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Reading raw input data
input_path = "/opt/data/employees_raw.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print(f"Raw count: {df.count()}")

# Removing records that cannot satisfy NOT NULL DB constraints
required_cols = [
    "employee_id",
    "first_name",
    "last_name",
    "email",
    "hire_date"
]

df = df.dropna(subset=required_cols)


# Deduplicating based on employee_id
df = df.dropDuplicates(["employee_id"])


# Email validation
email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
df = df.filter(col("email").rlike(email_regex))


# Date validation
df = df.filter(col("hire_date") <= current_date())


# Name standardization
df = (
    df.withColumn("first_name", initcap(lower(col("first_name"))))
      .withColumn("last_name", initcap(lower(col("last_name"))))
)


# Salary cleaning
df = df.withColumn(
    "salary",
    regexp_replace(col("salary"), r"[₹$,]", "").cast(DecimalType(10, 2))
)


# Salary band
df = df.withColumn(
    "salary_band",
    when(col("salary") < 50000, "Junior")
    .when((col("salary") >= 50000) & (col("salary") <= 80000), "Mid")
    .otherwise("Senior")
)


# Data enrichment
df = (
    df.withColumn("full_name", trim(concat_ws(" ", col("first_name"), col("last_name"))))
      .withColumn("email", lower(col("email")))
      .withColumn("email_domain", split(col("email"), "@").getItem(1))
)

# Handling duplicates in email to satisfy DB constraint
df = df.dropDuplicates(["email"])

# Age & tenure calculation
df = (
    df.withColumn("age", floor(datediff(current_date(), col("birth_date")) / 365))
      .withColumn(
          "tenure_years",
          (datediff(current_date(), col("hire_date")) / 365).cast(DecimalType(3, 1))
      )
)


# Status normalization
df = df.withColumn(
    "status",
    when(lower(col("status")) == "inactive", "Inactive").otherwise("Active")
)

print(f"Clean count: {df.count()}")

final_df = df.select(
    "employee_id",
    "first_name",
    "last_name",
    "full_name",
    "email",
    "email_domain",
    "hire_date",
    "job_title",
    "department",
    "salary",
    "salary_band",
    "manager_id",
    "address",
    "city",
    "state",
    "zip_code",
    "birth_date",
    "age",
    "tenure_years",
    "status"
)


# Write to PostgreSQL
(
    final_df.write
    .mode("append")
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "employees_clean")
    .option("user", DB_USER)
    .option("password", DB_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .save()
)

print("Data successfully loaded into PostgreSQL")

spark.stop()