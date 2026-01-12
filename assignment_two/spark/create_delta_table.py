from pyspark.sql import SparkSession

RAW_PATH = "/opt/spark/delta-lake/_raw/customer_transactions"
DELTA_PATH = "/opt/spark/delta-lake/customer_transactions"

spark = (
    SparkSession.builder
    .appName("create-delta-customer-transactions")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Read raw parquet
df = spark.read.parquet(RAW_PATH)

print(f"Raw records read: {df.count()}")

# Write Delta table
(
    df.write
      .format("delta")
      .mode("overwrite")
      .save(DELTA_PATH)
)

# Register table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS customer_transactions
    USING DELTA
    LOCATION '{DELTA_PATH}'
""")

print("Delta table created")

# Verify Delta history
spark.sql("DESCRIBE HISTORY customer_transactions").show(truncate=False)

spark.stop()
