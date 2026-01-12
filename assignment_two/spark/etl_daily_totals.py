from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum

SOURCE_DELTA_PATH = "/opt/spark/delta-lake/customer_transactions"
TARGET_DELTA_PATH = "/opt/spark/delta-lake/customer_daily_aggregates"

spark = (
    SparkSession.builder
    .appName("etl-daily-customer-aggregation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Read source Delta table
df = spark.read.format("delta").load(SOURCE_DELTA_PATH)

print(f"Input records: {df.count()}")

# 1. Deduplicate
df_dedup = df.dropDuplicates(["transaction_id"])

# 2. Filter invalid amounts
df_valid = df_dedup.filter(col("amount") > 0)

# 3. Add transaction_date
df_enriched = df_valid.withColumn(
    "transaction_date",
    to_date(col("timestamp"))
)

# 4. Aggregate daily totals per customer
df_agg = (
    df_enriched
    .groupBy("customer_id", "transaction_date")
    .agg(
        _sum("amount").alias("total_amount"),
        _sum(col("amount") * 0 + 1).alias("transaction_count")
    )
)

print(f"Aggregated rows: {df_agg.count()}")

# 5. Write aggregated Delta table
(
    df_agg.write
      .format("delta")
      .mode("overwrite")
      .save(TARGET_DELTA_PATH)
)

# Register table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS customer_daily_aggregates
    USING DELTA
    LOCATION '{TARGET_DELTA_PATH}'
""")

# Verify history
spark.sql("DESCRIBE HISTORY customer_daily_aggregates").show(truncate=False)

spark.stop()
