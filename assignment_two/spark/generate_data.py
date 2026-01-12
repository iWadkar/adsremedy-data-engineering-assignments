from faker import Faker
import random
import uuid
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, TimestampType
)

spark = (
    SparkSession.builder
    .appName("generate-customer-transactions")
    .getOrCreate()
)


fake = Faker()

NUM_RECORDS = 1200
MERCHANTS = ["STORE_01", "STORE_02", "ONLINE_SHOP", "GAS_STATION", "RESTAURANT"]

rows = []

base_time = datetime.utcnow()

for i in range(NUM_RECORDS):
    txn_id = str(uuid.uuid4())

    # introduce duplicates (~5%)
    if i % 20 == 0:
        txn_id = rows[-1][0] if rows else txn_id

    amount = round(random.uniform(-50, 500), 2)  # includes negative & zero
    customer_id = f"C{random.randint(1000, 1100)}"

    ts = base_time - timedelta(
        days=random.randint(0, 5),
        hours=random.randint(0, 23)
    )

    merchant = random.choice(MERCHANTS)

    rows.append((txn_id, customer_id, amount, ts, merchant))

schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", FloatType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("merchant", StringType(), False),
])

df = spark.createDataFrame(rows, schema=schema)

output_path = "/opt/spark/delta-lake/_raw/customer_transactions"

record_count = df.count()
print(f"About to write {record_count} records")

(
    df.coalesce(4)   # avoid tiny files
      .write
      .mode("overwrite")
      .parquet(output_path)
)

written_df = spark.read.parquet(output_path)
print(f"Written records: {written_df.count()}")

print(f"Generated {df.count()} records")

spark.stop()
