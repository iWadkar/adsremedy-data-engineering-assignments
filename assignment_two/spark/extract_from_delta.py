from pyspark.sql import SparkSession

DELTA_PATH = "/opt/spark/delta-lake/customer_transactions"

spark = (
    SparkSession.builder
    .appName("extract-delta-sanity")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# minimal read to validate availability
spark.read.format("delta").load(DELTA_PATH).limit(1).count()

print("Delta source validated")

spark.stop()
