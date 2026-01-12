from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
import time


DELTA_PATH = "/opt/spark/delta-lake/customer_daily_aggregates"
SCYLLA_HOST = "scylla"
KEYSPACE = "analytics"
TABLE = "customer_daily_aggregates"


def wait_for_scylla(host, retries=10, delay=5):
    for i in range(retries):
        try:
            cluster = Cluster([host], port=9042)
            session = cluster.connect()
            session.execute("SELECT now() FROM system.local")
            session.shutdown()
            cluster.shutdown()
            print("Scylla is ready")
            return
        except Exception:
            print(f"Waiting for Scylla... ({i+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Scylla not ready")

wait_for_scylla("scylla")


spark = (
    SparkSession.builder
    .appName("load-delta-to-scylla")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


df = spark.read.format("delta").load(DELTA_PATH)

row_count = df.count()
print(f"Rows to load: {row_count}")

def write_partition(rows):
    from cassandra.cluster import Cluster
    import time

    for attempt in range(5):
        try:
            cluster = Cluster(["scylla"], port=9042)
            session = cluster.connect(KEYSPACE)
            break
        except Exception:
            time.sleep(3)
    else:
        raise RuntimeError("Scylla not reachable from executor")

    prepared = session.prepare("""
        INSERT INTO customer_daily_aggregates
        (customer_id, transaction_date, total_amount, transaction_count)
        VALUES (?, ?, ?, ?)
    """)

    for r in rows:
        session.execute(prepared, (
            r.customer_id,
            r.transaction_date,
            float(r.total_amount),
            int(r.transaction_count)
        ))

    session.shutdown()
    cluster.shutdown()


df = df.repartition(2)

df.foreachPartition(write_partition)

print("Load to Scylla complete")

spark.stop()
