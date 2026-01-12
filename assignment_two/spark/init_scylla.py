from cassandra.cluster import Cluster

cluster = Cluster(["scylla"], port=9042)
session = cluster.connect()

session.execute("""
CREATE KEYSPACE IF NOT EXISTS analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.execute("""
CREATE TABLE IF NOT EXISTS analytics.customer_daily_aggregates (
    customer_id text,
    transaction_date date,
    total_amount double,
    transaction_count int,
    PRIMARY KEY (customer_id, transaction_date)
) WITH CLUSTERING ORDER BY (transaction_date DESC)
""")

session.shutdown()
cluster.shutdown()
