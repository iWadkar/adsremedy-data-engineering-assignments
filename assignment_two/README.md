# Customer Transactions Data Pipeline
## Assignment Two – AdsRemedy

1. Overview:
    This project implements an end-to-end data pipeline to process customer transaction data using:
    • Apache Spark for data generation, transformation, and aggregation
    • Delta Lake for reliable, ACID-compliant storage
    • ScyllaDB as the serving datastore for aggregated analytics
    • Docker for reproducible, isolated execution
    Although Apache Airflow was part of the original plan, the final submission intentionally does not include Airflow orchestration, for reasons explained transparently in Section 6.

2. Architecture:
    High-level flow:
    1. Generate synthetic customer transaction data (Spark)
    2. Store raw data as Parquet
    3. Convert raw data to a Delta Lake table
    4. Perform daily customer-level aggregations
    5. Load aggregated data into ScyllaDB

Technologies used:
    • Apache Spark 3.5.0 (Docker)
    • Delta Lake 3.1.0
    • ScyllaDB 5.4
    • Python (PySpark + Cassandra driver)
    • Docker & Docker Compose

3. Why Spark Standalone (No Master/Worker Cluster)
    This assignment uses single-container Spark execution, not a multi-node Spark master/worker setup.
    Rationale:
        • Dataset size is small and synthetic
        • No distributed resource contention required
        • Simplifies reproducibility for evaluators
        • Avoids unnecessary operational overhead
    Spark is still used correctly as a distributed processing engine, and the code is fully compatible with a cluster deployment if scaled later.

4. Project Structure:

assignment_two/
├── spark/
│   ├── generate_data.py
│   ├── create_delta_table.py
│   ├── etl_daily_totals.py
│   ├── load_to_scylla.py
│   ├── init_scylla.py
│   └── Dockerfile
│
├── data/
│   ├── _raw/
│   │   └── customer_transactions/
│   ├── customer_transactions/          # Delta table
│   └── customer_daily_aggregates/       # Delta table
│
├── docker-compose.yml
└── README.md

5. Pipeline Scripts (What Runs):
    5.1 Data Generation
      `generate_data.py`
        • Uses Spark to generate synthetic customer transactions
        • Writes raw data as Parquet to `_raw/customer_transactions`
    5.2 Delta Table Creation
        • `create_delta_table.py`
        • Reads raw Parquet files
        • Writes them as a Delta Lake table (customer_transactions)
        • Enables schema enforcement and ACID guarantees
    5.3 Daily Aggregation
        `etl_daily_totals.py`
        • Reads from the Delta table
        • Aggregates per customer per day:
            • total transaction amount
            • transaction count
        • Writes results to `customer_daily_aggregates` Delta table
    5.4 ScyllaDB Initialization
        `init_scylla.py`
            • Creates keyspace analytics
            • Creates table `customer_daily_aggregates`
            • Designed to be idempotent
    5.5 Load to ScyllaDB
        `load_to_scylla.py`
            • Reads aggregated Delta table
            • Uses `foreachPartition` to:
                • Avoid driver memory overload
                • Reuse Scylla connections efficiently
            • Inserts into ScyllaDB using prepared statements

6. Why Apache Airflow Is Not Included (Important):
    Short answer:
        `Airflow was attempted extensively (Docker + local), but could not be stabilized within the       assignment timeframe due to environment constraints and provider incompatibilities.`

    Detailed explanation:
        1. Initial laptop crash
            • Mid-assignment, my primary laptop crashed
            • Work had to be resumed on a borrowed laptop with a different OS and environment
            • This caused loss of Docker cache, volumes, and local Airflow state
        2. Dockerized Airflow issues
            • Persistent Docker volume mount failures (`invalid mount config`)
            • Ivy/Delta dependency resolution conflicts inside DockerOperator
            • Repeated failures despite correct host paths and mounts
        3. Local Airflow issues
            • Airflow requires Python 3.8–3.11
            • Borrowed system had Python 3.12 by default
            • Installing compatible Python versions was not possible via apt
            • Accidental installation of Airflow 3.x, which:
                • Breaks provider compatibility
                • Removes `airflow.__version__`
                • Causes Docker provider import failures
        4. Time vs value trade-off
            • After significant time investment, Airflow orchestration was becoming an infrastructure     problem, not a data engineering problem
            • The pipeline logic itself runs correctly end-to-end without Airflow
            • For this assignment, correctness and clarity of the data pipeline were prioritized
        Conclusion:
            Airflow was intentionally excluded to ensure a working, testable, and complete data pipeline, rather than submitting a partially broken orchestration layer.

7. How to Run the Pipeline (Final Execution):
    ```console
    # Start ScyllaDB
    docker compose up -d scylla

    # Build Spark image
    docker build -t assignment_two_spark ./spark

    # Initialize Scylla schema
    docker run --rm --network assignment_two_default \
    assignment_two_spark \
    /opt/spark/bin/spark-submit /app/init_scylla.py

    # Generate data
    docker run --rm -v ${PWD}/data:/opt/spark/delta-lake \
    assignment_two_spark \
    /opt/spark/bin/spark-submit /app/generate_data.py

    # Create Delta table
    docker run --rm -v ${PWD}/data:/opt/spark/delta-lake \
    assignment_two_spark \
    /opt/spark/bin/spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    /app/create_delta_table.py

    # Aggregate data
    docker run --rm -v ${PWD}/data:/opt/spark/delta-lake \
    assignment_two_spark \
    /opt/spark/bin/spark-submit \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    /app/etl_daily_totals.py

    # Load into ScyllaDB
    docker run --rm --network assignment_two_default \
    -v ${PWD}/data:/opt/spark/delta-lake \
    assignment_two_spark \
    /opt/spark/bin/spark-submit /app/load_to_scylla.py
    ```

8. Design Decisions Summary:
| Decision         | Reason                                     |
| ---------------- | ------------------------------------------ |
| Delta Lake       | ACID guarantees, schema enforcement        |
| foreachPartition | Efficient Scylla writes                    |
| No Spark cluster | Dataset size + simplicity                  |
| Docker           | Reproducibility                            |
| No Airflow       | Environment instability + time constraints |

9. Final Notes:
    • The pipeline is fully functional end-to-end
    • All transformations, storage, and loading logic are production-grade
    • Airflow exclusion is documented, justified, and transparent
    • This project reflects real-world trade-offs engineers make under time and environment constraints