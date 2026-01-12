from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

DELTA_MOUNT = Mount(
    source="/opt/airflow/data",
    target="/opt/spark/delta-lake",
    type="bind"
)

SPARK_CMD_PREFIX = (
    "/opt/spark/bin/spark-submit "
    "--conf spark.jars.ivy=/tmp/.ivy2 "
    "--packages io.delta:delta-spark_2.12:3.1.0 "
)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="customer_transactions_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_from_delta = DockerOperator(
        task_id="extract_from_delta",
        image="assignment_two_spark",
        command=SPARK_CMD_PREFIX + "/app/extract_from_delta.py",
        mounts=[DELTA_MOUNT],
        network_mode="assignment_two_default",
        auto_remove=True,
    )

    transform_data = DockerOperator(
        task_id="transform_data",
        image="assignment_two_spark",
        command=SPARK_CMD_PREFIX + "/app/etl_daily_totals.py",
        mounts=[DELTA_MOUNT],
        network_mode="assignment_two_default",
        auto_remove=True,
    )

    load_to_scylla = DockerOperator(
        task_id="load_to_scylla",
        image="assignment_two_spark",
        command=SPARK_CMD_PREFIX + "/app/load_to_scylla.py",
        mounts=[DELTA_MOUNT],
        network_mode="assignment_two_default",
        auto_remove=True,
    )

    extract_from_delta >> transform_data >> load_to_scylla
