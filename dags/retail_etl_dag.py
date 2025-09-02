from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Normalize Windows path for Docker mounts
PROJ = (os.environ.get("AIRFLOW_PROJ_DIR") or "/opt/airflow").strip('"').replace("\\", "/").rstrip("/")

DATA_DIR = "/opt/airflow/data"
ARCHIVE_DIR = "/opt/airflow/archive"
SCRIPT_PATH = "/opt/airflow/dags/retail_project/RetailToPostgres.py"

# Put the task container on the same Compose network so 'postgres' resolves
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "retailetlproject_default")

default_args = {"owner": "retail", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="retail_etl_daily",
    description="Process next CSV batch with Spark, then archive it",
    start_date=datetime(2025, 8, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["retail", "etl", "spark"],
) as dag:

    # read creds from the Airflow host environment (or fallback to common compose defaults)
    PG_USER = os.environ.get("PG_USER") or os.environ.get("POSTGRES_USER") or "postgres"
    PG_PASS = os.environ.get("PG_PASS") or os.environ.get("POSTGRES_PASSWORD") or "postgres"
    PG_DB   = os.environ.get("PG_DB")   or os.environ.get("POSTGRES_DB")   or "retail"
    PG_HOST = os.environ.get("PG_HOST", "postgres")
    PG_PORT = os.environ.get("PG_PORT", "5432")


    process_next_batch = DockerOperator(
        task_id="process_next_batch",
        image="retail-spark:3.5.1",            # your custom Spark image
        api_version="auto",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode=DOCKER_NETWORK,           # <<< key: same network as compose
        tty=True,
        environment={
            "JAVA_HOME": "/opt/bitnami/java",
            "PYSPARK_PYTHON": "/opt/bitnami/python/bin/python",
            "PYSPARK_DRIVER_PYTHON": "/opt/bitnami/python/bin/python",

            # DB connection
            "PG_HOST": PG_HOST,
            "PG_PORT": PG_PORT,
            "PG_DB":   PG_DB,
            "PG_USER": PG_USER,
            "PG_PASS": PG_PASS,
        },
        mount_tmp_dir=False,  # important on Windows
        command=(
            "bash -lc 'set -euo pipefail; "
            f"file=$(ls -1 {DATA_DIR}/retail_batch_*.csv 2>/dev/null | head -n 1 || true); "
            'if [ -z \"$file\" ]; then echo \"No new files found\"; exit 0; fi; '
            "echo Processing: $file; "
            # ensure psycopg2 is available for DDL/upsert
            "/opt/bitnami/python/bin/python -c \"import psycopg2\" 2>/dev/null || "
            "/opt/bitnami/python/bin/pip install --no-cache-dir psycopg2-binary==2.9.9; "
            # run Spark job (includes Postgres JDBC)
            "/opt/bitnami/spark/bin/spark-submit "
            "--master local[2] "
            "--conf spark.pyspark.python=/opt/bitnami/python/bin/python "
            "--conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python "
            "--packages org.postgresql:postgresql:42.7.3 "
            f"{SCRIPT_PATH} \"$file\"; "
            f"mv \"$file\" {ARCHIVE_DIR}/'"
        ),
        mounts=[
            Mount(source=f"{PROJ}/dags",    target="/opt/airflow/dags",    type="bind"),
            Mount(source=f"{PROJ}/data",    target="/opt/airflow/data",    type="bind"),
            Mount(source=f"{PROJ}/archive", target="/opt/airflow/archive", type="bind"),
        ],
        working_dir="/opt/airflow/dags",
    )
