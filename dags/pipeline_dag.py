from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.sensors.http import HttpSensor
from docker.types import Mount

from tasks.cdc.connector import ensure_connector

# ---------------------------------------------------------------------------
# DockerOperator configuration — all Spark tasks run inside the Jupyter image
# ---------------------------------------------------------------------------
_SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "quay.io/jupyter/pyspark-notebook:2025-12-31")
_HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", "")
_COMPOSE_NETWORK = os.environ.get("COMPOSE_NETWORK", "bigdata-project-3_default")
_IVY_VOLUME = "bigdata-project-3_ivy-cache"

_SPARK_ENV = {
    "KAFKA_BOOTSTRAP":      os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
    "CONNECT_URL":          os.environ.get("CONNECT_URL", "http://connect:8083"),
    "MINIO_ROOT_USER":      os.environ.get("MINIO_ROOT_USER", "admin"),
    "MINIO_ROOT_PASSWORD":  os.environ.get("MINIO_ROOT_PASSWORD", "admin123"),
    "PG_HOST":              os.environ.get("PG_HOST", "postgres"),
    "PG_PORT":              os.environ.get("PG_PORT", "5432"),
    "PG_DB":                os.environ.get("PG_DB", "sourcedb"),
    "PG_USER":              os.environ.get("PG_USER", "cdc_user"),
    "PG_PASSWORD":          os.environ.get("PG_PASSWORD", "cdc_pass"),
    "AWS_REGION":           "us-east-1",
}


def _spark_task(task_id: str, module: str, func: str) -> DockerOperator:
    code = (
        "import sys; "
        "sys.path.insert(0, '/home/jovyan/project/dags'); "
        f"from {module} import {func}; "
        f"{func}()"
    )
    return DockerOperator(
        task_id=task_id,
        image=_SPARK_IMAGE,
        command=["python", "-c", code],
        environment=_SPARK_ENV,
        mounts=[
            Mount(source=_HOST_PROJECT_DIR, target="/home/jovyan/project", type="bind"),
            Mount(source=_IVY_VOLUME, target="/home/jovyan/.ivy2", type="volume"),
        ],
        network_mode=_COMPOSE_NETWORK,
        auto_remove="success",
        mount_tmp_dir=False,
        docker_url="unix:///var/run/docker.sock",
    )


# ---------------------------------------------------------------------------
# Airflow DB connection for the Debezium HTTP sensor
# ---------------------------------------------------------------------------
def _ensure_http_connection():
    session = settings.Session()
    conn_id = "debezium_connect"
    existing = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing is None:
        conn = Connection(
            conn_id=conn_id,
            conn_type="http",
            host="connect",
            port=8083,
        )
        session.add(conn)
        session.commit()
    session.close()

_ensure_http_connection()


def _check_connector_status(response):
    data = response.json()
    return data.get("connector", {}).get("state") == "RUNNING"


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "sla": timedelta(minutes=30),
}

with DAG(
    dag_id="lakehouse_pipeline",
    description="Lakehouse: PostgreSQL CDC + Taxi -> Bronze -> Silver -> Gold (Iceberg)",
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 4, 25),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["lakehouse", "cdc", "taxi"],
) as dag:

    # --- Infrastructure Tasks ---
    t_ensure_connector = PythonOperator(
        task_id="ensure_connector",
        python_callable=ensure_connector,
    )

    connector_health = HttpSensor(
        task_id="connector_health",
        http_conn_id="debezium_connect",
        endpoint="connectors/pg-cdc-connector/status",
        response_check=_check_connector_status,
        poke_interval=30,
        timeout=120,
    )

    # --- Bronze Tasks ---
    t_bronze_cdc  = _spark_task("bronze_cdc",  "tasks.cdc.bronze",  "bronze_cdc")
    t_bronze_taxi = _spark_task("bronze_taxi", "tasks.taxi.bronze", "bronze_taxi")

    # --- Silver Tasks ---
    t_silver_cdc  = _spark_task("silver_cdc",  "tasks.cdc.silver",  "silver_cdc")
    t_silver_taxi = _spark_task("silver_taxi", "tasks.taxi.silver", "silver_taxi")

    # --- Gold Tasks ---
    t_gold_taxi      = _spark_task("gold_taxi",      "tasks.taxi.gold",           "gold_taxi")
    t_gold_anomalies = _spark_task("gold_anomalies", "tasks.taxi.gold_anomalies", "gold_taxi_anomalies")

    # --- Validation ---
    t_validate = _spark_task("validate", "tasks.cdc.validate", "validate")

    # ---------------------------------------------------------------------------
    # Task Graph Logic
    # ---------------------------------------------------------------------------
    
    # 1. Start with Infrastructure
    t_ensure_connector >> connector_health
    
    # 2. Fan out to Bronze ingestion after health check
    connector_health >> [t_bronze_cdc, t_bronze_taxi]
    
    # 3. CDC Pipeline: Bronze -> Silver
    t_bronze_cdc >> t_silver_cdc
    
    # 4. Taxi Pipeline: Bronze -> Silver -> Gold (Stats & Anomalies in parallel)
    t_bronze_taxi >> t_silver_taxi
    t_silver_taxi >> [t_gold_taxi, t_gold_anomalies]
    
    # 5. Final Validation: Wait for CDC Silver and both Taxi Gold tables
    [t_silver_cdc, t_gold_taxi, t_gold_anomalies] >> t_validate
    