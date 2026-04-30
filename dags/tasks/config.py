from __future__ import annotations

import importlib.util
import os
import subprocess
import sys

# ---------------------------------------------------------------------------
# Self-install Python dependencies that the Airflow image doesn't ship with.
# Called lazily at task execution time (NOT at DAG parse time).
# ---------------------------------------------------------------------------
def _ensure_packages():
    needed = {"pyspark": "pyspark", "psycopg2": "psycopg2-binary", "kafka": "kafka-python-ng"}
    for mod, pip_name in needed.items():
        if importlib.util.find_spec(mod) is None:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "-q", pip_name],
            )


# ---------------------------------------------------------------------------
# Environment & constants
# ---------------------------------------------------------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083")

S3_ENDPOINT = "http://minio:9000"
S3_BUCKET = "s3://warehouse"
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "admin123")

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", 5432))
PG_DB = os.environ.get("PG_DB", "sourcedb")
PG_USER = os.environ.get("PG_USER", "cdc_user")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "cdc_pass")

CDC_TOPICS = {
    "customers": "dbserver1.public.customers",
    "drivers": "dbserver1.public.drivers",
}
TAXI_TOPIC = "taxi-trips"

SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
    "org.apache.iceberg:iceberg-aws-bundle:1.10.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
])

ZONE_LOOKUP_PATH = os.environ.get(
    "ZONE_LOOKUP_PATH", "/home/jovyan/project/data/taxi_zone_lookup.parquet"
)
