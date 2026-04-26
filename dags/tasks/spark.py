from __future__ import annotations

import json
import logging

from tasks.config import (
    BOOTSTRAP, MINIO_PASS, MINIO_USER, S3_BUCKET, S3_ENDPOINT,
    SPARK_PACKAGES, _ensure_packages,
)

log = logging.getLogger(__name__)


def create_spark_session(app_name: str = "airflow_lakehouse"):
    import os
    _ensure_packages()
    os.makedirs("/home/jovyan/.ivy2/cache", exist_ok=True)
    os.makedirs("/home/jovyan/.ivy2/jars", exist_ok=True)
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.jars.ivy", os.path.expanduser("~/.ivy2"))
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl",
                "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.lakehouse.warehouse", S3_BUCKET)
        .config("spark.sql.catalog.lakehouse.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", S3_ENDPOINT)
        .config("spark.sql.catalog.lakehouse.s3.access-key-id", MINIO_USER)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", MINIO_PASS)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def starting_offsets(spark, table_fqn: str, topic: str,
                     partition_col: str = "kafka_partition",
                     offset_col: str = "kafka_offset") -> str:
    """Build a Kafka startingOffsets JSON from the max offset already stored
    in the bronze Iceberg table.  Falls back to ``"earliest"`` when the
    table doesn't exist or is empty."""
    try:
        df = spark.sql(
            f"SELECT `{partition_col}`, MAX(`{offset_col}`) AS max_off "
            f"FROM {table_fqn} GROUP BY `{partition_col}`"
        )
        rows = df.collect()
        if not rows:
            return "earliest"
        offsets = {str(r[partition_col]): r["max_off"] + 1 for r in rows}
        return json.dumps({topic: offsets})
    except Exception:
        return "earliest"
