from __future__ import annotations

import logging

from tasks.config import BOOTSTRAP, TAXI_TOPIC, _ensure_packages
from tasks.spark import create_spark_session, starting_offsets

log = logging.getLogger(__name__)


def bronze_taxi(**ctx):
    _ensure_packages()
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    existing_topics = consumer.topics()
    consumer.close()
    if TAXI_TOPIC not in existing_topics:
        log.info("Topic %s not yet created — no taxi data to ingest, skipping", TAXI_TOPIC)
        return

    spark = create_spark_session("bronze_taxi")
    from pyspark.sql import functions as F
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.taxi.bronze_trips (
                key         STRING,
                value       STRING,
                topic       STRING,
                partition   INT,
                offset      BIGINT,
                timestamp   TIMESTAMP
            ) USING iceberg
        """)

        offsets = starting_offsets(
            spark, "lakehouse.taxi.bronze_trips", TAXI_TOPIC,
            partition_col="partition", offset_col="offset",
        )
        log.info("bronze_trips startingOffsets: %s", offsets)

        raw = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", TAXI_TOPIC)
            .option("startingOffsets", offsets)
            .load()
        )

        parsed = raw.select(
            F.col("key").cast("string"),
            F.col("value").cast("string"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp"),
        ).cache()

        try:
            n = parsed.count()
            if n > 0:
                parsed.writeTo("lakehouse.taxi.bronze_trips").append()
                log.info("bronze_trips: appended %d rows", n)
            else:
                log.info("bronze_trips: no new rows")
        finally:
            parsed.unpersist()
    finally:
        spark.stop()
