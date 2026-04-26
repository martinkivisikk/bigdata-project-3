from __future__ import annotations

import logging

from tasks.config import BOOTSTRAP, CDC_TOPICS
from tasks.spark import create_spark_session, starting_offsets

log = logging.getLogger(__name__)


def bronze_cdc(**ctx):
    spark = create_spark_session("bronze_cdc")
    from pyspark.sql import functions as F
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.cdc")
        spark.sql("USE lakehouse.cdc")

        # ── customers ──────────────────────────────────────────────────
        spark.sql("""
            CREATE TABLE IF NOT EXISTS bronze_customers (
                kafka_offset        LONG,
                kafka_partition     INT,
                kafka_timestamp     TIMESTAMP,
                op                  STRING,
                event_type          STRING,
                before_id           INT,
                before_name         STRING,
                before_email        STRING,
                before_country      STRING,
                after_id            INT,
                after_name          STRING,
                after_email         STRING,
                after_country       STRING,
                source_lsn          LONG,
                ts_ms               LONG
            ) USING iceberg
        """)
        offsets = starting_offsets(spark, "lakehouse.cdc.bronze_customers",
                                  CDC_TOPICS["customers"])
        log.info("bronze_customers startingOffsets: %s", offsets)

        cust_raw = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", CDC_TOPICS["customers"])
            .option("startingOffsets", offsets)
            .load()
        )
        cust_parsed = cust_raw.select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("raw_value"),
        ).select(
            "kafka_offset", "kafka_partition", "kafka_timestamp",
            F.when(F.col("raw_value").isNull(), "tombstone")
             .otherwise("cdc_event").alias("event_type"),
            F.get_json_object("raw_value", "$.payload.op").alias("op"),
            F.get_json_object("raw_value", "$.payload.before.id").cast("int").alias("before_id"),
            F.get_json_object("raw_value", "$.payload.before.name").alias("before_name"),
            F.get_json_object("raw_value", "$.payload.before.email").alias("before_email"),
            F.get_json_object("raw_value", "$.payload.before.country").alias("before_country"),
            F.get_json_object("raw_value", "$.payload.after.id").cast("int").alias("after_id"),
            F.get_json_object("raw_value", "$.payload.after.name").alias("after_name"),
            F.get_json_object("raw_value", "$.payload.after.email").alias("after_email"),
            F.get_json_object("raw_value", "$.payload.after.country").alias("after_country"),
            F.get_json_object("raw_value", "$.payload.source.lsn").cast("long").alias("source_lsn"),
            F.get_json_object("raw_value", "$.payload.ts_ms").cast("long").alias("ts_ms"),
        )

        n_cust = cust_parsed.count()
        if n_cust > 0:
            cust_parsed.writeTo("bronze_customers").append()
            log.info("bronze_customers: appended %d rows", n_cust)
        else:
            log.info("bronze_customers: no new rows")

        # ── drivers ────────────────────────────────────────────────────
        spark.sql("""
            CREATE TABLE IF NOT EXISTS bronze_drivers (
                kafka_offset            LONG,
                kafka_partition         INT,
                kafka_timestamp         TIMESTAMP,
                op                      STRING,
                event_type              STRING,
                before_id               INT,
                before_name             STRING,
                before_license_number   STRING,
                before_rating           STRING,
                before_city             STRING,
                before_active           BOOLEAN,
                after_id                INT,
                after_name              STRING,
                after_license_number    STRING,
                after_rating            STRING,
                after_city              STRING,
                after_active            BOOLEAN,
                source_lsn              LONG,
                ts_ms                   LONG
            ) USING iceberg
        """)
        offsets = starting_offsets(spark, "lakehouse.cdc.bronze_drivers",
                                  CDC_TOPICS["drivers"])
        log.info("bronze_drivers startingOffsets: %s", offsets)

        drv_raw = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", CDC_TOPICS["drivers"])
            .option("startingOffsets", offsets)
            .load()
        )
        drv_parsed = drv_raw.select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("raw_value"),
        ).select(
            "kafka_offset", "kafka_partition", "kafka_timestamp",
            F.when(F.col("raw_value").isNull(), "tombstone")
             .otherwise("cdc_event").alias("event_type"),
            F.get_json_object("raw_value", "$.payload.op").alias("op"),
            F.get_json_object("raw_value", "$.payload.before.id").cast("int").alias("before_id"),
            F.get_json_object("raw_value", "$.payload.before.name").alias("before_name"),
            F.get_json_object("raw_value", "$.payload.before.license_number").alias("before_license_number"),
            F.get_json_object("raw_value", "$.payload.before.rating").alias("before_rating"),
            F.get_json_object("raw_value", "$.payload.before.city").alias("before_city"),
            F.get_json_object("raw_value", "$.payload.before.active").cast("boolean").alias("before_active"),
            F.get_json_object("raw_value", "$.payload.after.id").cast("int").alias("after_id"),
            F.get_json_object("raw_value", "$.payload.after.name").alias("after_name"),
            F.get_json_object("raw_value", "$.payload.after.license_number").alias("after_license_number"),
            F.get_json_object("raw_value", "$.payload.after.rating").alias("after_rating"),
            F.get_json_object("raw_value", "$.payload.after.city").alias("after_city"),
            F.get_json_object("raw_value", "$.payload.after.active").cast("boolean").alias("after_active"),
            F.get_json_object("raw_value", "$.payload.source.lsn").cast("long").alias("source_lsn"),
            F.get_json_object("raw_value", "$.payload.ts_ms").cast("long").alias("ts_ms"),
        )

        n_drv = drv_parsed.count()
        if n_drv > 0:
            drv_parsed.writeTo("bronze_drivers").append()
            log.info("bronze_drivers: appended %d rows", n_drv)
        else:
            log.info("bronze_drivers: no new rows")

    finally:
        spark.stop()
