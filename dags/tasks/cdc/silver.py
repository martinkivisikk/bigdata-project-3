from __future__ import annotations

import logging

from tasks.spark import create_spark_session

log = logging.getLogger(__name__)


def silver_cdc(**ctx):
    spark = create_spark_session("silver_cdc")
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.cdc")
        spark.sql("USE lakehouse.cdc")

        w = Window.partitionBy("entity_id").orderBy(
            F.col("ts_ms").desc(), F.col("kafka_offset").desc()
        )

        # ── silver_customers ───────────────────────────────────────────
        spark.sql("""
            CREATE TABLE IF NOT EXISTS silver_customers (
                id               INT,
                name             STRING,
                email            STRING,
                country          STRING,
                last_updated_ms  BIGINT
            ) USING iceberg
        """)

        bronze_cust = spark.table("lakehouse.cdc.bronze_customers")
        deduped_cust = (
            bronze_cust
            .filter(F.col("op").isNotNull())
            .filter(F.col("event_type") == "cdc_event")
            .withColumn("entity_id", F.coalesce(F.col("after_id"), F.col("before_id")))
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        deduped_cust = spark.createDataFrame(deduped_cust.collect(), deduped_cust.schema)
        deduped_cust.createOrReplaceTempView("cdc_batch")
        spark.sql("""
            MERGE INTO lakehouse.cdc.silver_customers AS t
            USING cdc_batch AS s
            ON t.id = s.entity_id
            WHEN MATCHED AND s.op = 'd' THEN DELETE
            WHEN MATCHED AND s.op IN ('c', 'u', 'r') THEN UPDATE SET
                t.name            = s.after_name,
                t.email           = s.after_email,
                t.country         = s.after_country,
                t.last_updated_ms = s.ts_ms
            WHEN NOT MATCHED AND s.op != 'd' THEN INSERT
                (id, name, email, country, last_updated_ms)
                VALUES (s.after_id, s.after_name, s.after_email, s.after_country, s.ts_ms)
        """)
        sc = spark.sql("SELECT count(*) AS n FROM lakehouse.cdc.silver_customers").collect()[0]["n"]
        log.info("silver_customers: %d rows after MERGE", sc)

        # ── silver_drivers ─────────────────────────────────────────────
        spark.sql("""
            CREATE TABLE IF NOT EXISTS silver_drivers (
                id               INT,
                name             STRING,
                license_number   STRING,
                city             STRING,
                rating           DECIMAL(3,2),
                active           BOOLEAN,
                last_updated_ms  BIGINT
            ) USING iceberg
        """)

        driver_columns = [
            row["col_name"]
            for row in spark.sql("DESCRIBE lakehouse.cdc.silver_drivers").collect()
        ]
    
        if "active" not in driver_columns:
            spark.sql("ALTER TABLE lakehouse.cdc.silver_drivers ADD COLUMNS (active BOOLEAN)")

        bronze_drv = spark.table("lakehouse.cdc.bronze_drivers")
        deduped_drv = (
            bronze_drv
            .filter(F.col("op").isNotNull())
            .filter(F.col("event_type") == "cdc_event")
            .withColumn("entity_id", F.coalesce(F.col("after_id"), F.col("before_id")))
            .withColumn(
                "rating",
                F.conv(
                    F.hex(F.unbase64(F.coalesce(F.col("after_rating"), F.col("before_rating")))),
                    16, 10,
                ).cast("int") / 100.0,
            )
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        deduped_drv = spark.createDataFrame(deduped_drv.collect(), deduped_drv.schema)
        deduped_drv.createOrReplaceTempView("cdc_drivers_batch")
        spark.sql("""
            MERGE INTO lakehouse.cdc.silver_drivers AS t
            USING cdc_drivers_batch AS s
            ON t.id = s.entity_id
            WHEN MATCHED AND s.op = 'd' THEN DELETE
            WHEN MATCHED AND s.op IN ('c', 'u', 'r') THEN UPDATE SET
                t.name            = s.after_name,
                t.license_number  = s.after_license_number,
                t.city            = s.after_city,
                t.rating          = s.rating,
                t.active          = s.after_active,
                t.last_updated_ms = s.ts_ms
            WHEN NOT MATCHED AND s.op != 'd' THEN INSERT
                (id, name, license_number, city, rating, active, last_updated_ms)
                VALUES (s.after_id, s.after_name, s.after_license_number,
                        s.after_city, s.rating, s.after_active, s.ts_ms)
        """)
        sd = spark.sql("SELECT count(*) AS n FROM lakehouse.cdc.silver_drivers").collect()[0]["n"]
        log.info("silver_drivers: %d rows after MERGE", sd)
    finally:
        spark.stop()
