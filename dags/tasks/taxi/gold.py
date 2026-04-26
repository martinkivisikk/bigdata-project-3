from __future__ import annotations

import logging

from tasks.spark import create_spark_session

log = logging.getLogger(__name__)


def gold_taxi(**ctx):
    spark = create_spark_session("gold_taxi")
    from pyspark.sql import functions as F
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.taxi.gold_route_stats (
                pickup_borough    STRING,
                dropoff_borough   STRING,
                trip_count        BIGINT,
                total_revenue     DOUBLE,
                avg_distance      DOUBLE,
                revenue_per_mile  DOUBLE
            ) USING iceberg
            PARTITIONED BY (pickup_borough)
        """)

        silver = spark.table("lakehouse.taxi.silver_trips")
        agg = (
            silver
            .groupBy("pickup_borough", "dropoff_borough")
            .agg(
                F.count("*").alias("trip_count"),
                F.round(F.sum("total_amount"), 2).alias("total_revenue"),
                F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
                F.round(
                    F.sum("total_amount") / F.when(
                        F.sum("trip_distance") > 0, F.sum("trip_distance")
                    ).otherwise(1),
                    2,
                ).alias("revenue_per_mile"),
            )
        )

        agg.writeTo("lakehouse.taxi.gold_route_stats").overwritePartitions()

        n = spark.sql("SELECT count(*) AS n FROM lakehouse.taxi.gold_route_stats").collect()[0]["n"]
        log.info("gold_route_stats: %d rows after overwrite", n)
    finally:
        spark.stop()
