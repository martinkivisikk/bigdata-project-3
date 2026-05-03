from __future__ import annotations

import logging

from tasks.spark import create_spark_session

log = logging.getLogger(__name__)


def gold_taxi_anomalies():
    spark = create_spark_session("gold_taxi_anomalies")
    from pyspark.sql import functions as F
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.taxi.gold_trip_anomalies (
                VendorID              INT,
                tpep_pickup_datetime  TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                trip_distance         DOUBLE,
                fare_amount           DOUBLE,
                tip_amount            DOUBLE,
                total_amount          DOUBLE,
                pickup_borough        STRING,
                pickup_zone           STRING,
                dropoff_borough       STRING,
                dropoff_zone          STRING,
                duration_minutes      DOUBLE,
                which_rules           STRING,
                rule_count            INT,
                severity              STRING
            ) USING iceberg
            PARTITIONED BY (days(tpep_pickup_datetime))
        """)

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.taxi.gold_anomaly_summary (
                trip_date                DATE,
                total_trips              BIGINT,
                total_anomalies          BIGINT,
                anomaly_rate_pct         DOUBLE,
                n_zero_dist_high_fare    BIGINT,
                n_impossible_speed       BIGINT,
                n_tip_exceeds_fare       BIGINT,
                n_negative_fare          BIGINT,
                n_midnight_cheap         BIGINT
            ) USING iceberg
            PARTITIONED BY (trip_date)
        """)
        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.taxi.gold_anomalies_by_zone (
                pickup_borough          STRING,
                pickup_zone             STRING,
                total_trips             BIGINT,
                total_anomalies         BIGINT,
                anomaly_rate_pct        DOUBLE,
                n_zero_dist_high_fare   BIGINT,
                n_impossible_speed      BIGINT,
                n_tip_exceeds_fare      BIGINT,
                n_negative_fare         BIGINT,
                n_midnight_cheap        BIGINT
            ) USING iceberg
            PARTITIONED BY (pickup_borough)
        """)

        silver = spark.table("lakehouse.taxi.silver_trip_custom_scenario")

        flagged = (
            silver
            .withColumn("duration_minutes",
                (F.unix_timestamp("tpep_dropoff_datetime")
                 - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
            .withColumn("is_zero_dist_high_fare",
                ((F.col("trip_distance") == 0) & (F.col("fare_amount") > 10)).cast("int"))
            .withColumn("is_impossible_speed",
                ((F.col("duration_minutes") < 1) & (F.col("trip_distance") > 5)).cast("int"))
            .withColumn("is_tip_exceeds_fare",
                ((F.col("tip_amount") > F.col("fare_amount")) & (F.col("fare_amount") > 0)).cast("int"))
            .withColumn("is_negative_fare",
                (F.col("fare_amount") < 0).cast("int"))
            .withColumn("is_midnight_cheap",
                ((F.to_date("tpep_pickup_datetime") != F.to_date("tpep_dropoff_datetime"))
                 & (F.col("fare_amount") < 5)).cast("int"))
            .withColumn("rule_count",
                F.col("is_zero_dist_high_fare") + F.col("is_impossible_speed")
                + F.col("is_tip_exceeds_fare") + F.col("is_negative_fare")
                + F.col("is_midnight_cheap"))
        )

        anomalies = (
            flagged
            .filter(F.col("rule_count") > 0)
            .withColumn("which_rules",
                F.concat_ws(", ",
                    F.when(F.col("is_zero_dist_high_fare") == 1, F.lit("zero_dist_high_fare")),
                    F.when(F.col("is_impossible_speed")    == 1, F.lit("impossible_speed")),
                    F.when(F.col("is_tip_exceeds_fare")    == 1, F.lit("tip_exceeds_fare")),
                    F.when(F.col("is_negative_fare")       == 1, F.lit("negative_fare")),
                    F.when(F.col("is_midnight_cheap")      == 1, F.lit("midnight_cheap")),
                ))
            .withColumn("severity",
                F.when(F.col("rule_count") >= 3, "high")
                 .when(F.col("rule_count") == 2, "medium")
                 .otherwise("low"))
            .select(
                "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                "trip_distance", "fare_amount", "tip_amount", "total_amount",
                "pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone",
                "duration_minutes", "which_rules", "rule_count", "severity",
            )
        )

        anomalies.writeTo("lakehouse.taxi.gold_trip_anomalies").overwritePartitions()

        summary = (
            flagged
            .groupBy(F.to_date("tpep_pickup_datetime").alias("trip_date"))
            .agg(
                F.count("*").alias("total_trips"),
                F.sum(F.when(F.col("rule_count") > 0, 1).otherwise(0)).alias("total_anomalies"),
                F.sum("is_zero_dist_high_fare").alias("n_zero_dist_high_fare"),
                F.sum("is_impossible_speed").alias("n_impossible_speed"),
                F.sum("is_tip_exceeds_fare").alias("n_tip_exceeds_fare"),
                F.sum("is_negative_fare").alias("n_negative_fare"),
                F.sum("is_midnight_cheap").alias("n_midnight_cheap"),
            )
            .withColumn("anomaly_rate_pct",
                F.round(F.col("total_anomalies") / F.col("total_trips") * 100, 2))
            .select(
                "trip_date", "total_trips", "total_anomalies", "anomaly_rate_pct",
                "n_zero_dist_high_fare", "n_impossible_speed", "n_tip_exceeds_fare",
                "n_negative_fare", "n_midnight_cheap",
            )
        )

        summary.writeTo("lakehouse.taxi.gold_anomaly_summary").overwritePartitions()

        zone_summary = (
            flagged
            .groupBy("pickup_borough", "pickup_zone")
            .agg(
                F.count("*").alias("total_trips"),
                F.sum(F.when(F.col("rule_count") > 0, 1).otherwise(0)).alias("total_anomalies"),
                F.sum("is_zero_dist_high_fare").alias("n_zero_dist_high_fare"),
                F.sum("is_impossible_speed").alias("n_impossible_speed"),
                F.sum("is_tip_exceeds_fare").alias("n_tip_exceeds_fare"),
                F.sum("is_negative_fare").alias("n_negative_fare"),
                F.sum("is_midnight_cheap").alias("n_midnight_cheap"),
            )
            .withColumn(
                "anomaly_rate_pct",
                F.round(F.col("total_anomalies") / F.col("total_trips") * 100, 2)
            )
            .select(
                "pickup_borough",
                "pickup_zone",
                "total_trips",
                "total_anomalies",
                "anomaly_rate_pct",
                "n_zero_dist_high_fare",
                "n_impossible_speed",
                "n_tip_exceeds_fare",
                "n_negative_fare",
                "n_midnight_cheap",
            )
        )

        zone_summary.writeTo("lakehouse.taxi.gold_anomalies_by_zone").overwritePartitions()

        n_trips = spark.sql("SELECT count(*) AS n FROM lakehouse.taxi.gold_trip_anomalies").collect()[0]["n"]
        n_days  = spark.sql("SELECT count(*) AS n FROM lakehouse.taxi.gold_anomaly_summary").collect()[0]["n"]
        n_zones = spark.sql("SELECT count(*) AS n FROM lakehouse.taxi.gold_anomalies_by_zone").collect()[0]["n"]
        log.info("gold_trip_anomalies: %d flagged trips", n_trips)
        log.info("gold_anomaly_summary: %d days", n_days)
        log.info("gold_anomalies_by_zone: %d pickup zones", n_zones)

    finally:
        spark.stop()
