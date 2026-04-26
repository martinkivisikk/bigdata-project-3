from __future__ import annotations

import logging

from tasks.config import ZONE_LOOKUP_PATH
from tasks.spark import create_spark_session

log = logging.getLogger(__name__)


def silver_taxi(**ctx):
    spark = create_spark_session("silver_taxi")
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, IntegerType, DoubleType, TimestampType,
    )
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.taxi")

        spark.sql("""
            CREATE TABLE IF NOT EXISTS lakehouse.taxi.silver_trips (
                VendorID              INT,
                tpep_pickup_datetime  TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count       INT,
                trip_distance         DOUBLE,
                PULocationID          INT,
                DOLocationID          INT,
                fare_amount           DOUBLE,
                total_amount          DOUBLE,
                pickup_borough        STRING,
                pickup_zone           STRING,
                pickup_service_zone   STRING,
                dropoff_borough       STRING,
                dropoff_zone          STRING,
                dropoff_service_zone  STRING
            ) USING iceberg
            PARTITIONED BY (days(tpep_pickup_datetime))
        """)

        schema = StructType([
            StructField("VendorID",              IntegerType()),
            StructField("tpep_pickup_datetime",  TimestampType()),
            StructField("tpep_dropoff_datetime", TimestampType()),
            StructField("passenger_count",       IntegerType()),
            StructField("trip_distance",         DoubleType()),
            StructField("PULocationID",          IntegerType()),
            StructField("DOLocationID",          IntegerType()),
            StructField("fare_amount",           DoubleType()),
            StructField("total_amount",          DoubleType()),
        ])

        zones = spark.read.parquet(ZONE_LOOKUP_PATH)
        pu = zones.alias("pu")
        do = zones.alias("do")

        parsed = (
            spark.table("lakehouse.taxi.bronze_trips")
            .withColumn("json", F.from_json(F.col("value").cast("string"), schema))
            .select("json.*")
        )

        transformed = (
            parsed
            .select(
                F.col("VendorID").cast("int"),
                F.col("tpep_pickup_datetime"),
                F.col("tpep_dropoff_datetime"),
                F.when(
                    (F.col("passenger_count").isNull()) | (F.col("passenger_count") <= 0), 1
                ).otherwise(F.col("passenger_count")).cast("int").alias("passenger_count"),
                F.col("trip_distance").cast("double"),
                F.col("PULocationID").cast("int"),
                F.col("DOLocationID").cast("int"),
                F.col("fare_amount").cast("double"),
                F.col("total_amount").cast("double"),
            )
            .dropna(subset=[
                "tpep_pickup_datetime", "tpep_dropoff_datetime",
                "PULocationID", "DOLocationID",
            ])
            .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
            .filter(F.col("trip_distance") > 0)
            .filter((F.col("passenger_count") > 0) & (F.col("passenger_count") < 10))
            .filter(F.col("fare_amount") >= 0)
        )

        dedup_keys = [
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "PULocationID", "DOLocationID",
        ]
        enriched = (
            transformed
            .join(F.broadcast(pu), transformed.PULocationID == F.col("pu.LocationID"), "left")
            .join(F.broadcast(do), transformed.DOLocationID == F.col("do.LocationID"), "left")
            .select(
                transformed["*"],
                F.col("pu.Borough").alias("pickup_borough"),
                F.col("pu.Zone").alias("pickup_zone"),
                F.col("pu.service_zone").alias("pickup_service_zone"),
                F.col("do.Borough").alias("dropoff_borough"),
                F.col("do.Zone").alias("dropoff_zone"),
                F.col("do.service_zone").alias("dropoff_service_zone"),
            )
            .filter(F.col("pickup_zone").isNotNull() & F.col("dropoff_zone").isNotNull())
            .dropDuplicates(dedup_keys)
        )

        enriched.writeTo("lakehouse.taxi.silver_trips").overwritePartitions()

        n = spark.sql("SELECT count(*) AS n FROM lakehouse.taxi.silver_trips").collect()[0]["n"]
        log.info("silver_trips: %d rows after overwrite", n)
    finally:
        spark.stop()
