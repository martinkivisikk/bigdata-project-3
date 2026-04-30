from __future__ import annotations
import logging
from tasks.spark import create_spark_session
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

def gold_taxi_anomalies():
    spark = create_spark_session("gold_taxi_anomalies")
    try:
        silver = spark.table("lakehouse.taxi.silver_trips")

        # Define specific boolean flags for each rule
        flagged = silver.withColumn("is_zero_dist_high_fare", F.when((F.col("trip_distance") == 0) & (F.col("fare_amount") > 10), 1).otherwise(0)) \
                        .withColumn("is_impossible_speed", F.when((F.col("trip_distance") > 5) & 
                                   ((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) < 60), 1).otherwise(0)) \
                        .withColumn("is_tip_exceeds_fare", F.when(F.col("tip_amount") > F.col("fare_amount"), 1).otherwise(0)) \
                        .withColumn("is_negative_fare", F.when(F.col("fare_amount") < 0, 1).otherwise(0)) \
                        .withColumn("is_midnight_cheap", F.when((F.to_date("tpep_pickup_datetime") != F.to_date("tpep_dropoff_datetime")) & 
                                   (F.col("fare_amount") < 5), 1).otherwise(0))

        # Calculate rule_count and severity for the main anomaly table
        anomalies_df = flagged.withColumn("rule_count", 
            F.col("is_zero_dist_high_fare") + F.col("is_impossible_speed") + 
            F.col("is_tip_exceeds_fare") + F.col("is_negative_fare") + F.col("is_midnight_cheap")
        ).filter(F.col("rule_count") > 0) \
         .withColumn("severity", F.when(F.col("rule_count") >= 3, "high")
                                .when(F.col("rule_count") == 2, "medium")
                                .otherwise("low"))

        # Write the main anomaly table
        anomalies_df.writeTo("lakehouse.taxi.gold_trip_anomalies").createOrReplace()

        # Build Daily Summary with the EXACT column names used in your notebook query
        summary = flagged.groupBy(F.to_date("tpep_pickup_datetime").alias("date")) \
            .agg(
                F.count("*").alias("total_trips"),
                F.sum(F.when(
                    (F.col("is_zero_dist_high_fare") + F.col("is_impossible_speed") + 
                     F.col("is_tip_exceeds_fare") + F.col("is_negative_fare") + F.col("is_midnight_cheap")) > 0, 1
                ).otherwise(0)).alias("total_anomalies"),
                F.sum("is_zero_dist_high_fare").alias("n_zero_dist_high_fare"),
                F.sum("is_impossible_speed").alias("n_impossible_speed"),
                F.sum("is_tip_exceeds_fare").alias("n_tip_exceeds_fare"),
                F.sum("is_negative_fare").alias("n_negative_fare"),
                F.sum("is_midnight_cheap").alias("n_midnight_cheap")
            )
        
        summary.writeTo("lakehouse.taxi.gold_anomaly_summary").createOrReplace()
        log.info("Gold anomaly tables created successfully.")
    finally:
        spark.stop()