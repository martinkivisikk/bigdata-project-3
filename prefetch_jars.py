"""
Build-time script — run once during `docker build` to download all Spark
connector JARs into the image layer. Not used at runtime.
"""
from pyspark.sql import SparkSession

PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
    "org.apache.iceberg:iceberg-aws-bundle:1.10.1",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
])
spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.jars.packages", PACKAGES)
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.stop()
print("Connector JARs pre-fetched successfully.")
