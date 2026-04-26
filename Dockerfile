FROM quay.io/jupyter/pyspark-notebook:2025-12-31

# Download connector JARs into the image so every container starts cold
# without hitting Maven Central.  The JARs land in ~/.ivy2/cache/ which
# is the same path spark.jars.ivy points to at runtime.
COPY prefetch_jars.py /tmp/prefetch_jars.py
RUN SPARK_HOME=/usr/local/spark \
    PYTHONPATH="$(ls /usr/local/spark/python/lib/py4j-*.zip | head -1):/usr/local/spark/python" \
    /opt/conda/bin/python3 /tmp/prefetch_jars.py && rm /tmp/prefetch_jars.py
