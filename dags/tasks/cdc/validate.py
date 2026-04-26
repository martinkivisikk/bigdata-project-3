from __future__ import annotations

import logging

from tasks.pg import pg_execute
from tasks.spark import create_spark_session

log = logging.getLogger(__name__)


def validate(**ctx):
    spark = create_spark_session("validate")
    try:
        spark.sql("USE lakehouse.cdc")

        results = []
        for table, pg_table in [("silver_customers", "customers"),
                                ("silver_drivers", "drivers")]:
            iceberg_count = spark.sql(
                f"SELECT count(*) AS n FROM lakehouse.cdc.{table}"
            ).collect()[0]["n"]
            pg_count = pg_execute(
                f"SELECT COUNT(*) FROM {pg_table}", fetch=True
            )[0][0]
            drift = abs(iceberg_count - pg_count)
            results.append((table, iceberg_count, pg_count, drift))
            log.info(
                "%s: iceberg=%d  postgres=%d  drift=%d",
                table, iceberg_count, pg_count, drift,
            )

        max_drift = max(r[3] for r in results)
        if max_drift > 5:
            raise ValueError(
                f"Row drift too large ({max_drift} > 5). "
                f"Details: {results}"
            )
        log.info("Validation passed — max drift = %d (<= 5)", max_drift)
    finally:
        spark.stop()
