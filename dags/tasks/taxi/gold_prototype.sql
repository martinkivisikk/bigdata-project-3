-- gold_prototype.sql
-- Scenario: Fraud anomaly detection for taxi trips
-- Silver tables: lakehouse.taxi.silver_trips + lakehouse.cdc.silver_customers
-- Join key: MOD(ROW_NUMBER() OVER (ORDER BY tpep_pickup_datetime), customer_count) + 1 = customer.id
-- Gold adds: per-trip rule flags, severity scoring, daily summary

WITH customer_count AS (
    SELECT count(*) AS cnt FROM lakehouse.cdc.silver_customers
),
trips_with_id AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY tpep_pickup_datetime) AS trip_id
    FROM lakehouse.taxi.silver_trips
),
base AS (
    SELECT
        t.tpep_pickup_datetime,
        t.tpep_dropoff_datetime,
        t.trip_distance,
        t.fare_amount,
        t.tip_amount,
        t.total_amount,
        t.pickup_zone,
        t.dropoff_zone,
        c.name    AS customer_name,
        c.country AS customer_country,
        (unix_timestamp(t.tpep_dropoff_datetime)
         - unix_timestamp(t.tpep_pickup_datetime)) / 60.0         AS duration_min,
        (t.trip_distance = 0 AND t.fare_amount > 10)              AS r_zero_dist,
        ((unix_timestamp(t.tpep_dropoff_datetime)
          - unix_timestamp(t.tpep_pickup_datetime)) / 60.0 < 1
          AND t.trip_distance > 5)                                 AS r_impossible,
        (t.tip_amount > t.fare_amount AND t.fare_amount > 0)       AS r_tip,
        (t.fare_amount < 0)                                        AS r_neg_fare,
        (date(t.tpep_pickup_datetime) != date(t.tpep_dropoff_datetime)
         AND t.fare_amount < 5)                                    AS r_midnight
    FROM trips_with_id t
    CROSS JOIN customer_count cc
    JOIN lakehouse.cdc.silver_customers c
        ON MOD(t.trip_id, cc.cnt) + 1 = c.id
),
flagged AS (
    SELECT *,
        CAST(r_zero_dist AS INT) + CAST(r_impossible AS INT) +
        CAST(r_tip AS INT) + CAST(r_neg_fare AS INT) +
        CAST(r_midnight AS INT) AS rule_count
    FROM base
)
SELECT
    tpep_pickup_datetime,
    ROUND(trip_distance, 2)  AS distance,
    ROUND(fare_amount, 2)    AS fare,
    ROUND(tip_amount, 2)     AS tip,
    ROUND(duration_min, 1)   AS dur_min,
    pickup_zone,
    customer_name,
    customer_country,
    concat_ws(' + ',
        CASE WHEN r_zero_dist  THEN 'zero_dist_high_fare' END,
        CASE WHEN r_impossible THEN 'impossible_speed'    END,
        CASE WHEN r_tip        THEN 'tip_exceeds_fare'    END,
        CASE WHEN r_neg_fare   THEN 'negative_fare'       END,
        CASE WHEN r_midnight   THEN 'midnight_cheap'      END
    ) AS which_rules,
    CASE WHEN rule_count >= 3 THEN 'high'
         WHEN rule_count  = 2 THEN 'medium'
         ELSE 'low' END AS severity
FROM flagged
WHERE rule_count > 0
ORDER BY rule_count DESC, fare_amount DESC
LIMIT 20
