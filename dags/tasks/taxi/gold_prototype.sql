-- gold_prototype.sql
-- Scenario: Fraud anomaly detection for taxi trips
-- Silver tables: lakehouse.taxi.silver_trips + lakehouse.cdc.silver_customers
-- Join key: MOD(t.trip_id, customer_count) + 1 = c.id
-- Gold adds: per-trip rule flags, severity scoring, daily summary

WITH customer_count AS (
    SELECT count(*) AS cnt
    FROM lakehouse.cdc.silver_customers
),

base AS (
    SELECT
        t.*,
        c.name          AS customer_name,
        c.country       AS customer_country,

        -- Derive duration in minutes
        (unix_timestamp(t.tpep_dropoff_datetime)
         - unix_timestamp(t.tpep_pickup_datetime)) / 60.0  AS duration_minutes,

        -- Midnight cross flag
        date(t.tpep_pickup_datetime) != date(t.tpep_dropoff_datetime)
                                                            AS crosses_midnight,

        -- Individual rule flags
        (t.trip_distance = 0 AND t.fare_amount > 10)
            AS rule_zero_dist_high_fare,

        ((unix_timestamp(t.tpep_dropoff_datetime)
          - unix_timestamp(t.tpep_pickup_datetime)) / 60.0 < 1
         AND t.trip_distance > 5)
            AS rule_impossible_speed,

        (t.tip_amount > t.fare_amount AND t.fare_amount > 0)
            AS rule_tip_exceeds_fare,

        (t.fare_amount < 0)
            AS rule_negative_fare,

        (date(t.tpep_pickup_datetime) != date(t.tpep_dropoff_datetime)
         AND t.fare_amount < 5)
            AS rule_midnight_cheap

    FROM lakehouse.taxi.silver_trips t
    CROSS JOIN customer_count cc
    JOIN lakehouse.cdc.silver_customers c
        ON MOD(t.trip_id, cc.cnt) + 1 = c.id
),

flagged AS (
    SELECT
        *,
        -- Count how many rules fired
        (CAST(rule_zero_dist_high_fare AS INT)
       + CAST(rule_impossible_speed   AS INT)
       + CAST(rule_tip_exceeds_fare   AS INT)
       + CAST(rule_negative_fare      AS INT)
       + CAST(rule_midnight_cheap     AS INT))   AS rule_count

    FROM base
)

SELECT
    trip_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    PULocationID,
    DOLocationID,
    trip_distance,
    duration_minutes,
    fare_amount,
    tip_amount,
    total_amount,
    customer_name,
    customer_country,

    -- Which rules fired as readable string
    concat_ws(', ',
        CASE WHEN rule_zero_dist_high_fare THEN 'zero_dist_high_fare' END,
        CASE WHEN rule_impossible_speed    THEN 'impossible_speed'    END,
        CASE WHEN rule_tip_exceeds_fare    THEN 'tip_exceeds_fare'    END,
        CASE WHEN rule_negative_fare       THEN 'negative_fare'       END,
        CASE WHEN rule_midnight_cheap      THEN 'midnight_cheap'      END
    )  AS which_rules,

    rule_count,

    CASE
        WHEN rule_count >= 3 THEN 'high'
        WHEN rule_count =  2 THEN 'medium'
        WHEN rule_count =  1 THEN 'low'
    END AS severity

FROM flagged
WHERE rule_count > 0
ORDER BY rule_count DESC, fare_amount DESC
LIMIT 20