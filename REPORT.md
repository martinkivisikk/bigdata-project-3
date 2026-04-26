# Project 3 — CDC & Orchestrated Lakehouse Pipeline

**Group E** | PostgreSQL CDC → Debezium → Kafka → Iceberg + Taxi Pipeline, orchestrated with Apache Airflow.

---

## 1. CDC Correctness

### Silver Mirrors PostgreSQL

After a full DAG run the validation task compares Iceberg row counts against live PostgreSQL:

| Table | Silver (Iceberg) | PostgreSQL | Drift |
|---|---|---|---|
| `silver_customers` | 172 | 174 | 2 |
| `silver_drivers` | 51 | 51 | 0 |

A drift of ≤ 2 on customers is expected: `simulate.py` runs at 1 op/s and a few inserts can land in PostgreSQL after the Kafka batch window closes but before the validation query executes. The validate task tolerates up to 5 rows of drift and raises an exception otherwise.

**Spot checks** — three rows compared directly between `silver_customers` and `SELECT * FROM customers WHERE id IN (6, 7, 30)`:

| id | Silver name | Silver email | Silver country | PG matches? |
|---|---|---|---|---|
| 6 | Frank Muller | updated_6_485@example.com | Lithuania | ✓ |
| 7 | Grace Kim | updated_7_741@inbox.org | Australia | ✓ |
| 30 | Lena Muller | updated_30_431@test.net | Lithuania | ✓ |

### DELETE Propagation

When PostgreSQL deletes a row (e.g., `DELETE FROM customers WHERE id = 12`), Debezium emits:
1. A CDC event with `op = 'd'`, `before.id = 12`, `after = NULL`
2. A tombstone message (null Kafka value) for log compaction

Both are stored in bronze. The silver MERGE matches on `t.id = s.entity_id` and issues `WHEN MATCHED AND op = 'd' THEN DELETE`. After the run, id 12 is absent from `silver_customers`. Confirmed from notebook output: ids 12, 13, 16, 17, 18, 22, 23 all show `op='d'` in bronze and are absent from silver.

### Idempotency

Running the DAG twice with no new changes produces the same silver state:

1. `bronze_cdc` reads Kafka starting from `max(kafka_offset) + 1` per partition (via `starting_offsets()`). With no new Kafka messages, 0 rows are appended.
2. `silver_cdc` deduplicates the full bronze table with `ROW_NUMBER OVER (PARTITION BY entity_id ORDER BY ts_ms DESC, kafka_offset DESC)` — the same latest row per key is selected on every run.
3. The MERGE applies the same operations against an already-current silver table:
   - `MATCHED AND op IN ('c','u','r') → UPDATE` — writes the same values already stored (no change).
   - `MATCHED AND op = 'd' → DELETE` — row is already absent; the MERGE source row is NOT MATCHED and excluded by `WHEN NOT MATCHED AND op != 'd'`.
   - `NOT MATCHED AND op != 'd' → INSERT` — row is already present, so the MATCHED branch fires instead.

Re-run result: `silver_customers = 172`, `silver_drivers = 51` — identical to the first run.

---

## 2. Lakehouse Design

### Table Schemas

**`cdc.bronze_customers`** (append-only, no deletes):
```
kafka_offset LONG, kafka_partition INT, kafka_timestamp TIMESTAMP,
op STRING, event_type STRING,
before_id INT, before_name STRING, before_email STRING, before_country STRING,
after_id INT,  after_name STRING,  after_email STRING,  after_country STRING,
source_lsn LONG, ts_ms LONG
```
Every Debezium event stored verbatim. `event_type = 'tombstone'` for null-value messages; `op` is NULL for tombstones and non-null for real CDC events.

**`cdc.bronze_drivers`** — same envelope plus:
```
before_license_number STRING, before_rating STRING, before_city STRING, before_active BOOLEAN,
after_license_number  STRING, after_rating  STRING, after_city  STRING, after_active  BOOLEAN
```
`rating` kept as raw base64 string (Debezium-encoded `DECIMAL`). Decoded in silver.

**`cdc.silver_customers`** (MERGE target, one row per PK):
```
id INT, name STRING, email STRING, country STRING, last_updated_ms BIGINT
```

**`cdc.silver_drivers`** (MERGE target):
```
id INT, name STRING, license_number STRING, city STRING,
rating DECIMAL(3,2), last_updated_ms BIGINT
```
Rating decoded: `conv(hex(unbase64(after_rating)), 16, 10) / 100.0`.

**`taxi.bronze_trips`** (append-only):
```
key STRING, value STRING, topic STRING, partition INT, offset BIGINT, timestamp TIMESTAMP
```
Raw Kafka messages, schema-on-read. Offset tracked per partition for incremental runs.

**`taxi.silver_trips`** (partitioned by `days(tpep_pickup_datetime)`):
```
VendorID INT, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP,
passenger_count INT, trip_distance DOUBLE, PULocationID INT, DOLocationID INT,
fare_amount DOUBLE, total_amount DOUBLE,
pickup_borough STRING, pickup_zone STRING, pickup_service_zone STRING,
dropoff_borough STRING, dropoff_zone STRING, dropoff_service_zone STRING
```
Invalid trips removed (null timestamps, distance ≤ 0, passenger count outside 1–9, negative fare). Zone names joined from parquet lookup.

**`taxi.gold_route_stats`** (partitioned by `pickup_borough`):
```
pickup_borough STRING, dropoff_borough STRING,
trip_count BIGINT, total_revenue DOUBLE, avg_distance DOUBLE, revenue_per_mile DOUBLE
```

### Why Each Layer Differs

- **Bronze** preserves every event including before/after state and Kafka metadata. Never modified — enables full reprocessing from raw.
- **Silver** collapses history to current state: one row per entity, tombstones removed, types decoded, deduplication applied. Directly mirrors the source system.
- **Gold** aggregates silver into business-level metrics; individual rows are discarded in favour of summaries.

### Iceberg Snapshot History (silver_customers)

```sql
SELECT snapshot_id, committed_at, operation
FROM lakehouse.cdc.silver_customers.snapshots
ORDER BY committed_at;
```

Each MERGE creates an `overwrite` snapshot. After three DAG runs the history shows three distinct snapshots. Snapshot IDs are used for time-travel.

### Time-Travel and Rollback

View state before a specific MERGE:
```sql
SELECT * FROM lakehouse.cdc.silver_customers VERSION AS OF <snapshot_id>;
```

Roll back a bad MERGE without data loss:
```sql
CALL lakehouse.system.rollback_to_snapshot('cdc.silver_customers', <snapshot_id>);
```
This updates only the metadata pointer; the data files for the current snapshot remain in MinIO and can be re-applied.

---

## 3. Orchestration Design

### DAG Task Graph

```
ensure_connector → connector_health ─┬─ bronze_cdc  → silver_cdc  ─────────────┐
                                      └─ bronze_taxi → silver_taxi → gold_taxi ──┴─→ validate
```

| Task | Purpose |
|---|---|
| `ensure_connector` | Idempotent POST to Kafka Connect: creates Debezium connector if absent |
| `connector_health` | `HttpSensor` polling `GET /connectors/pg-cdc-connector/status` every 30 s (timeout 120 s). Downstream tasks are skipped if this fails |
| `bronze_cdc` | Spark batch: read new Kafka CDC events since last offset, append to bronze |
| `bronze_taxi` | Spark batch: read new taxi JSON events since last offset, append to bronze |
| `silver_cdc` | Dedup + MERGE bronze into silver for customers and drivers |
| `silver_taxi` | Re-process full bronze into cleaned, zone-enriched silver (overwritePartitions) |
| `gold_taxi` | Aggregate silver into borough-level route stats (overwritePartitions) |
| `validate` | Compare Iceberg silver row counts vs PostgreSQL; fail if drift > 5 |

### Scheduling Strategy

`schedule_interval = timedelta(minutes=15)` supports a **15-minute data freshness SLA**. Debezium captures changes within seconds; the 15-minute window covers batch processing, MERGE, and gold aggregation overhead. Sufficient for analytics dashboards that do not require near-real-time updates. `max_active_runs=1` prevents concurrent MERGE races on the silver tables.

### Retry and Failure Handling

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "sla": timedelta(minutes=30),
}
```

Retry schedule for a failing task: 1 min → 2 min → 4 min (capped at 10 min). If `connector_health` exhausts its retries (e.g., Kafka Connect is down), all downstream tasks are in `upstream_failed` state and do not run. Once Connect recovers and the DAG is re-triggered, `connector_health` succeeds and the full run proceeds normally.

**Example failure scenario**: `connect` container restarted mid-run. `connector_health` fails on first poke, retries at T+1 min and T+3 min. After Connect restarts, the sensor detects `RUNNING` and all downstream tasks execute. The `ensure_connector` task re-registers the connector if it was lost during the restart.

### DAG Run History

Three consecutive successful runs are visible in the Airflow UI (see screenshots). Each run is identified by its `execution_date` at 15-minute intervals. Re-runs for the same interval produce the same silver state (idempotent).

`catchup=False` — missed intervals are not backfilled automatically. Because bronze is offset-based, the next scheduled run reads all messages that accumulated during the missed window, so data is never lost.

---

## 4. Taxi Pipeline

### Bronze → Silver → Gold

**Bronze** (`bronze_trips`): raw JSON events appended from Kafka. Incremental — each run reads only messages with `offset > max(offset)` seen in the previous run.

**Silver** (`silver_trips`): invalid trips are removed (null timestamps, `trip_distance ≤ 0`, passenger count outside 1–9, negative fare, `dropoff ≤ pickup`). Zone names are joined via broadcast of the `taxi_zone_lookup.parquet` file. Deduplication by `(VendorID, pickup_datetime, dropoff_datetime, PULocationID, DOLocationID)`. Partitioned by `days(tpep_pickup_datetime)` for efficient time scans.

**Gold** (`gold_route_stats`): grouped by `(pickup_borough, dropoff_borough)` — `trip_count`, `total_revenue`, `avg_distance`, `revenue_per_mile = total_revenue / total_distance`. Partitioned by `pickup_borough` for borough-filtered queries.

### Improvements over Project 2

todo
---

## 5. Custom Scenario

todo