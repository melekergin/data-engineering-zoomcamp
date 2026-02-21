/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: Trip pickup timestamp.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: total_amount
    type: double
    description: Total charged amount.

custom_checks:
  - name: row_count_greater_than_zero
    description: Staging table should contain rows for the selected interval.
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
    value: 1

@bruin */

WITH filtered AS (
  SELECT
    *,
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime_ts,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime_ts
  FROM ingestion.trips
  WHERE CAST(pickup_datetime AS TIMESTAMP) >= '{{ start_datetime }}'
    AND CAST(pickup_datetime AS TIMESTAMP) < '{{ end_datetime }}'
),
ranked AS (
  SELECT
    vendorid,
    pickup_datetime_ts AS pickup_datetime,
    dropoff_datetime_ts AS dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    total_amount,
    taxi_type,
    source_file,
    CAST(extracted_at AS TIMESTAMP) AS extracted_at,
    ROW_NUMBER() OVER (
      PARTITION BY
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        fare_amount,
        total_amount,
        taxi_type
      ORDER BY extracted_at DESC
    ) AS rn
  FROM filtered
)
SELECT
  r.vendorid,
  r.pickup_datetime,
  r.dropoff_datetime,
  r.passenger_count,
  r.trip_distance,
  r.ratecodeid,
  r.store_and_fwd_flag,
  r.pulocationid,
  r.dolocationid,
  r.payment_type,
  p.payment_type_name,
  r.fare_amount,
  r.total_amount,
  r.taxi_type,
  r.source_file,
  r.extracted_at
FROM ranked r
LEFT JOIN ingestion.payment_lookup p
  ON r.payment_type = p.payment_type_id
WHERE r.rn = 1
