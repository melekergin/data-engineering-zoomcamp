/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: taxi_type
    type: string
    description: Taxi type.
    primary_key: true
  - name: payment_type_name
    type: string
    description: Payment category.
    primary_key: true
  - name: pickup_date
    type: DATE
    description: Pickup date.
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Number of trips.
    checks:
      - name: non_negative
  - name: total_amount
    type: DOUBLE
    description: Total amount summed across trips.

@bruin */

SELECT
  taxi_type,
  COALESCE(payment_type_name, 'unknown') AS payment_type_name,
  DATE_TRUNC('day', pickup_datetime)::DATE AS pickup_date,
  MIN(pickup_datetime) AS pickup_datetime,
  COUNT(*) AS trip_count,
  SUM(total_amount) AS total_amount
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1,2,3
