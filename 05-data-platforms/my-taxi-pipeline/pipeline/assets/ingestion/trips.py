"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default
materialization:
  type: table
  strategy: append
columns:
  - name: vendorid
    type: integer
    description: Vendor identifier.
  - name: pickup_datetime
    type: string
    description: Trip pickup timestamp.
  - name: dropoff_datetime
    type: string
    description: Trip dropoff timestamp.
  - name: passenger_count
    type: integer
    description: Number of passengers.
  - name: trip_distance
    type: double
    description: Trip distance in miles.
  - name: ratecodeid
    type: integer
    description: Rate code identifier.
  - name: store_and_fwd_flag
    type: string
    description: Store-and-forward flag.
  - name: pulocationid
    type: integer
    description: Pickup location ID.
  - name: dolocationid
    type: integer
    description: Dropoff location ID.
  - name: payment_type
    type: integer
    description: Payment type identifier.
  - name: fare_amount
    type: double
    description: Fare amount.
  - name: total_amount
    type: double
    description: Total charged amount.
  - name: taxi_type
    type: string
    description: Taxi type (yellow/green).
  - name: source_file
    type: string
    description: Source parquet file name.
  - name: extracted_at
    type: string
    description: Ingestion timestamp in UTC.

@bruin"""

import json
import os
from datetime import datetime
from io import BytesIO

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _month_starts(start_date: str, end_date: str) -> list[datetime]:
    start = datetime.fromisoformat(start_date).replace(day=1)
    end = datetime.fromisoformat(end_date).replace(day=1)

    months = []
    current = start
    while current <= end:
        months.append(current)
        current = current + relativedelta(months=1)
    return months


def _load_vars() -> dict:
    raw = os.getenv("BRUIN_VARS", "{}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def materialize():
    start_date = os.getenv("BRUIN_START_DATE")
    end_date = os.getenv("BRUIN_END_DATE")
    if not start_date or not end_date:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE are required")

    vars_payload = _load_vars()
    taxi_types = vars_payload.get("taxi_types", ["yellow"])
    extracted_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    frames = []
    for month in _month_starts(start_date, end_date):
        ym = f"{month.year}-{month.month:02d}"
        for taxi_type in taxi_types:
            file_name = f"{taxi_type}_tripdata_{ym}.parquet"
            url = f"{BASE_URL}/{file_name}"

            response = requests.get(url, timeout=120)
            if response.status_code == 404:
                continue
            response.raise_for_status()

            df = pd.read_parquet(BytesIO(response.content))
            df.columns = [c.lower() for c in df.columns]

            if "tpep_pickup_datetime" in df.columns:
                df = df.rename(
                    columns={
                        "tpep_pickup_datetime": "pickup_datetime",
                        "tpep_dropoff_datetime": "dropoff_datetime",
                    }
                )
            if "lpep_pickup_datetime" in df.columns:
                df = df.rename(
                    columns={
                        "lpep_pickup_datetime": "pickup_datetime",
                        "lpep_dropoff_datetime": "dropoff_datetime",
                    }
                )

            if "pickup_datetime" in df.columns:
                df["pickup_datetime"] = pd.to_datetime(
                    df["pickup_datetime"], errors="coerce"
                ).dt.strftime("%Y-%m-%d %H:%M:%S")
            if "dropoff_datetime" in df.columns:
                df["dropoff_datetime"] = pd.to_datetime(
                    df["dropoff_datetime"], errors="coerce"
                ).dt.strftime("%Y-%m-%d %H:%M:%S")

            df["taxi_type"] = taxi_type
            df["source_file"] = file_name
            df["extracted_at"] = extracted_at
            frames.append(df)

    if not frames:
        return pd.DataFrame(
            columns=[
                "vendorid",
                "pickup_datetime",
                "dropoff_datetime",
                "passenger_count",
                "trip_distance",
                "ratecodeid",
                "store_and_fwd_flag",
                "pulocationid",
                "dolocationid",
                "payment_type",
                "fare_amount",
                "total_amount",
                "taxi_type",
                "source_file",
                "extracted_at",
            ]
        )

    return pd.concat(frames, ignore_index=True)
