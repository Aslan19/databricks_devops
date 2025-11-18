from pyspark.sql import Row
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from helpers.project_functions import enrich_silver

def test_enrichment_adds_trip_duration(spark):
    df = spark.createDataFrame([
        Row(
            pickup_datetime="2023-01-01 10:00:00",
            dropoff_datetime="2023-01-01 10:30:00",
            trip_distance=10.0,
            vendor_id="1"
        )
    ])

    row = enrich_silver(df).collect()[0]

    assert round(row.trip_duration_minutes) == 30

def test_enrichment_adds_avg_speed(spark):
    df = spark.createDataFrame([
        Row(
            pickup_datetime="2023-01-01 10:00:00",
            dropoff_datetime="2023-01-01 11:00:00",
            trip_distance=60.0,
            vendor_id="1"
        )
    ])

    row = enrich_silver(df).collect()[0]

    assert round(row.average_speed_mph) == 60

def test_vendor_normalization(spark):
    df = spark.createDataFrame([
        Row(
            vendor_id="7",
            pickup_datetime="2023-01-01",
            dropoff_datetime="2023-01-01",
            trip_distance=1.0
        )
    ])

    row = enrich_silver(df).collect()[0]
    assert row.vendor_id_norm == "unknown"

def test_pickup_date_added(spark):
    df = spark.createDataFrame([
        Row(
            vendor_id="2",
            pickup_datetime="2023-05-15 10:00:00",
            dropoff_datetime="2023-05-15 10:05:00",
            trip_distance=1.0
        )
    ])

    row = enrich_silver(df).collect()[0]
    assert str(row.pickup_date) == "2023-05-15"
