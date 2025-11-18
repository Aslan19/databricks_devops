from pyspark.sql import Row
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from helpers.project_functions import validate_and_cast_silver

def test_validation_filters_negative_fare(spark):
    df = spark.createDataFrame([
        Row(fare_amount=-10, trip_distance=1,
            pickup_datetime="2023-01-01 10:00:00",
            dropoff_datetime="2023-01-01 10:10:00")
    ])
    assert validate_and_cast_silver(df).count() == 0

def test_validation_filters_zero_distance(spark):
    df = spark.createDataFrame([
        Row(fare_amount=10, trip_distance=0,
            pickup_datetime="2023-01-01 10:00:00",
            dropoff_datetime="2023-01-01 10:10:00")
    ])
    assert validate_and_cast_silver(df).count() == 0

def test_validation_cast_types(spark):
    df = spark.createDataFrame([
        Row(
            vendor_id="1",
            pickup_datetime="2023-01-01 10:00:00",
            dropoff_datetime="2023-01-01 10:10:00",
            passenger_count="2",
            trip_distance="3.5",
            pickup_longitude="-73.98",
            pickup_latitude="40.75",
            rate_code_id="1",
            store_and_fwd_flag="N",
            dropoff_longitude="-73.99",
            dropoff_latitude="40.76",
            payment_type="CARD",
            fare_amount="15.5",
            extra="0",
            mta_tax="0.5",
            tip_amount="2",
            tolls_amount="0",
            total_amount="18"
        )
    ])

    row = validate_and_cast_silver(df).collect()[0]

    assert isinstance(row.trip_distance, float)
    assert isinstance(row.passenger_count, int)
    assert isinstance(row.fare_amount, float)
