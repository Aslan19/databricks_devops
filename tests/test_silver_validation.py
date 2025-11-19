from pyspark.sql import Row
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from helpers.project_functions import validate_and_cast_silver

def make_row(**overrides):
    base = dict(
        vendor_id="1",
        pickup_datetime="2023-01-01 10:00:00",
        dropoff_datetime="2023-01-01 10:10:00",
        passenger_count=1,
        trip_distance=1.0,
        pickup_longitude=-73.0,
        pickup_latitude=40.0,
        rate_code_id=1,
        store_and_fwd_flag="N",
        dropoff_longitude=-73.1,
        dropoff_latitude=40.1,
        payment_type="CARD",
        fare_amount=10.0,
        extra=0.0,
        mta_tax=0.5,
        tip_amount=2.0,
        tolls_amount=0.0,
        total_amount=12.5
    )
    base.update(overrides)
    return Row(**base)


def test_validation_filters_negative_fare(spark):
    df = spark.createDataFrame([ make_row(fare_amount=-10) ])
    assert validate_and_cast_silver(df).count() == 0


def test_validation_filters_zero_distance(spark):
    df = spark.createDataFrame([ make_row(trip_distance=0) ])
    assert validate_and_cast_silver(df).count() == 0

def test_validation_cast_types(spark):
    df = spark.createDataFrame([ make_row() ])
    row = validate_and_cast_silver(df).collect()[0]

    assert isinstance(row.trip_distance, float)
    assert isinstance(row.passenger_count, int)
    assert isinstance(row.fare_amount, float)
