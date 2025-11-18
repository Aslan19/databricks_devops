from pyspark.sql import Row
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from helpers.project_functions import (
    clean_bronze,
    validate_and_cast_silver,
    enrich_silver
)

def test_full_pipeline_end_to_end(spark):
    df = spark.createDataFrame([
        Row(
            vendor_id="1",
            pickup_datetime="2023-01-01 10:00:00",
            dropoff_datetime="2023-01-01 10:10:00",
            passenger_count=1,
            trip_distance=3.0,
            pickup_longitude=-73.98,
            pickup_latitude=40.75,
            rate_code_id=1,
            store_and_fwd_flag="N",
            dropoff_longitude=-73.99,
            dropoff_latitude=40.76,
            payment_type="CARD",
            fare_amount=10.0,
            extra=0.0,
            mta_tax=0.5,
            tip_amount=2.0,
            tolls_amount=0.0,
            total_amount=12.5
        )
    ])

    bronze = clean_bronze(df)
    silver = validate_and_cast_silver(bronze)
    enriched = enrich_silver(silver)

    row = enriched.collect()[0]

    assert row.trip_duration_minutes > 0
    assert row.average_speed_mph > 0
    assert row.vendor_id_norm == "1"
    assert row.pickup_date is not None
