import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from helpers import project_functions
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, TimestampType, StringType


def test_schema_is_struct_type():
    schema = project_functions.get_nyc_schema()
    assert isinstance(schema, StructType)

def test_schema_field_count():
    schema = project_functions.get_nyc_schema()
    assert len(schema.fields) == 18  # expected number

def test_schema_contains_required_fields():
    schema = project_functions.get_nyc_schema()
    field_names = [f.name for f in schema.fields]

    expected = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "pickup_longitude",
        "pickup_latitude",
        "rate_code_id",
        "store_and_fwd_flag",
        "dropoff_longitude",
        "dropoff_latitude",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "total_amount"
    ]

    assert set(field_names) == set(expected)

def test_schema_field_types():
    schema = project_functions.get_nyc_schema()
    fields = {f.name: type(f.dataType) for f in schema.fields}

    assert fields["fare_amount"] == DoubleType
    assert fields["pickup_datetime"] == TimestampType
    assert fields["passenger_count"] == IntegerType
    assert fields["vendor_id"] == StringType
