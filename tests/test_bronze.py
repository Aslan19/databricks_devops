from pyspark.sql import Row
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from helpers import project_functions

from pyspark.sql import SparkSession


def test_clean_bronze_adds_processing_time(spark):
    df = spark.createDataFrame([Row(vendor_id="1")])
    result = project_functions.clean_bronze(df)
    assert "processing_time" in result.columns

def test_clean_bronze_converts_empty_vendor_to_null(spark):
    df = spark.createDataFrame([Row(vendor_id="")])
    row = project_functions.clean_bronze(df).collect()[0]
    assert row.vendor_id is None
