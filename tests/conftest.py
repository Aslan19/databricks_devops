import pytest
import sys
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    sys.dont_write_bytecode = True

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("Must run inside a Databricks cluster.")

    return spark
