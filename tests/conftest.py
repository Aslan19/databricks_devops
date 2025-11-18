import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    # Create a Spark session. If one already exists, reuse it.
    spark = SparkSession.builder.getOrCreate()
    
    # Pass the Spark session to the test function.
    yield spark
