from pyspark.sql.functions import (
    col, to_date, count, sum, avg, round,
    current_timestamp, when, datediff
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

def get_nyc_schema():
    return StructType([
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("rate_code_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])


def clean_bronze(df):
    """Adds metadata columns and applies basic cleanup."""
    return (
        df.withColumn("processing_time", current_timestamp())
          .withColumn("vendor_id", when(col("vendor_id") == "", None).otherwise(col("vendor_id")))
    )


def validate_and_cast_silver(df):
    """Cleans & casts columns into correct types, validates numeric ranges."""
    return (
        df
        # validate numeric values
        .filter((col("fare_amount") > 0) & (col("trip_distance") > 0))
        .filter(col("pickup_datetime").isNotNull() & col("dropoff_datetime").isNotNull())

        # cast all fields to expected types
        .select(
            col("vendor_id").cast("string"),
            col("pickup_datetime").cast("timestamp"),
            col("dropoff_datetime").cast("timestamp"),
            col("passenger_count").cast("int"),
            col("trip_distance").cast("double"),
            col("pickup_longitude").cast("double"),
            col("pickup_latitude").cast("double"),
            col("rate_code_id").cast("int"),
            col("store_and_fwd_flag").cast("string"),
            col("dropoff_longitude").cast("double"),
            col("dropoff_latitude").cast("double"),
            col("payment_type").cast("string"),
            col("fare_amount").cast("double"),
            col("extra").cast("double"),
            col("mta_tax").cast("double"),
            col("tip_amount").cast("double"),
            col("tolls_amount").cast("double"),
            col("total_amount").cast("double"),
        )
    )


def enrich_silver(df):
    """Adds new business-level transformations."""
    return (
        df
        # trip duration in minutes
        .withColumn("trip_duration_minutes",
                    (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60)

        # avg speed in miles per hour
        .withColumn("average_speed_mph",
                    col("trip_distance") / (col("trip_duration_minutes") / 60))

        # normalize vendor_id
        .withColumn("vendor_id_norm",
                    when(col("vendor_id").isin("1", "2"), col("vendor_id"))
                    .otherwise("unknown"))

        # date column for partitioning and aggregations
        .withColumn("pickup_date", to_date(col("pickup_datetime")))
    )