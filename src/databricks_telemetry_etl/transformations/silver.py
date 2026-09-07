from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, year
from pyspark import pipelines as dp


def cleanse_events(events: DataFrame) -> DataFrame:
    return (
        events
        .withColumn("event_time", to_timestamp("event_time"))
        .filter(col("userId").isNotNull())
        .filter(year("event_time") > 1900)
    )

@dp.table
def silver_eu_telemetry_events():
    bronze = dp.read_stream("bronze_telemetry_events")
    eu_events = bronze.filter(col("geoArea") == "Europe")
    return cleanse_events(eu_events)

@dp.table
def silver_row_telemetry_events():
    bronze = dp.read_stream("bronze_telemetry_events")
    row_events = bronze.filter(col("geoArea") != "Europe")
    return cleanse_events(row_events)