from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, year
from pyspark import pipelines as dp

catalog = spark.conf.get("telemetry.catalog")
bronze_schema = spark.conf.get("telemetry.bronze_schema")
silver_schema = spark.conf.get("telemetry.silver_schema")

bronze_table = f"{catalog}.{bronze_schema}.bronze_telemetry_events"
silver_eu_table = f"{catalog}.{silver_schema}.silver_eu_telemetry_events"
silver_row_table = f"{catalog}.{silver_schema}.silver_row_telemetry_events"

def cleanse_events(events: DataFrame) -> DataFrame:
    return (
        events
        .withColumn("event_time", to_timestamp("event_time"))
        .filter(col("userId").isNotNull())
        .filter(year("event_time") > 1900)
    )

@dp.table(name=silver_eu_table,
    comment="Silver table for EU telemetry events",):
def silver_eu_table():
    bronze = dp.read_stream(bronze_table)
    eu_events = bronze.filter(col("geoArea") == "Europe")
    return cleanse_events(eu_events)

@dp.table(name=silver_row_table,
    comment="Silver table for RoW telemetry events",):
def silver_row_table():
    bronze = dp.read_stream(bronze_table)
    row_events = bronze.filter(col("geoArea") != "Europe")
    return cleanse_events(row_events)