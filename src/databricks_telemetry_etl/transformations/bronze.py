from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

raw_json_path = spark.conf.get("telemetry.raw_json_path")
catalog = spark.conf.get("telemetry.catalog")
bronze_schema = spark.conf.get("telemetry.bronze_schema")
bronze_table = f"{catalog}.{bronze_schema}.bronze_telemetry_events"

@dp.table(
    name=bronze_table,
    comment="Raw telemetry events ingested incrementally from JSON landing files",
)
def bronze_telemetry_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load(raw_json_path)
        .select(
            "*",
            col("_metadata.file_path").alias("_source_file"),
            col("_metadata.file_modification_time").alias("_source_file_modified_at"),
            current_timestamp().alias("_ingested_at"),
        )
    )