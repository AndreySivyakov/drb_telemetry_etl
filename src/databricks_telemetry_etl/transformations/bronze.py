from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

raw_json_path = spark.conf.get("telemetry.raw_json_path")


@dp.table(
    name="bronze_telemetry_events",
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