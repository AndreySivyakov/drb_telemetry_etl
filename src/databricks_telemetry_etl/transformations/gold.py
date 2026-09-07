from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, sha2, to_date


def dimension_key(column_name: str):
    return sha2(concat_ws("|", col(column_name).cast("string")), 256)


def all_clean_events() -> DataFrame:
    return dp.read("silver_eu_telemetry_events").unionByName(
        dp.read("silver_row_telemetry_events")
    )


@dp.materialized_view(name="dim_application", comment="Telemetry application dimension")
def dim_application():
    return all_clean_events().select(
        dimension_key("appName").alias("application_key"),
        col("appName").alias("app_name"),
    ).dropDuplicates()


@dp.materialized_view(name="dim_device", comment="Telemetry device-type dimension")
def dim_device():
    return all_clean_events().select(
        dimension_key("deviceType").alias("device_key"),
        col("deviceType").alias("device_type"),
    ).dropDuplicates()


@dp.materialized_view(name="dim_geography", comment="Telemetry geography dimension")
def dim_geography():
    return all_clean_events().select(
        dimension_key("geoArea").alias("geography_key"),
        col("geoArea").alias("geo_area"),
    ).dropDuplicates()


@dp.materialized_view(name="dim_operating_system", comment="Telemetry operating-system dimension")
def dim_operating_system():
    return all_clean_events().select(
        dimension_key("os").alias("operating_system_key"),
        col("os").alias("operating_system"),
    ).dropDuplicates()


@dp.materialized_view(name="dim_action", comment="Telemetry action dimension")
def dim_action():
    return all_clean_events().select(
        dimension_key("actionId").alias("action_key"),
        col("actionId").alias("action_id"),
    ).dropDuplicates()


@dp.materialized_view(name="dim_date", comment="Telemetry event-date dimension")
def dim_date():
    return all_clean_events().select(
        to_date("event_time").alias("event_date"),
    ).dropDuplicates().withColumn("date_key", col("event_date").cast("string"))


def telemetry_fact(events: DataFrame, region_group: str) -> DataFrame:
    return events.select(
        col("eventId").alias("event_id"),
        col("userId").alias("user_id"),
        col("event_time"),
        to_date("event_time").cast("string").alias("date_key"),
        dimension_key("appName").alias("application_key"),
        dimension_key("deviceType").alias("device_key"),
        dimension_key("geoArea").alias("geography_key"),
        dimension_key("os").alias("operating_system_key"),
        dimension_key("actionId").alias("action_key"),
        lit(region_group).alias("region_group"),
    )


@dp.materialized_view(
    name="fact_eu_telemetry_events",
    comment="Clean telemetry events for Europe",
)
def fact_eu_telemetry_events():
    return telemetry_fact(dp.read("silver_eu_telemetry_events"), "EU")


@dp.materialized_view(
    name="fact_row_telemetry_events",
    comment="Clean telemetry events for rest-of-world regions",
)
def fact_row_telemetry_events():
    return telemetry_fact(dp.read("silver_row_telemetry_events"), "RoW")