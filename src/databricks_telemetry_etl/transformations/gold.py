from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, count, count_distinct, lit, sha2, to_date

catalog = spark.conf.get("telemetry.catalog")
silver_schema = spark.conf.get("telemetry.silver_schema")
gold_schema = spark.conf.get("telemetry.gold_schema")

silver_eu_table = f"{catalog}.{silver_schema}.silver_eu_telemetry_events"
silver_row_table = f"{catalog}.{silver_schema}.silver_row_telemetry_events"
dim_application_table = f"{catalog}.{gold_schema}.dim_application"
dim_device_table = f"{catalog}.{gold_schema}.dim_device"
dim_geography_table = f"{catalog}.{gold_schema}.dim_geography"
dim_operating_system_table = f"{catalog}.{gold_schema}.dim_operating_system"
dim_action_table = f"{catalog}.{gold_schema}.dim_action"
dim_date_table = f"{catalog}.{gold_schema}.dim_date"
fact_eu_table = f"{catalog}.{gold_schema}.fact_eu_telemetry_events"
fact_row_table = f"{catalog}.{gold_schema}.fact_row_telemetry_events"

# Gold dimensional model: shared dimensions plus separate EU and RoW facts.
def dimension_key(column_name: str):
    # Deterministic keys let facts and dimensions use the same business-key value.
    return sha2(concat_ws("|", col(column_name).cast("string")), 256)


def all_clean_events() -> DataFrame:
    # Dimensions are shared, so they are built from both regional Silver tables.
    return dp.read(silver_eu_table).unionByName(
        dp.read(silver_row_table)
    )

@dp.materialized_view(name=dim_application_table, comment="Telemetry application dimension")
def dim_application():
    return all_clean_events().select(
        dimension_key("appName").alias("application_key"),
        col("appName").alias("app_name"),
    ).dropDuplicates()


@dp.materialized_view(name=dim_device_table, comment="Telemetry device-type dimension")
def dim_device():
    return all_clean_events().select(
        dimension_key("deviceType").alias("device_key"),
        col("deviceType").alias("device_type"),
    ).dropDuplicates()


@dp.materialized_view(name=dim_geography_table, comment="Telemetry geography dimension")
def dim_geography():
    return all_clean_events().select(
        dimension_key("geoArea").alias("geography_key"),
        col("geoArea").alias("geo_area"),
    ).dropDuplicates()


@dp.materialized_view(name=dim_operating_system_table, comment="Telemetry operating-system dimension")
def dim_operating_system():
    return all_clean_events().select(
        dimension_key("os").alias("operating_system_key"),
        col("os").alias("operating_system"),
    ).dropDuplicates()


@dp.materialized_view(name=dim_action_table, comment="Telemetry action dimension")
def dim_action():
    return all_clean_events().select(
        dimension_key("actionId").alias("action_key"),
        col("actionId").alias("action_id"),
    ).dropDuplicates()


@dp.materialized_view(name=dim_date_table, comment="Telemetry event-date dimension")
def dim_date():
    return all_clean_events().select(
        to_date("event_time").alias("event_date"),
    ).dropDuplicates().withColumn("date_key", col("event_date").cast("string"))


def telemetry_fact(events: DataFrame, region_group: str) -> DataFrame:
    # One row per day and dimension combination, with actions and distinct users as measures.
    keyed_events = events.select(
        col("userId").alias("user_id"),
        to_date("event_time").cast("string").alias("date_key"),
        dimension_key("appName").alias("application_key"),
        dimension_key("deviceType").alias("device_key"),
        dimension_key("geoArea").alias("geography_key"),
        dimension_key("os").alias("operating_system_key"),
        dimension_key("actionId").alias("action_key"),
        lit(region_group).alias("region_group"),
    )

    fact_dimensions = [
        "date_key",
        "application_key",
        "device_key",
        "geography_key",
        "operating_system_key",
        "action_key",
        "region_group",
    ]

    return keyed_events.groupBy(*fact_dimensions).agg(
        count(lit(1)).alias("action_count"),
        count_distinct("user_id").alias("user_count"),
    )


@dp.materialized_view(
    name=fact_eu_table,
    comment="Daily telemetry action and distinct-user counts for Europe",
)
def fact_eu_telemetry_events():
    return telemetry_fact(dp.read(silver_eu_table), "EU")


@dp.materialized_view(
    name=fact_row_table,
    comment="Daily telemetry action and distinct-user counts for rest-of-world regions",
)
def fact_row_telemetry_events():
    return telemetry_fact(dp.read(silver_row_table), "RoW")