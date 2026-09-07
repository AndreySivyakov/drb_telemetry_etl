from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, count_distinct, grouping, lit, when


DIMENSIONS = ["appName", "deviceType", "geoArea", "os", "actionId"]


def aggregate_all_rollups(events: DataFrame) -> DataFrame:
    aggregated = events.cube(*DIMENSIONS).agg(
        count(lit(1)).alias("action_count"),
        count_distinct("userId").alias("user_count"),
    )

    return aggregated.select(
        *[
            when(grouping(dimension) == 1, lit("AllUp"))
            .otherwise(col(dimension))
            .alias(dimension)
            for dimension in DIMENSIONS
        ],
        "action_count",
        "user_count",
    )


@dp.materialized_view(
    name="gold_eu_telemetry_metrics",
    comment="Telemetry action and user counts across all dimension rollups for Europe",
)
def gold_eu_telemetry_metrics():
    return aggregate_all_rollups(dp.read("silver_eu_telemetry_events"))


@dp.materialized_view(
    name="gold_row_telemetry_metrics",
    comment="Telemetry action and user counts across all dimension rollups for rest-of-world regions",
)
def gold_row_telemetry_metrics():
    return aggregate_all_rollups(dp.read("silver_row_telemetry_events"))