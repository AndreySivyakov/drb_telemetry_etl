from itertools import combinations
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, count, count_distinct, lit


DIMENSIONS = ["appName", "deviceType", "geoArea", "os", "actionId"]


def aggregate_all_rollups(events: DataFrame) -> DataFrame:
    rollups = []

    for dimension_count in range(len(DIMENSIONS) + 1):
        for grouped_dimensions in combinations(DIMENSIONS, dimension_count):
            aggregated = events.groupBy(*grouped_dimensions).agg(
                count(lit(1)).alias("action_count"),
                count_distinct("userId").alias("user_count"),
            )
            rollups.append(
                aggregated.select(
                    *[
                        coalesce(col(dimension), lit("AllUp")).alias(dimension)
                        if dimension in grouped_dimensions
                        else lit("AllUp").alias(dimension)
                        for dimension in DIMENSIONS
                    ],
                    "action_count",
                    "user_count",
                )
            )

    return reduce(lambda left, right: left.unionByName(right), rollups)


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