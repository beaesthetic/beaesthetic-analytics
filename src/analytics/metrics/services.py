"""Service metrics."""

import polars as pl

from analytics.models import Metric

from .base import MetricDef


def _services_unique_count_scalar(lf: pl.LazyFrame) -> int | float:
    result = (
        lf.select(pl.col("data.services").explode().drop_nulls().n_unique())
        .collect()
    )
    if result.is_empty():
        return 0
    return result[0, 0]


SERVICES_UNIQUE_COUNT = MetricDef(
    key=Metric.SERVICES_UNIQUE_COUNT,
    scalar_compute=_services_unique_count_scalar,
    timeseries_agg=(
        pl.col("data.services")
        .explode()
        .drop_nulls()
        .n_unique()
        .alias("services.unique_count")
    ),
)

SERVICES_AVG_PER_APPOINTMENT = MetricDef(
    key=Metric.SERVICES_AVG_PER_APPOINTMENT,
    requires=["_services_total", "_total"],
    derive_expr=(
        pl.when(pl.col("_total") > 0)
        .then((pl.col("_services_total").cast(pl.Float64) / pl.col("_total")).round(2))
        .otherwise(0.0)
        .alias("services.avg_per_appointment")
    ),
)
