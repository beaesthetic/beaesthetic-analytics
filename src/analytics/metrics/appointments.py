"""Appointment metrics."""

import polars as pl

from analytics.models import Metric

from .base import MetricDef

APPOINTMENTS_COUNT = MetricDef(
    key=Metric.APPOINTMENTS_COUNT,
    requires=["_total"],
    derive_expr=pl.col("_total").alias("appointments.count"),
)

APPOINTMENTS_CANCELLED = MetricDef(
    key=Metric.APPOINTMENTS_CANCELLED,
    requires=["_cancelled"],
    derive_expr=pl.col("_cancelled").alias("appointments.cancelled"),
)

APPOINTMENTS_COMPLETED = MetricDef(
    key=Metric.APPOINTMENTS_COMPLETED,
    requires=["_total", "_cancelled"],
    derive_expr=(pl.col("_total") - pl.col("_cancelled")).alias("appointments.completed"),
)

APPOINTMENTS_CANCELLATION_RATE = MetricDef(
    key=Metric.APPOINTMENTS_CANCELLATION_RATE,
    requires=["_total", "_cancelled"],
    derive_expr=(
        pl.when(pl.col("_total") > 0)
        .then(pl.col("_cancelled") / pl.col("_total") * 100)
        .otherwise(0.0)
        .round(2)
        .alias("appointments.cancellation_rate")
    ),
)
