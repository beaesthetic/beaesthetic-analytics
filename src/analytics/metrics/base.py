"""Base metric definitions and atomic expressions."""

from dataclasses import dataclass, field
from typing import Callable

import polars as pl

from analytics.models import Metric

_is_customer_cancel = (
    (pl.col("isCancelled") == True) & (pl.col("cancelReason") == "CUSTOMER_CANCEL")  # noqa: E712
)

ATOMICS: dict[str, pl.Expr] = {
    "_total": pl.len().alias("_total"),
    "_cancelled": (
        pl.when(_is_customer_cancel).then(1).otherwise(0).sum().alias("_cancelled")
    ),
    "_services_total": (
        pl.col("data.services").list.len().sum().alias("_services_total")
    ),
}


@dataclass(frozen=True)
class MetricDef:
    """Declarative definition of a metric.

    Standard metrics provide ``requires`` + ``derive_expr``.
    Custom metrics provide ``timeseries_agg`` and/or ``scalar_compute``.
    """

    key: Metric
    requires: list[str] = field(default_factory=list)
    derive_expr: pl.Expr | None = None
    timeseries_agg: pl.Expr | None = None
    scalar_compute: Callable[[pl.LazyFrame], int | float] | None = None
