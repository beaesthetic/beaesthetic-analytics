"""Analytics service for time series and summary metrics."""

from datetime import datetime

import polars as pl
from dateutil.relativedelta import relativedelta

from analytics.cache import async_cached, cache
from analytics.models import (
    Granularity,
    Metric,
    PeriodComparison,
    PeriodRange,
    SeriesPoint,
    SummaryResponse,
    TimeSeriesResponse,
)
from analytics.repositories.agenda import AgendaRepository

# Expression for customer cancellation (disdetta)
_is_customer_cancel = (pl.col("isCancelled") == True) & (pl.col("cancelReason") == "CUSTOMER_CANCEL")

# Atomic expressions (computed in .agg())
ATOMIC_EXPRESSIONS = {
    "_total": pl.len().alias("_total"),
    "_cancelled": pl.when(_is_customer_cancel).then(1).otherwise(0).sum().alias("_cancelled"),
}

# Metric configuration: dependencies and how to compute/rename
METRIC_CONFIG = {
    Metric.APPOINTMENTS_COUNT: {
        "requires": ["_total"],
        "expr": pl.col("_total").alias("appointments.count"),
    },
    Metric.APPOINTMENTS_CANCELLED: {
        "requires": ["_cancelled"],
        "expr": pl.col("_cancelled").alias("appointments.cancelled"),
    },
    Metric.APPOINTMENTS_COMPLETED: {
        "requires": ["_total", "_cancelled"],
        "expr": (pl.col("_total") - pl.col("_cancelled")).alias("appointments.completed"),
    },
    Metric.APPOINTMENTS_CANCELLATION_RATE: {
        "requires": ["_total", "_cancelled"],
        "expr": (
            pl.when(pl.col("_total") > 0)
            .then(pl.col("_cancelled") / pl.col("_total") * 100)
            .otherwise(0.0)
            .round(2)
            .alias("appointments.cancellation_rate")
        ),
    },
}


def _resolve_dependencies(metrics: list[Metric]) -> set[str]:
    """Resolve which atomic columns are needed for the requested metrics."""
    required = set()
    for metric in metrics:
        required.update(METRIC_CONFIG[metric]["requires"])
    return required


class AnalyticsService:
    """Service for computing appointment analytics using Polars LazyFrames."""

    def __init__(self, repository: AgendaRepository) -> None:
        self._repository = repository

    async def _load_lazy(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> pl.LazyFrame:
        """Load appointments as LazyFrame for deferred execution."""
        return await self._repository.find_as_lazy(start_date, end_date)

    async def _compute_metric(
        self,
        metric: Metric,
        start_date: datetime,
        end_date: datetime,
    ) -> int | float:
        """Compute a single metric value for a date range."""
        lf = await self._load_lazy(start_date, end_date)

        # Resolve and compute atomic dependencies
        config = METRIC_CONFIG[metric]
        required = config["requires"]
        agg_exprs = [ATOMIC_EXPRESSIONS[name] for name in required]

        result = (
            lf.select(*agg_exprs)
            .with_columns(config["expr"])
            .select(metric.value)
            .collect()
        )

        if result.is_empty():
            return 0

        return result[metric.value][0]

    @staticmethod
    def _change_percent(current: int | float, previous: int | float) -> float | None:
        """Calculate percentage change, None if no previous data."""
        if previous == 0:
            return None
        return round((current - previous) / previous * 100, 2)

    @async_cached(cache)
    async def get_summary(
        self,
        metric: Metric,
        start_date: datetime,
        end_date: datetime,
    ) -> SummaryResponse:
        """Get metric summary with previous period and previous year comparisons."""
        # Current value
        current_value = await self._compute_metric(metric, start_date, end_date)

        # Previous period: shift range back by same duration
        duration = end_date - start_date
        prev_start = start_date - duration
        prev_end = end_date - duration
        prev_value = await self._compute_metric(metric, prev_start, prev_end)

        # Previous year: shift range back by 1 year
        prev_year_start = start_date - relativedelta(years=1)
        prev_year_end = end_date - relativedelta(years=1)
        prev_year_value = await self._compute_metric(metric, prev_year_start, prev_year_end)

        return SummaryResponse(
            metric=metric.value,
            period=PeriodRange(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
            ),
            value=current_value,
            previous_period=PeriodComparison(
                previous_value=prev_value,
                change_percent=self._change_percent(current_value, prev_value),
            ),
            previous_year=PeriodComparison(
                previous_value=prev_year_value,
                change_percent=self._change_percent(current_value, prev_year_value),
            ),
        )

    @async_cached(cache)
    async def get_timeseries(
        self,
        granularity: Granularity,
        metrics: tuple[Metric, ...],
        start_date: datetime,
        end_date: datetime,
        timezone: str,
    ) -> TimeSeriesResponse:
        """Get time series analytics by granularity and metrics."""
        lf = await self._load_lazy(start_date, end_date)

        # Convert to timezone and extract period based on granularity
        match granularity:
            case Granularity.DAY:
                lf = lf.with_columns(
                    pl.col("start")
                    .dt.convert_time_zone(timezone)
                    .dt.strftime("%Y-%m-%d")
                    .alias("period")
                )
            case Granularity.WEEK:
                lf = lf.with_columns(
                    pl.col("start")
                    .dt.convert_time_zone(timezone)
                    .dt.strftime("%G-W%V")
                    .alias("period")
                )
            case Granularity.MONTH:
                lf = lf.with_columns(
                    pl.col("start")
                    .dt.convert_time_zone(timezone)
                    .dt.strftime("%Y-%m")
                    .alias("period")
                )
            case Granularity.YEAR:
                lf = lf.with_columns(
                    pl.col("start")
                    .dt.convert_time_zone(timezone)
                    .dt.strftime("%Y")
                    .alias("period")
                )

        # Resolve dependencies: which atomic columns do we need?
        required_atomics = _resolve_dependencies(metrics)
        agg_expressions = [ATOMIC_EXPRESSIONS[name] for name in required_atomics]

        # Step 1: Aggregate atomic metrics
        result = lf.group_by("period").agg(*agg_expressions)

        # Step 2: Derive requested metrics from atomics
        derived_expressions = [METRIC_CONFIG[m]["expr"] for m in metrics]
        result = result.with_columns(*derived_expressions)

        # Step 3: Select only period + requested metric columns
        metric_names = [m.value for m in metrics]
        result = result.select(["period"] + metric_names).sort("period").collect()

        # Build series with values dict
        metric_names = [m.value for m in metrics]
        series = [
            SeriesPoint(
                period=row["period"],
                values={name: row[name] for name in metric_names},
            )
            for row in result.to_dicts()
        ]

        return TimeSeriesResponse(
            granularity=granularity,
            timezone=timezone,
            metrics=metric_names,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            series=series,
        )
