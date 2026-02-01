"""Analytics service for time series and summary metrics."""

from datetime import datetime

import polars as pl
from dateutil.relativedelta import relativedelta

from analytics.cache import async_cached, cache
from analytics.metrics import ATOMICS, METRIC_REGISTRY
from analytics.models import (
    Granularity,
    Metric,
    PeriodComparison,
    PeriodRange,
    SeriesPoint,
    ServiceBreakdownItem,
    ServiceBreakdownResponse,
    SummaryResponse,
    TimeSeriesResponse,
)
from analytics.repositories.agenda import AgendaRepository

# Expression for customer cancellation — used only in get_services_breakdown
_is_customer_cancel = (
    (pl.col("isCancelled") == True) & (pl.col("cancelReason") == "CUSTOMER_CANCEL")  # noqa: E712
)


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
        definition = METRIC_REGISTRY[metric]

        # Custom metrics with their own scalar computation
        if definition.scalar_compute is not None:
            return definition.scalar_compute(lf)

        # Standard path: aggregate atomics then derive
        agg_exprs = [ATOMICS[name] for name in definition.requires]

        result = (
            lf.select(*agg_exprs)
            .with_columns(definition.derive_expr)
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
                period=PeriodRange(
                    start=prev_start.strftime("%Y-%m-%d"),
                    end=prev_end.strftime("%Y-%m-%d"),
                ),
                previous_value=prev_value,
                change_percent=self._change_percent(current_value, prev_value),
            ),
            previous_year=PeriodComparison(
                period=PeriodRange(
                    start=prev_year_start.strftime("%Y-%m-%d"),
                    end=prev_year_end.strftime("%Y-%m-%d"),
                ),
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

        # Collect required atomics from all requested metrics
        required_atomics: set[str] = set()
        for metric in metrics:
            required_atomics.update(METRIC_REGISTRY[metric].requires)
        agg_expressions = [ATOMICS[name] for name in required_atomics]

        # Add custom timeseries_agg expressions
        for metric in metrics:
            defn = METRIC_REGISTRY[metric]
            if defn.timeseries_agg is not None:
                agg_expressions.append(defn.timeseries_agg)

        # Step 1: Aggregate
        result = lf.group_by("period").agg(*agg_expressions)

        # Step 2: Derive standard metrics (those without timeseries_agg)
        derived_expressions = [
            METRIC_REGISTRY[m].derive_expr
            for m in metrics
            if METRIC_REGISTRY[m].derive_expr is not None and METRIC_REGISTRY[m].timeseries_agg is None
        ]
        if derived_expressions:
            result = result.with_columns(*derived_expressions)

        # Step 3: Select only period + requested metric columns
        metric_names = [m.value for m in metrics]
        result = result.select(["period"] + metric_names).sort("period").collect()

        # Build series with values dict
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

    @async_cached(cache)
    async def get_services_breakdown(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> ServiceBreakdownResponse:
        """Get breakdown of services with counts and cancellation rates."""
        lf = await self._load_lazy(start_date, end_date)

        total_appointments = lf.select(pl.len()).collect()[0, 0]

        # Explode services: one row per service per appointment
        result = (
            lf.explode("data.services")
            .filter(pl.col("data.services").is_not_null())
            .group_by("data.services")
            .agg(
                pl.len().alias("count"),
                _is_customer_cancel.sum().alias("cancelled"),
            )
            .with_columns(
                (pl.col("count").cast(pl.Float64) / max(total_appointments, 1) * 100)
                .round(2)
                .alias("percentage"),
                (
                    pl.when(pl.col("count") > 0)
                    .then(pl.col("cancelled").cast(pl.Float64) / pl.col("count") * 100)
                    .otherwise(0.0)
                    .round(2)
                    .alias("cancellation_rate")
                ),
            )
            .sort("count", descending=True)
            .collect()
        )

        services = [
            ServiceBreakdownItem(
                service=row["data.services"],
                count=row["count"],
                percentage=row["percentage"],
                cancelled=row["cancelled"],
                cancellation_rate=row["cancellation_rate"],
            )
            for row in result.to_dicts()
        ]

        return ServiceBreakdownResponse(
            period=PeriodRange(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
            ),
            total_appointments=total_appointments,
            services=services,
        )
