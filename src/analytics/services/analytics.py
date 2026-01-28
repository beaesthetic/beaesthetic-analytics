"""Analytics service with MoM and YoY calculations using Polars LazyFrames."""

from datetime import datetime

import polars as pl

from analytics.models import (
    AppointmentSummary,
    Granularity,
    Metric,
    MoMComparison,
    SeriesPoint,
    TimeSeriesResponse,
    YoYComparison,
)
from analytics.repositories.agenda import AgendaRepository

# Expression for customer cancellation (disdetta)
_is_customer_cancel = (pl.col("isCancelled") == True) & (pl.col("cancelReason") == "CUSTOMER_CANCEL")
_cancelled_count = pl.when(_is_customer_cancel).then(1).otherwise(0).sum()

# Mapping from metric enum to Polars column/expression
METRIC_EXPRESSIONS = {
    Metric.APPOINTMENTS_COUNT: pl.len().alias("appointments.count"),
    Metric.APPOINTMENTS_CANCELLED: _cancelled_count.alias("appointments.cancelled"),
    Metric.APPOINTMENTS_COMPLETED: (pl.len() - _cancelled_count).alias("appointments.completed"),
    Metric.APPOINTMENTS_CANCELLATION_RATE: (
        pl.when(pl.len() > 0)
        .then(_cancelled_count / pl.len() * 100)
        .otherwise(0.0)
        .round(2)
        .alias("appointments.cancellation_rate")
    ),
}


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

    def _summarize(self, lf: pl.LazyFrame, period: str) -> AppointmentSummary:
        """Compute summary statistics from LazyFrame."""
        stats = (
            lf.select(
                pl.len().alias("total"),
                pl.col("isCancelled").sum().alias("cancelled"),
            )
            .collect()
        )

        if stats.is_empty() or stats["total"][0] == 0:
            return AppointmentSummary(
                period=period,
                total_count=0,
                cancelled_count=0,
                completed_count=0,
                cancellation_rate=0.0,
            )

        total = stats["total"][0]
        cancelled = stats["cancelled"][0]

        return AppointmentSummary(
            period=period,
            total_count=total,
            cancelled_count=cancelled,
            completed_count=total - cancelled,
            cancellation_rate=round((cancelled / total * 100) if total > 0 else 0.0, 2),
        )

    async def compute_mom(self, year: int, month: int) -> MoMComparison:
        """Compute Month over Month comparison using Polars LazyFrames."""
        # Date boundaries
        current_start = datetime(year, month, 1)
        if month == 12:
            current_end = datetime(year + 1, 1, 1)
            prev_start = datetime(year, 11, 1)
        else:
            current_end = datetime(year, month + 1, 1)
            prev_start = datetime(year - 1, 12, 1) if month == 1 else datetime(year, month - 1, 1)

        # Load all data in one query, filter lazily
        lf = await self._load_lazy(prev_start, current_end)

        # Lazy filtering
        lf = lf.with_columns(
            pl.col("createdAt").dt.year().alias("_year"),
            pl.col("createdAt").dt.month().alias("_month"),
        )

        current_lf = lf.filter(
            (pl.col("_year") == year) & (pl.col("_month") == month)
        )

        prev_month = 12 if month == 1 else month - 1
        prev_year = year - 1 if month == 1 else year
        prev_lf = lf.filter(
            (pl.col("_year") == prev_year) & (pl.col("_month") == prev_month)
        )

        # Summaries
        current_period = f"{year}-{month:02d}"
        prev_period = f"{prev_year}-{prev_month:02d}"

        current_summary = self._summarize(current_lf, current_period)
        prev_summary = self._summarize(prev_lf, prev_period)

        # Calculate changes with Polars
        changes = (
            pl.LazyFrame({
                "curr": [current_summary.total_count],
                "prev": [prev_summary.total_count],
                "curr_rate": [current_summary.cancellation_rate],
                "prev_rate": [prev_summary.cancellation_rate],
            })
            .select(
                (pl.col("curr") - pl.col("prev")).alias("count_change"),
                pl.when(pl.col("prev") > 0)
                .then((pl.col("curr") - pl.col("prev")) / pl.col("prev") * 100)
                .otherwise(0.0)
                .round(2)
                .alias("count_pct"),
                (pl.col("curr_rate") - pl.col("prev_rate")).round(2).alias("rate_change"),
            )
            .collect()
            .row(0, named=True)
        )

        return MoMComparison(
            current_month=current_summary,
            previous_month=prev_summary,
            count_change=changes["count_change"],
            count_change_percent=changes["count_pct"],
            cancellation_rate_change=changes["rate_change"],
        )

    async def compute_yoy(self, year: int, month: int | None = None) -> YoYComparison:
        """Compute Year over Year comparison using Polars LazyFrames."""
        if month:
            current_start = datetime(year, month, 1)
            current_end = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
            prev_start = datetime(year - 1, month, 1)
            prev_end = datetime(year, 1, 1) if month == 12 else datetime(year - 1, month + 1, 1)
            current_period = f"{year}-{month:02d}"
            prev_period = f"{year - 1}-{month:02d}"
        else:
            current_start = datetime(year, 1, 1)
            current_end = datetime(year + 1, 1, 1)
            prev_start = datetime(year - 1, 1, 1)
            prev_end = datetime(year, 1, 1)
            current_period = str(year)
            prev_period = str(year - 1)

        # Load both periods as LazyFrames
        current_lf = await self._load_lazy(current_start, current_end)
        prev_lf = await self._load_lazy(prev_start, prev_end)

        current_summary = self._summarize(current_lf, current_period)
        prev_summary = self._summarize(prev_lf, prev_period)

        # Calculate changes
        changes = (
            pl.LazyFrame({
                "curr": [current_summary.total_count],
                "prev": [prev_summary.total_count],
                "curr_rate": [current_summary.cancellation_rate],
                "prev_rate": [prev_summary.cancellation_rate],
            })
            .select(
                (pl.col("curr") - pl.col("prev")).alias("count_change"),
                pl.when(pl.col("prev") > 0)
                .then((pl.col("curr") - pl.col("prev")) / pl.col("prev") * 100)
                .otherwise(0.0)
                .round(2)
                .alias("count_pct"),
                (pl.col("curr_rate") - pl.col("prev_rate")).round(2).alias("rate_change"),
            )
            .collect()
            .row(0, named=True)
        )

        return YoYComparison(
            current_year=current_summary,
            previous_year=prev_summary,
            count_change=changes["count_change"],
            count_change_percent=changes["count_pct"],
            cancellation_rate_change=changes["rate_change"],
        )

    async def get_timeseries(
        self,
        granularity: Granularity,
        metrics: list[Metric],
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
                    pl.col("createdAt")
                    .dt.convert_time_zone(timezone)
                    .dt.strftime("%Y-%m-%d")
                    .alias("period")
                )
            case Granularity.WEEK:
                lf = lf.with_columns(
                    pl.col("createdAt")
                    .dt.convert_time_zone(timezone)
                    .dt.strftime("%G-W%V")
                    .alias("period")
                )
            case Granularity.MONTH:
                lf = lf.with_columns(
                    pl.col("createdAt")
                    .dt.convert_time_zone(timezone)
                    .dt.strftime("%Y-%m")
                    .alias("period")
                )
            case Granularity.YEAR:
                lf = lf.with_columns(
                    pl.col("createdAt")
                    .dt.convert_time_zone(timezone)
                    .dt.strftime("%Y")
                    .alias("period")
                )

        # Build aggregation expressions for requested metrics
        agg_expressions = [METRIC_EXPRESSIONS[m] for m in metrics]

        # Aggregate by period with requested metrics
        result = (
            lf.group_by("period")
            .agg(*agg_expressions)
            .sort("period")
            .collect()
        )

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
