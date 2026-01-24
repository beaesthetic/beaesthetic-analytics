"""Analytics service with MoM and YoY calculations using Polars LazyFrames."""

from datetime import datetime

import polars as pl

from analytics.models import AppointmentSummary, MoMComparison, YoYComparison
from analytics.repositories.agenda import AgendaRepository


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

    async def get_daily_breakdown(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Get daily breakdown using Polars lazy aggregations."""
        lf = await self._load_lazy(start_date, end_date)

        return (
            lf.with_columns(pl.col("createdAt").dt.date().alias("date"))
            .group_by("date")
            .agg(
                pl.len().alias("total"),
                pl.col("isCancelled").sum().alias("cancelled"),
            )
            .with_columns(
                (pl.col("total") - pl.col("cancelled")).alias("completed"),
                pl.when(pl.col("total") > 0)
                .then(pl.col("cancelled") / pl.col("total") * 100)
                .otherwise(0.0)
                .round(2)
                .alias("cancellation_rate"),
            )
            .sort("date")
            .collect()
            .to_dicts()
        )

    async def get_monthly_trend(self, year: int) -> list[dict]:
        """Get monthly trend for a year using Polars lazy execution."""
        lf = await self._load_lazy(datetime(year, 1, 1), datetime(year + 1, 1, 1))

        return (
            lf.with_columns(pl.col("createdAt").dt.month().alias("month"))
            .group_by("month")
            .agg(
                pl.len().alias("total"),
                pl.col("isCancelled").sum().alias("cancelled"),
            )
            .with_columns(
                (pl.col("total") - pl.col("cancelled")).alias("completed"),
                pl.when(pl.col("total") > 0)
                .then(pl.col("cancelled") / pl.col("total") * 100)
                .otherwise(0.0)
                .round(2)
                .alias("cancellation_rate"),
            )
            .sort("month")
            .collect()
            .to_dicts()
        )

    async def get_weekday_distribution(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Get weekday distribution using Polars lazy execution."""
        lf = await self._load_lazy(start_date, end_date)

        weekday_names = {
            1: "Monday",
            2: "Tuesday",
            3: "Wednesday",
            4: "Thursday",
            5: "Friday",
            6: "Saturday",
            7: "Sunday",
        }

        return (
            lf.with_columns(pl.col("createdAt").dt.weekday().alias("weekday"))
            .group_by("weekday")
            .agg(
                pl.len().alias("total"),
                pl.col("isCancelled").sum().alias("cancelled"),
            )
            .with_columns(
                (pl.col("total") - pl.col("cancelled")).alias("completed"),
                pl.when(pl.col("total") > 0)
                .then(pl.col("cancelled") / pl.col("total") * 100)
                .otherwise(0.0)
                .round(2)
                .alias("cancellation_rate"),
                pl.col("weekday")
                .replace_strict(weekday_names, default="Unknown")
                .alias("weekday_name"),
            )
            .sort("weekday")
            .collect()
            .to_dicts()
        )

    async def get_hourly_distribution(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Get hourly distribution using Polars lazy execution."""
        lf = await self._load_lazy(start_date, end_date)

        return (
            lf.with_columns(pl.col("start").dt.hour().alias("hour"))
            .group_by("hour")
            .agg(
                pl.len().alias("total"),
                pl.col("isCancelled").sum().alias("cancelled"),
            )
            .with_columns((pl.col("total") - pl.col("cancelled")).alias("completed"))
            .sort("hour")
            .collect()
            .to_dicts()
        )
