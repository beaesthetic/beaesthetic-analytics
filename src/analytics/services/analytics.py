"""Analytics service with MoM and YoY calculations using Polars."""

from datetime import datetime

import polars as pl

from analytics.models import AppointmentSummary, MoMComparison, YoYComparison
from analytics.repositories.agenda import AgendaRepository


class AnalyticsService:
    """Service for computing appointment analytics using Polars."""

    def __init__(self, repository: AgendaRepository) -> None:
        self._repository = repository

    async def _load_dataframe(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> pl.DataFrame:
        """Load appointments from MongoDB into a Polars DataFrame."""
        appointments = await self._repository.find_by_created_at_range(start_date, end_date)

        if not appointments:
            return pl.DataFrame(
                schema={
                    "_id": pl.Utf8,
                    "start": pl.Datetime("us"),
                    "end": pl.Datetime("us"),
                    "isCancelled": pl.Boolean,
                    "createdAt": pl.Datetime("us"),
                }
            )

        records = [
            {
                "_id": apt["_id"],
                "start": apt["start"],
                "end": apt["end"],
                "isCancelled": apt.get("isCancelled", False),
                "createdAt": apt["createdAt"],
            }
            for apt in appointments
        ]

        return pl.DataFrame(records)

    def _summarize(self, df: pl.DataFrame, period: str) -> AppointmentSummary:
        """Compute summary statistics from DataFrame using Polars."""
        if df.is_empty():
            return AppointmentSummary(
                period=period,
                total_count=0,
                cancelled_count=0,
                completed_count=0,
                cancellation_rate=0.0,
            )

        stats = df.select(
            pl.len().alias("total"),
            pl.col("isCancelled").sum().alias("cancelled"),
        ).row(0, named=True)

        total = stats["total"]
        cancelled = stats["cancelled"]

        return AppointmentSummary(
            period=period,
            total_count=total,
            cancelled_count=cancelled,
            completed_count=total - cancelled,
            cancellation_rate=round((cancelled / total * 100) if total > 0 else 0.0, 2),
        )

    async def get_appointments_in_range(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Get all appointments created within a date range."""
        return await self._repository.find_by_created_at_range(start_date, end_date)

    async def compute_mom(self, year: int, month: int) -> MoMComparison:
        """Compute Month over Month comparison using Polars."""
        # Date boundaries
        current_start = datetime(year, month, 1)
        if month == 12:
            current_end = datetime(year + 1, 1, 1)
            prev_start = datetime(year, 11, 1)
        else:
            current_end = datetime(year, month + 1, 1)
            prev_start = datetime(year - 1, 12, 1) if month == 1 else datetime(year, month - 1, 1)
        prev_end = current_start

        # Load all data in one query
        df = await self._load_dataframe(prev_start, current_end)

        if df.is_empty():
            current_df = df
            prev_df = df
        else:
            # Use Polars to extract year/month and filter
            df = df.with_columns(
                pl.col("createdAt").dt.year().alias("_year"),
                pl.col("createdAt").dt.month().alias("_month"),
            )

            current_df = df.filter(
                (pl.col("_year") == year) & (pl.col("_month") == month)
            )

            prev_month = 12 if month == 1 else month - 1
            prev_year = year - 1 if month == 1 else year
            prev_df = df.filter(
                (pl.col("_year") == prev_year) & (pl.col("_month") == prev_month)
            )

        # Summaries
        current_period = f"{year}-{month:02d}"
        prev_month_num = 12 if month == 1 else month - 1
        prev_year_num = year - 1 if month == 1 else year
        prev_period = f"{prev_year_num}-{prev_month_num:02d}"

        current_summary = self._summarize(current_df, current_period)
        prev_summary = self._summarize(prev_df, prev_period)

        # Calculate changes with Polars
        changes = pl.DataFrame({
            "curr": [current_summary.total_count],
            "prev": [prev_summary.total_count],
            "curr_rate": [current_summary.cancellation_rate],
            "prev_rate": [prev_summary.cancellation_rate],
        }).select(
            (pl.col("curr") - pl.col("prev")).alias("count_change"),
            pl.when(pl.col("prev") > 0)
            .then((pl.col("curr") - pl.col("prev")) / pl.col("prev") * 100)
            .otherwise(0.0)
            .round(2)
            .alias("count_pct"),
            (pl.col("curr_rate") - pl.col("prev_rate")).round(2).alias("rate_change"),
        ).row(0, named=True)

        return MoMComparison(
            current_month=current_summary,
            previous_month=prev_summary,
            count_change=changes["count_change"],
            count_change_percent=changes["count_pct"],
            cancellation_rate_change=changes["rate_change"],
        )

    async def compute_yoy(self, year: int, month: int | None = None) -> YoYComparison:
        """Compute Year over Year comparison using Polars."""
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

        # Load both periods
        current_df = await self._load_dataframe(current_start, current_end)
        prev_df = await self._load_dataframe(prev_start, prev_end)

        current_summary = self._summarize(current_df, current_period)
        prev_summary = self._summarize(prev_df, prev_period)

        # Calculate changes with Polars
        changes = pl.DataFrame({
            "curr": [current_summary.total_count],
            "prev": [prev_summary.total_count],
            "curr_rate": [current_summary.cancellation_rate],
            "prev_rate": [prev_summary.cancellation_rate],
        }).select(
            (pl.col("curr") - pl.col("prev")).alias("count_change"),
            pl.when(pl.col("prev") > 0)
            .then((pl.col("curr") - pl.col("prev")) / pl.col("prev") * 100)
            .otherwise(0.0)
            .round(2)
            .alias("count_pct"),
            (pl.col("curr_rate") - pl.col("prev_rate")).round(2).alias("rate_change"),
        ).row(0, named=True)

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
        """Get daily breakdown using Polars aggregations."""
        df = await self._load_dataframe(start_date, end_date)

        if df.is_empty():
            return []

        return (
            df.with_columns(pl.col("createdAt").dt.date().alias("date"))
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
            .to_dicts()
        )

    async def get_monthly_trend(self, year: int) -> list[dict]:
        """Get monthly trend for a year using Polars."""
        df = await self._load_dataframe(datetime(year, 1, 1), datetime(year + 1, 1, 1))

        if df.is_empty():
            return []

        return (
            df.with_columns(
                pl.col("createdAt").dt.month().alias("month"),
            )
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
            .to_dicts()
        )

    async def get_weekday_distribution(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Get weekday distribution using Polars."""
        df = await self._load_dataframe(start_date, end_date)

        if df.is_empty():
            return []

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
            df.with_columns(pl.col("createdAt").dt.weekday().alias("weekday"))
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
            .to_dicts()
        )

    async def get_hourly_distribution(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Get hourly distribution using Polars."""
        df = await self._load_dataframe(start_date, end_date)

        if df.is_empty():
            return []

        return (
            df.with_columns(pl.col("start").dt.hour().alias("hour"))
            .group_by("hour")
            .agg(
                pl.len().alias("total"),
                pl.col("isCancelled").sum().alias("cancelled"),
            )
            .with_columns((pl.col("total") - pl.col("cancelled")).alias("completed"))
            .sort("hour")
            .to_dicts()
        )
