"""Insights service for automated analytics."""

from datetime import UTC, datetime

import polars as pl
from dateutil.relativedelta import relativedelta

from analytics.config import settings
from analytics.models import (
    InactiveCustomerItem,
    InactiveCustomersResponse,
    PeriodRange,
)
from analytics.repositories.agenda import AgendaRepository
from analytics.repositories.customers import CustomersRepository


class InsightsService:
    """Service for computing insight-based analytics."""

    def __init__(self, agenda_repo: AgendaRepository, customers_repo: CustomersRepository) -> None:
        self._agenda_repo = agenda_repo
        self._customers_repo = customers_repo

    async def get_inactive_customers(self, limit: int = 50) -> InactiveCustomersResponse:
        """Return customers with the fewest appointments in the configured window."""
        end_date = datetime.now(UTC)
        start_date = end_date - relativedelta(months=settings.insight_inactive_months)

        customers_lf = await self._customers_repo.find_all_as_lazy()
        appointments_lf = await self._agenda_repo.find_as_lazy(start_date, end_date)

        # Frequency per customer
        freq = appointments_lf.group_by("attendee.id").agg(
            pl.len().alias("total_appointments"),
            pl.col("start").max().alias("last_appointment"),
        )

        # Left join: include customers with 0 appointments
        result = (
            customers_lf.join(freq, left_on="_id", right_on="attendee.id", how="left")
            .with_columns(pl.col("total_appointments").fill_null(0))
        )

        # Threshold: 25th percentile of the distribution
        threshold_value = result.select(
            pl.col("total_appointments").quantile(0.25, interpolation="nearest")
        ).collect().item(0, 0)
        threshold = int(threshold_value)

        # Filter and sort by least frequent first
        inactive = (
            result.filter(pl.col("total_appointments") <= threshold)
            .sort("total_appointments", "last_appointment")
            .head(limit)
            .collect()
        )

        customers = [
            InactiveCustomerItem(
                id=str(row["_id"]),
                name=row["name"],
                surname=row["surname"],
                total_appointments=row["total_appointments"],
                last_appointment=(
                    row["last_appointment"].strftime("%Y-%m-%d")
                    if row["last_appointment"] is not None
                    else None
                ),
            )
            for row in inactive.iter_rows(named=True)
        ]

        return InactiveCustomersResponse(
            period=PeriodRange(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
            ),
            threshold=threshold,
            total_customers=result.collect().height,
            inactive_count=len(customers),
            customers=customers,
        )
