"""Analytics business endpoints."""

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from analytics.models import Granularity, Metric, MoMComparison, TimeSeriesResponse, YoYComparison
from analytics.services.analytics import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["Analytics"])


def get_analytics_service() -> AnalyticsService:
    """Dependency for analytics service - injected at app level."""
    raise NotImplementedError("Must be overridden by dependency_overrides")


@router.get("/timeseries", response_model=TimeSeriesResponse)
async def get_timeseries(
    granularity: Annotated[Granularity, Query(description="Time granularity (DAY, WEEK, MONTH, YEAR)")],
    metrics: Annotated[list[Metric], Query(description="Metrics to retrieve")],
    start_date: Annotated[datetime, Query(description="Start date (inclusive)")],
    end_date: Annotated[datetime, Query(description="End date (exclusive)")],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
    timezone: Annotated[str, Query(description="Timezone for aggregation")] = "UTC",
) -> TimeSeriesResponse:
    """Get time series analytics with configurable granularity and metrics."""
    return await service.get_timeseries(granularity, metrics, start_date, end_date, timezone)


@router.get("/mom", response_model=MoMComparison)
async def get_mom_comparison(
    year: Annotated[int, Query(description="Year for the comparison", ge=2000, le=2100)],
    month: Annotated[int, Query(description="Month for the comparison", ge=1, le=12)],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> MoMComparison:
    """Get Month over Month comparison."""
    return await service.compute_mom(year, month)


@router.get("/yoy", response_model=YoYComparison)
async def get_yoy_comparison(
    year: Annotated[int, Query(description="Year for the comparison", ge=2000, le=2100)],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
    month: Annotated[int | None, Query(description="Optional month for monthly YoY", ge=1, le=12)] = None,
) -> YoYComparison:
    """Get Year over Year comparison."""
    return await service.compute_yoy(year, month)
