"""Analytics business endpoints."""

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from analytics.models import Granularity, Metric, ServiceBreakdownResponse, SummaryResponse, TimeSeriesResponse
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
    return await service.get_timeseries(granularity, tuple(metrics), start_date, end_date, timezone)


@router.get("/services/breakdown", response_model=ServiceBreakdownResponse)
async def get_services_breakdown(
    start_date: Annotated[datetime, Query(description="Start date (inclusive)")],
    end_date: Annotated[datetime, Query(description="End date (exclusive)")],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> ServiceBreakdownResponse:
    """Get breakdown of services with counts and cancellation rates."""
    return await service.get_services_breakdown(start_date, end_date)


@router.get("/summary", response_model=SummaryResponse)
async def get_summary(
    metric: Annotated[Metric, Query(description="Metric to summarize")],
    start_date: Annotated[datetime, Query(description="Start date (inclusive)")],
    end_date: Annotated[datetime, Query(description="End date (exclusive)")],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> SummaryResponse:
    """Get metric summary with MoM and YoY comparisons."""
    return await service.get_summary(metric, start_date, end_date)
