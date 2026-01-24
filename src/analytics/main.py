"""FastAPI application for BeAesthetic Analytics Service."""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

from fastapi import Depends, FastAPI, Query
from pymongo import MongoClient

from analytics.models import MoMComparison, YoYComparison
from analytics.repositories.agenda import AgendaRepository, get_mongo_client
from analytics.services.analytics import AnalyticsService

# Shared MongoDB client
_mongo_client: MongoClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global _mongo_client
    _mongo_client = get_mongo_client()
    yield
    if _mongo_client:
        _mongo_client.close()


app = FastAPI(
    title="BeAesthetic Analytics Service",
    description="Analytics API with PyMongoArrow + Polars LazyFrames",
    version="0.1.0",
    lifespan=lifespan,
)


def get_repository() -> AgendaRepository:
    """Dependency for agenda repository."""
    if _mongo_client is None:
        raise RuntimeError("MongoDB client not initialized")
    return AgendaRepository(_mongo_client)


def get_analytics_service(
    repository: Annotated[AgendaRepository, Depends(get_repository)],
) -> AnalyticsService:
    """Dependency for analytics service."""
    return AnalyticsService(repository)


@app.get("/health")
async def health_check() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/analytics/daily")
async def get_daily_breakdown(
    start_date: Annotated[datetime, Query(description="Start date (inclusive)")],
    end_date: Annotated[datetime, Query(description="End date (exclusive)")],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> list[dict]:
    """Get daily breakdown of appointments (Polars LazyFrame aggregation)."""
    return await service.get_daily_breakdown(start_date, end_date)


@app.get("/analytics/monthly-trend")
async def get_monthly_trend(
    year: Annotated[int, Query(description="Year for the trend", ge=2000, le=2100)],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> list[dict]:
    """Get monthly trend for a full year (Polars LazyFrame aggregation)."""
    return await service.get_monthly_trend(year)


@app.get("/analytics/weekday-distribution")
async def get_weekday_distribution(
    start_date: Annotated[datetime, Query(description="Start date (inclusive)")],
    end_date: Annotated[datetime, Query(description="End date (exclusive)")],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> list[dict]:
    """Get appointment distribution by weekday (Polars LazyFrame aggregation)."""
    return await service.get_weekday_distribution(start_date, end_date)


@app.get("/analytics/hourly-distribution")
async def get_hourly_distribution(
    start_date: Annotated[datetime, Query(description="Start date (inclusive)")],
    end_date: Annotated[datetime, Query(description="End date (exclusive)")],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> list[dict]:
    """Get appointment distribution by hour (Polars LazyFrame aggregation)."""
    return await service.get_hourly_distribution(start_date, end_date)


@app.get("/analytics/mom", response_model=MoMComparison)
async def get_mom_comparison(
    year: Annotated[int, Query(description="Year for the comparison", ge=2000, le=2100)],
    month: Annotated[int, Query(description="Month for the comparison", ge=1, le=12)],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> MoMComparison:
    """Get Month over Month comparison (Polars LazyFrame analysis)."""
    return await service.compute_mom(year, month)


@app.get("/analytics/yoy", response_model=YoYComparison)
async def get_yoy_comparison(
    year: Annotated[int, Query(description="Year for the comparison", ge=2000, le=2100)],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
    month: Annotated[int | None, Query(description="Optional month for monthly YoY", ge=1, le=12)] = None,
) -> YoYComparison:
    """Get Year over Year comparison (Polars LazyFrame analysis)."""
    return await service.compute_yoy(year, month)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
