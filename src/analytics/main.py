"""FastAPI application for BeAesthetic Analytics Service."""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

from fastapi import Depends, FastAPI, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from analytics.models import (
    Appointment,
    AppointmentsResponse,
    MoMComparison,
    YoYComparison,
)
from analytics.repositories.agenda import AgendaRepository, get_database
from analytics.services.analytics import AnalyticsService


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    yield


app = FastAPI(
    title="BeAesthetic Analytics Service",
    description="Analytics API for appointment data with MoM and YoY comparisons (powered by Polars)",
    version="0.1.0",
    lifespan=lifespan,
)


def get_repository(
    db: Annotated[AsyncIOMotorDatabase, Depends(get_database)],
) -> AgendaRepository:
    """Dependency for agenda repository."""
    return AgendaRepository(db)


def get_analytics_service(
    repository: Annotated[AgendaRepository, Depends(get_repository)],
) -> AnalyticsService:
    """Dependency for analytics service."""
    return AnalyticsService(repository)


@app.get("/health")
async def health_check() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/appointments", response_model=AppointmentsResponse)
async def get_appointments(
    start_date: Annotated[datetime, Query(description="Start date (inclusive)")],
    end_date: Annotated[datetime, Query(description="End date (exclusive)")],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> AppointmentsResponse:
    """Get all appointments created within a date range."""
    appointments_data = await service.get_appointments_in_range(start_date, end_date)
    appointments = [Appointment.model_validate(apt) for apt in appointments_data]
    return AppointmentsResponse(appointments=appointments, total=len(appointments))


@app.get("/appointments/daily")
async def get_daily_breakdown(
    start_date: Annotated[datetime, Query(description="Start date (inclusive)")],
    end_date: Annotated[datetime, Query(description="End date (exclusive)")],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> list[dict]:
    """Get daily breakdown of appointments (Polars aggregation)."""
    return await service.get_daily_breakdown(start_date, end_date)


@app.get("/appointments/monthly-trend")
async def get_monthly_trend(
    year: Annotated[int, Query(description="Year for the trend", ge=2000, le=2100)],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> list[dict]:
    """Get monthly trend for a full year (Polars aggregation)."""
    return await service.get_monthly_trend(year)


@app.get("/appointments/weekday-distribution")
async def get_weekday_distribution(
    start_date: Annotated[datetime, Query(description="Start date (inclusive)")],
    end_date: Annotated[datetime, Query(description="End date (exclusive)")],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> list[dict]:
    """Get appointment distribution by weekday (Polars aggregation)."""
    return await service.get_weekday_distribution(start_date, end_date)


@app.get("/appointments/hourly-distribution")
async def get_hourly_distribution(
    start_date: Annotated[datetime, Query(description="Start date (inclusive)")],
    end_date: Annotated[datetime, Query(description="End date (exclusive)")],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> list[dict]:
    """Get appointment distribution by hour (Polars aggregation)."""
    return await service.get_hourly_distribution(start_date, end_date)


@app.get("/appointments/mom", response_model=MoMComparison)
async def get_mom_comparison(
    year: Annotated[int, Query(description="Year for the comparison", ge=2000, le=2100)],
    month: Annotated[int, Query(description="Month for the comparison", ge=1, le=12)],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
) -> MoMComparison:
    """Get Month over Month comparison (Polars analysis)."""
    return await service.compute_mom(year, month)


@app.get("/appointments/yoy", response_model=YoYComparison)
async def get_yoy_comparison(
    year: Annotated[int, Query(description="Year for the comparison", ge=2000, le=2100)],
    service: Annotated[AnalyticsService, Depends(get_analytics_service)],
    month: Annotated[int | None, Query(description="Optional month for monthly YoY", ge=1, le=12)] = None,
) -> YoYComparison:
    """Get Year over Year comparison (Polars analysis)."""
    return await service.compute_yoy(year, month)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
