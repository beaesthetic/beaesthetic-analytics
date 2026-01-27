"""Pydantic models for the analytics service."""

from enum import Enum

from pydantic import BaseModel


class Granularity(str, Enum):
    """Time granularity for analytics series."""

    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    YEAR = "YEAR"


class Metric(str, Enum):
    """Available metrics for analytics."""

    APPOINTMENTS_COUNT = "appointments.count"
    APPOINTMENTS_COMPLETED = "appointments.completed"
    APPOINTMENTS_CANCELLED = "appointments.cancelled"
    APPOINTMENTS_CANCELLATION_RATE = "appointments.cancellation_rate"


class SeriesPoint(BaseModel):
    """Single point in a time series."""

    period: str
    values: dict[str, int | float]


class TimeSeriesResponse(BaseModel):
    """Response for time series analytics."""

    granularity: Granularity
    timezone: str
    metrics: list[str]
    start_date: str
    end_date: str
    series: list[SeriesPoint]


class AppointmentSummary(BaseModel):
    """Summary of appointments for a period."""

    period: str
    total_count: int
    cancelled_count: int
    completed_count: int
    cancellation_rate: float


class MoMComparison(BaseModel):
    """Month over Month comparison."""

    current_month: AppointmentSummary
    previous_month: AppointmentSummary
    count_change: int
    count_change_percent: float
    cancellation_rate_change: float


class YoYComparison(BaseModel):
    """Year over Year comparison."""

    current_year: AppointmentSummary
    previous_year: AppointmentSummary
    count_change: int
    count_change_percent: float
    cancellation_rate_change: float
