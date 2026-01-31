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


class PeriodComparison(BaseModel):
    """Comparison against a previous period."""

    previous_value: int | float
    change_percent: float | None


class PeriodRange(BaseModel):
    """A date range."""

    start: str
    end: str


class SummaryResponse(BaseModel):
    """Response for summary/KPI endpoint."""

    metric: str
    period: PeriodRange
    value: int | float
    mom: PeriodComparison | None
    yoy: PeriodComparison | None
