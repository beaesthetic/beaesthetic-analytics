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
    SERVICES_UNIQUE_COUNT = "services.unique_count"
    SERVICES_AVG_PER_APPOINTMENT = "services.avg_per_appointment"


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


class PeriodRange(BaseModel):
    """A date range."""

    start: str
    end: str


class PeriodComparison(BaseModel):
    """Comparison against a previous period."""

    period: PeriodRange
    previous_value: int | float
    change_percent: float | None


class SummaryResponse(BaseModel):
    """Response for summary/KPI endpoint."""

    metric: str
    period: PeriodRange
    value: int | float
    previous_period: PeriodComparison | None
    previous_year: PeriodComparison | None


class ServiceBreakdownItem(BaseModel):
    """Single service in the breakdown."""

    service: str
    count: int
    percentage: float
    cancelled: int
    cancellation_rate: float


class ServiceBreakdownResponse(BaseModel):
    """Response for service breakdown endpoint."""

    period: PeriodRange
    total_appointments: int
    services: list[ServiceBreakdownItem]


class InactiveCustomerItem(BaseModel):
    """Single inactive customer."""

    id: str
    name: str
    surname: str
    total_appointments: int
    last_appointment: str | None  # YYYY-MM-DD, None if never came


class InactiveCustomersResponse(BaseModel):
    """Response for inactive customers insight."""

    period: PeriodRange
    threshold: int
    total_customers: int
    inactive_count: int
    customers: list[InactiveCustomerItem]
