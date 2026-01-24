"""Pydantic models for the analytics service."""

from pydantic import BaseModel


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
