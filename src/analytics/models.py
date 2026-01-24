"""Pydantic models for the analytics service."""

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class CancelReasonType(str, Enum):
    """Cancel reason types."""

    CUSTOMER_CANCEL = "CustomerCancel"
    NO_REASON = "NoReason"


class Attendee(BaseModel):
    """Attendee model."""

    id: str
    display_name: str = Field(alias="displayName")

    model_config = {"populate_by_name": True}


class AppointmentData(BaseModel):
    """Appointment schedule data."""

    type: Literal["appointment"] = "appointment"
    services: list[str]


class EventData(BaseModel):
    """Event schedule data."""

    type: Literal["event"] = "event"
    title: str
    description: str


class Appointment(BaseModel):
    """Appointment model matching AgendaEntity schema."""

    id: str = Field(alias="_id")
    start: datetime
    end: datetime
    attendee: Attendee
    data: AppointmentData | EventData
    cancel_reason: CancelReasonType | None = Field(default=None, alias="cancelReason")
    remind_before_seconds: int = Field(alias="remindBeforeSeconds")
    reminder_status: str = Field(alias="reminderStatus")
    reminder_sent_at: datetime | None = Field(default=None, alias="reminderSentAt")
    is_cancelled: bool = Field(alias="isCancelled")
    version: int
    created_at: datetime = Field(alias="createdAt")
    updated_at: datetime = Field(alias="updatedAt")

    model_config = {"populate_by_name": True}


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


class DateRangeQuery(BaseModel):
    """Query parameters for date range."""

    start_date: datetime
    end_date: datetime


class AppointmentsResponse(BaseModel):
    """Response for appointments list."""

    appointments: list[Appointment]
    total: int
