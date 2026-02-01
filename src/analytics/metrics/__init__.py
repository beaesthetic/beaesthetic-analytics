"""Metric registry."""

from analytics.models import Metric

from .appointments import (
    APPOINTMENTS_CANCELLATION_RATE,
    APPOINTMENTS_CANCELLED,
    APPOINTMENTS_COMPLETED,
    APPOINTMENTS_COUNT,
)
from .base import ATOMICS, MetricDef
from .services import SERVICES_AVG_PER_APPOINTMENT, SERVICES_UNIQUE_COUNT

METRIC_REGISTRY: dict[Metric, MetricDef] = {
    m.key: m
    for m in [
        APPOINTMENTS_COUNT,
        APPOINTMENTS_CANCELLED,
        APPOINTMENTS_COMPLETED,
        APPOINTMENTS_CANCELLATION_RATE,
        SERVICES_UNIQUE_COUNT,
        SERVICES_AVG_PER_APPOINTMENT,
    ]
}

__all__ = [
    "ATOMICS",
    "METRIC_REGISTRY",
    "MetricDef",
]
