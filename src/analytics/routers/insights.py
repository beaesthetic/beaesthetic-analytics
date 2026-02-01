"""Insights endpoints."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from analytics.models import InactiveCustomersResponse
from analytics.services.insights import InsightsService

router = APIRouter(prefix="/insights", tags=["Insights"])


def get_insights_service() -> InsightsService:
    """Dependency for insights service - injected at app level."""
    raise NotImplementedError("Must be overridden by dependency_overrides")


@router.get("/inactive-customers", response_model=InactiveCustomersResponse)
async def get_inactive_customers(
    service: Annotated[InsightsService, Depends(get_insights_service)],
    limit: Annotated[int, Query(description="Max results returned", ge=1)] = 50,
) -> InactiveCustomersResponse:
    """Get inactive customers based on configurable inactivity window."""
    return await service.get_inactive_customers(limit=limit)
