"""API routers."""

from analytics.routers.analytics import router as analytics_router
from analytics.routers.health import router as health_router
from analytics.routers.insights import router as insights_router

__all__ = ["analytics_router", "health_router", "insights_router"]
