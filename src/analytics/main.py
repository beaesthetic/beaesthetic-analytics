"""FastAPI application for BeAesthetic Analytics Service."""

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from loguru import logger

from analytics.log_config import setup_logging
from analytics.middleware import LoggingMiddleware
from analytics.repositories.agenda import AgendaRepository, get_mongo_client
from analytics.repositories.customers import CustomersRepository
from analytics.routers import analytics_router, health_router, insights_router
from analytics.routers.analytics import get_analytics_service
from analytics.routers.insights import get_insights_service
from analytics.services.analytics import AnalyticsService
from analytics.services.insights import InsightsService


def create_analytics_service(request: Request) -> AnalyticsService:
    """Dependency that creates AnalyticsService from app state."""
    repository = AgendaRepository(request.app.state.mongo_client)
    return AnalyticsService(repository)


def create_insights_service(request: Request) -> InsightsService:
    """Dependency that creates InsightsService from app state."""
    agenda_repo = AgendaRepository(request.app.state.mongo_client)
    customers_repo = CustomersRepository(request.app.state.mongo_client)
    return InsightsService(agenda_repo, customers_repo)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Setup logging
    setup_logging()
    logger.info("Starting BeAesthetic Analytics Service")

    # Initialize MongoDB and store in app state
    app.state.mongo_client = get_mongo_client()
    logger.info("MongoDB client initialized")

    yield

    # Cleanup
    app.state.mongo_client.close()
    logger.info("MongoDB client closed")
    logger.info("Service shutdown complete")


# Create application
app = FastAPI(
    title="BeAesthetic Analytics Service",
    description="Analytics API with PyMongoArrow + Polars LazyFrames",
    version="0.1.0",
    lifespan=lifespan,
)

# Middleware
app.add_middleware(LoggingMiddleware)

# Dependency overrides
app.dependency_overrides[get_analytics_service] = create_analytics_service
app.dependency_overrides[get_insights_service] = create_insights_service

# Routers
app.include_router(health_router)
app.include_router(analytics_router)
app.include_router(insights_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
