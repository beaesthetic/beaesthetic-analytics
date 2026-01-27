"""FastAPI application for BeAesthetic Analytics Service."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger
from pymongo import MongoClient

from analytics.logging import setup_logging
from analytics.middleware import LoggingMiddleware
from analytics.repositories.agenda import AgendaRepository, get_mongo_client
from analytics.routers import analytics_router, health_router
from analytics.routers.analytics import get_analytics_service
from analytics.services.analytics import AnalyticsService

# Shared MongoDB client
_mongo_client: MongoClient | None = None


def _create_analytics_service() -> AnalyticsService:
    """Factory for analytics service with current MongoDB client."""
    if _mongo_client is None:
        raise RuntimeError("MongoDB client not initialized")
    repository = AgendaRepository(_mongo_client)
    return AnalyticsService(repository)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    global _mongo_client

    # Setup logging
    setup_logging()
    logger.info("Starting BeAesthetic Analytics Service")

    # Initialize MongoDB
    _mongo_client = get_mongo_client()
    logger.info("MongoDB client initialized")

    yield

    # Cleanup
    if _mongo_client:
        _mongo_client.close()
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
app.dependency_overrides[get_analytics_service] = _create_analytics_service

# Routers
app.include_router(health_router)
app.include_router(analytics_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
