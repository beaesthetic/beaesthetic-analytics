"""Application configuration."""

from typing import Literal

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # MongoDB
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_database: str = "beaesthetic"

    # Logging
    environment: Literal["development", "production"] = "development"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    # Cache
    cache_maxsize: int = 256
    cache_ttl_closed: int = 86400  # TTL for closed periods (seconds)
    cache_ttl_open: int = 120  # TTL for open/current periods (seconds)

    model_config = {"env_prefix": "ANALYTICS_"}


settings = Settings()
