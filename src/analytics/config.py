"""Application configuration."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_database: str = "beaesthetic"

    model_config = {"env_prefix": "ANALYTICS_"}


settings = Settings()
