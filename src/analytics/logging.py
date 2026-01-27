"""Logging configuration with loguru."""

import sys
from functools import lru_cache

from loguru import logger

from analytics.config import Settings


def json_formatter(record: dict) -> str:
    """Format log record as JSON."""
    import json
    from datetime import datetime, timezone

    log_entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": record["level"].name,
        "message": record["message"],
        "module": record["name"],
        "function": record["function"],
        "line": record["line"],
    }

    # Add extra fields from record
    if record["extra"]:
        log_entry.update(record["extra"])

    # Add exception info if present
    if record["exception"]:
        log_entry["exception"] = {
            "type": record["exception"].type.__name__ if record["exception"].type else None,
            "value": str(record["exception"].value) if record["exception"].value else None,
            "traceback": record["exception"].traceback is not None,
        }

    return json.dumps(log_entry) + "\n"


@lru_cache
def setup_logging() -> None:
    """Configure loguru based on environment."""
    settings = Settings()

    # Remove default handler
    logger.remove()

    if settings.environment == "production":
        # JSON format for production
        logger.add(
            sys.stdout,
            format="{message}",
            level=settings.log_level,
            serialize=True,  # Built-in JSON serialization
        )
    else:
        # Colored output for development
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            level=settings.log_level,
            colorize=True,
        )

    logger.info("Logging configured", environment=settings.environment, level=settings.log_level)


def get_logger(name: str = __name__):
    """Get a logger instance with context."""
    return logger.bind(module=name)
