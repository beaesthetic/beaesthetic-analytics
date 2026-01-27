"""HTTP middleware for the analytics service."""

import time
from uuid import uuid4

from fastapi import Request
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware


class LoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for logging HTTP requests with timing and correlation ID."""

    async def dispatch(self, request: Request, call_next):
        """Process request with logging."""
        request_id = str(uuid4())[:8]
        start_time = time.perf_counter()

        with logger.contextualize(request_id=request_id):
            logger.info(
                "Request started",
                method=request.method,
                path=request.url.path,
                query=str(request.query_params) if request.query_params else None,
            )

            response = await call_next(request)

            duration_ms = (time.perf_counter() - start_time) * 1000
            logger.info(
                "Request completed",
                method=request.method,
                path=request.url.path,
                status_code=response.status_code,
                duration_ms=round(duration_ms, 2),
            )

        response.headers["X-Request-ID"] = request_id
        return response
