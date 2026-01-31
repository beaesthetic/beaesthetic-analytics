"""Async-aware caching with per-item TTL for analytics services."""

import functools
import inspect
import time
from datetime import datetime, timezone

from cachetools import TLRUCache
from loguru import logger

from analytics.config import settings


class _CacheEntry:
    """Wrapper for cached values with end_date metadata for TTL computation."""

    __slots__ = ("value", "end_date")

    def __init__(self, value, end_date: datetime):
        self.value = value
        self.end_date = end_date


def _ttu(_key, entry: _CacheEntry, now):
    """Compute per-item TTU (time-to-use) based on whether the period is closed."""
    if entry.end_date < datetime.now(tz=entry.end_date.tzinfo):
        return now + settings.cache_ttl_closed
    return now + settings.cache_ttl_open


# Service-level cache with per-item TTL
cache = TLRUCache(maxsize=settings.cache_maxsize, ttu=_ttu, timer=time.monotonic)


def async_cached(cache: TLRUCache, *, end_date_arg: str = "end_date"):
    """Decorator for caching async method results with per-item TTL.

    Args:
        cache: TLRUCache instance.
        end_date_arg: Name of the argument used to determine if the period is closed.
    """
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(self, *args, **kwargs):
            key = (fn.__name__, args, tuple(sorted(kwargs.items())))
            try:
                entry = cache[key]
                logger.debug("Cache hit", method=fn.__name__)
                return entry.value
            except KeyError:
                pass

            result = await fn(self, *args, **kwargs)

            # Extract end_date from bound arguments for TTL computation
            bound = inspect.signature(fn).bind(self, *args, **kwargs)
            bound.apply_defaults()
            end_date = bound.arguments.get(end_date_arg, datetime.now(timezone.utc))

            cache[key] = _CacheEntry(value=result, end_date=end_date)
            logger.debug("Cache miss", method=fn.__name__)
            return result
        return wrapper
    return decorator
