"""Customers repository for MongoDB access with Arrow integration."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import polars as pl
from pymongoarrow.api import find_arrow_all
from pymongo import MongoClient

from analytics.config import settings

_executor = ThreadPoolExecutor(max_workers=4)


class CustomersRepository:
    """Repository for accessing customers collection in MongoDB."""

    def __init__(self, client: MongoClient) -> None:
        self._collection = client[settings.mongodb_database]["customers"]

    def _find_all_arrow_sync(self):
        """Synchronous Arrow query - runs in thread pool."""
        return find_arrow_all(self._collection, {})

    async def find_all_as_polars(self) -> pl.DataFrame:
        """Find all customers and return as Polars DataFrame."""
        loop = asyncio.get_event_loop()
        arrow_table = await loop.run_in_executor(
            _executor,
            partial(self._find_all_arrow_sync),
        )
        df = pl.from_arrow(arrow_table)
        # Keep only relevant columns
        available = [c for c in ["_id", "name", "surname"] if c in df.columns]
        return df.select(available)

    async def find_all_as_lazy(self) -> pl.LazyFrame:
        """Find all customers and return as Polars LazyFrame."""
        df = await self.find_all_as_polars()
        return df.lazy()
