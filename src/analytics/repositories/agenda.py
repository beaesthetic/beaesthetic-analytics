"""Agenda repository for MongoDB access with Arrow integration."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial

import polars as pl
import pyarrow as pa
from pymongoarrow.api import find_arrow_all
from pymongo import MongoClient

from analytics.config import settings

# Thread pool for sync MongoDB operations
_executor = ThreadPoolExecutor(max_workers=4)

# Arrow schema for appointments - defines columns to extract
APPOINTMENT_SCHEMA = pa.schema([
    ("_id", pa.string()),
    ("start", pa.timestamp("ms")),
    ("end", pa.timestamp("ms")),
    ("isCancelled", pa.bool_()),
    ("createdAt", pa.timestamp("ms")),
])


class AgendaRepository:
    """Repository for accessing agenda collection in MongoDB.

    Uses PyMongoArrow for efficient MongoDB → Arrow → Polars conversion
    without loading all documents into Python lists.
    """

    def __init__(self, client: MongoClient) -> None:
        self._collection = client[settings.mongodb_database]["agenda"]

    def _find_arrow_sync(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> pa.Table:
        """Synchronous Arrow query - runs in thread pool."""
        query = {
            "createdAt": {"$gte": start_date, "$lt": end_date},
            "data.type": "appointment",
        }
        return find_arrow_all(
            self._collection,
            query,
            schema=APPOINTMENT_SCHEMA,
        )

    async def find_as_polars(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> pl.DataFrame:
        """Find appointments and return as Polars DataFrame.

        Uses PyMongoArrow for efficient zero-copy conversion:
        MongoDB cursor → Arrow Table → Polars DataFrame
        """
        loop = asyncio.get_event_loop()
        arrow_table = await loop.run_in_executor(
            _executor,
            partial(self._find_arrow_sync, start_date, end_date),
        )
        return pl.from_arrow(arrow_table)

    async def find_as_lazy(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> pl.LazyFrame:
        """Find appointments and return as Polars LazyFrame for deferred execution."""
        df = await self.find_as_polars(start_date, end_date)
        return df.lazy()


def get_mongo_client() -> MongoClient:
    """Get synchronous MongoDB client for PyMongoArrow."""
    return MongoClient(settings.mongodb_uri)
