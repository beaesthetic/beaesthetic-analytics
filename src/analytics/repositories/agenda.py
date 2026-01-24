"""Agenda repository for MongoDB access."""

from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from analytics.config import settings


class AgendaRepository:
    """Repository for accessing agenda collection in MongoDB.

    Only fetches raw data - all aggregations are done with Polars.
    """

    def __init__(self, db: AsyncIOMotorDatabase) -> None:
        self._collection = db["agenda"]

    async def find_by_created_at_range(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict]:
        """Find all appointments created within a date range."""
        query = {
            "createdAt": {
                "$gte": start_date,
                "$lt": end_date,
            },
            "data.type": "appointment",
        }
        cursor = self._collection.find(query)
        return await cursor.to_list(length=None)


def get_database() -> AsyncIOMotorDatabase:
    """Get MongoDB database instance."""
    client: AsyncIOMotorClient = AsyncIOMotorClient(settings.mongodb_uri)
    return client[settings.mongodb_database]
