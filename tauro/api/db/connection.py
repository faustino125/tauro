"""
Copyright (c) 2025 Faustino Lopez Ramos.
For licensing information, see the LICENSE file in the project root
"""
from typing import AsyncGenerator, Optional
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure  # type: ignore
from loguru import logger
from contextlib import asynccontextmanager


class MongoDBClient:
    """Async MongoDB Client wrapper using Motor"""

    def __init__(
        self,
        uri: str = "mongodb://localhost:27017",
        db_name: str = "tauro",
        timeout_seconds: int = 30,
    ):
        """
        Initialize MongoDB client

        Args:
            uri: MongoDB connection string
            db_name: Database name
            timeout_seconds: Connection timeout in seconds
        """
        self.uri = uri
        self.db_name = db_name
        self.timeout_seconds = timeout_seconds
        self._client: Optional[AsyncIOMotorClient] = None
        self._db: Optional[AsyncIOMotorDatabase] = None

    async def connect(self) -> AsyncIOMotorDatabase:
        """
        Connect to MongoDB

        Returns:
            AsyncDatabase instance

        Raises:
            ConnectionFailure: If connection fails
        """
        if self._client is not None:
            logger.debug("Already connected to MongoDB")
            return self._db

        try:
            logger.info(f"Connecting to MongoDB: {self.uri}")

            self._client = AsyncIOMotorClient(
                self.uri,
                serverSelectionTimeoutMS=self.timeout_seconds * 1000,
                connectTimeoutMS=self.timeout_seconds * 1000,
            )

            # Test connection
            await self._client.admin.command("ping")
            logger.info("Successfully connected to MongoDB")

            self._db = self._client[self.db_name]
            logger.info(f"Using database: {self.db_name}")

            return self._db

        except (ServerSelectionTimeoutError, ConnectionFailure) as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self._client = None
            self._db = None
            raise ConnectionFailure(f"MongoDB connection failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during MongoDB connection: {e}")
            self._client = None
            self._db = None
            raise

    def disconnect(self) -> None:
        """Disconnect from MongoDB"""
        if self._client is not None:
            logger.info("Disconnecting from MongoDB")
            self._client.close()
            self._client = None
            self._db = None
            logger.info("Disconnected from MongoDB")

    async def check_connection(self) -> bool:
        """
        Check if connected to MongoDB

        Returns:
            True if connection is healthy, False otherwise
        """
        if self._client is None or self._db is None:
            return False

        try:
            await self._client.admin.command("ping")
            return True
        except Exception as e:
            logger.warning(f"Connection health check failed: {e}")
            return False

    def get_database(self) -> AsyncIOMotorDatabase:
        """
        Get the current database instance

        Returns:
            AsyncIOMotorDatabase instance

        Raises:
            RuntimeError: If not connected
        """
        if self._db is None:
            raise RuntimeError("Not connected to MongoDB. Call connect() first.")
        return self._db

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncIOMotorDatabase, None]:
        """
        Context manager for database operations

        Yields:
            AsyncIOMotorDatabase instance
        """
        db = self.get_database()
        try:
            yield db
        except Exception as e:
            logger.error(f"Error during database operation: {e}")
            raise


# Global instance
_mongodb_client: Optional[MongoDBClient] = None


def init_mongodb(
    uri: str = "mongodb://localhost:27017",
    db_name: str = "tauro",
    timeout_seconds: int = 30,
) -> MongoDBClient:
    """
    Initialize global MongoDB client

    Args:
        uri: MongoDB connection string
        db_name: Database name
        timeout_seconds: Connection timeout in seconds

    Returns:
        MongoDBClient instance
    """
    global _mongodb_client

    if _mongodb_client is None:
        _mongodb_client = MongoDBClient(uri, db_name, timeout_seconds)
        logger.info("MongoDB client initialized")

    return _mongodb_client


async def get_database() -> AsyncGenerator[AsyncIOMotorDatabase, None]:
    """
    FastAPI dependency for getting MongoDB database

    Yields:
        AsyncIOMotorDatabase instance
    """
    global _mongodb_client

    if _mongodb_client is None:
        raise RuntimeError("MongoDB client not initialized. Call init_mongodb() first.")

    db = _mongodb_client.get_database()
    try:
        yield db
    except Exception as e:
        logger.error(f"Error in database operation: {e}")
        raise


def get_mongodb_client() -> MongoDBClient:
    """
    Get the global MongoDB client instance

    Returns:
        MongoDBClient instance

    Raises:
        RuntimeError: If not initialized
    """
    global _mongodb_client

    if _mongodb_client is None:
        raise RuntimeError("MongoDB client not initialized. Call init_mongodb() first.")

    return _mongodb_client
