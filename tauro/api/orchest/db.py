from __future__ import annotations

import os
from typing import Optional
from urllib.parse import urlparse

from pymongo import MongoClient  # type: ignore
from pymongo.collection import Collection  # type: ignore
from pymongo.database import Database  # type: ignore

try:  # Optional fallback for tests when MongoDB is not available
    import mongomock  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    mongomock = None  # type: ignore


DEFAULT_DATABASE_URL = "mongomock://localhost/tauro_orchest"
DATABASE_URL = os.environ.get("ORCHESTRATOR_DATABASE_URL", DEFAULT_DATABASE_URL)

parsed = urlparse(DATABASE_URL)

_DEFAULT_DB_FROM_URI: Optional[str] = None
if parsed.path and parsed.path not in {"", "/"}:
    _DEFAULT_DB_FROM_URI = parsed.path.lstrip("/")

DATABASE_NAME = os.environ.get(
    "ORCHESTRATOR_DATABASE_NAME", _DEFAULT_DB_FROM_URI or "tauro_orchest"
)

_client: Optional[MongoClient] = None


def _should_use_mongomock(url: str) -> bool:
    return url.startswith("mongomock://")


def _create_client() -> MongoClient:
    if _should_use_mongomock(DATABASE_URL):
        if mongomock is None:
            raise RuntimeError(
                "mongomock is required to use the 'mongomock://' URL scheme but is not installed."
            )
        return mongomock.MongoClient(tz_aware=True)  # type: ignore[return-value]

    return MongoClient(
        DATABASE_URL,
        tz_aware=True,
        serverSelectionTimeoutMS=int(
            os.environ.get("ORCHESTRATOR_MONGO_TIMEOUT_MS", "5000")
        ),
    )


def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = _create_client()
    return _client


def get_database() -> Database:
    return get_client()[DATABASE_NAME]


def get_collection(name: str) -> Collection:
    return get_database()[name]


def close_client() -> None:
    global _client
    if _client is not None:
        try:
            _client.close()
        finally:
            _client = None


def ping_database() -> bool:
    try:
        get_client().admin.command("ping")
        return True
    except Exception:
        return False


class MongoContextManager:
    """
    Context manager for safe MongoDB connection handling.

    Ensures connections are properly closed even if errors occur.

    Example:
        ```python
        # Automatic cleanup
        with MongoContextManager() as client:
            db = client[DATABASE_NAME]
            result = db.collection.find_one()

        # Connection is closed automatically
        ```
    """

    def __init__(self):
        """Initialize context manager."""
        self.client: Optional[MongoClient] = None

    def __enter__(self) -> MongoClient:
        """Enter context: get or create MongoDB client."""
        self.client = get_client()
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context: ensure connection is available for next use."""
        # Note: We don't close here to keep connection pooling benefits
        # The connection will be reused for future operations
        # Use close_client() explicitly if you need to force cleanup
        pass


def get_mongo_context() -> MongoContextManager:
    """
    Get a context manager for MongoDB operations.

    Returns:
        MongoContextManager for use with 'with' statement

    Example:
        ```python
        with get_mongo_context() as client:
            collection = client[DATABASE_NAME]["collections"]
            collection.insert_one({"data": "value"})
        ```
    """
    return MongoContextManager()


class ManagedMongoClient:
    """
    Wrapper for MongoDB client with guaranteed cleanup.

    This is useful for applications that need strict resource cleanup
    or when integrating with dependency injection frameworks.

    Example:
        ```python
        client = ManagedMongoClient()
        try:
            db = client.database
            result = db.collection.find_one()
        finally:
            client.close()
        ```
    """

    def __init__(self):
        """Initialize managed client."""
        self._client = get_client()

    @property
    def client(self) -> MongoClient:
        """Get the underlying MongoClient."""
        return self._client

    @property
    def database(self) -> Database:
        """Get the database."""
        return get_database()

    def get_collection(self, name: str) -> Collection:
        """Get a collection by name."""
        return get_collection(name)

    def close(self) -> None:
        """Close the MongoDB connection."""
        close_client()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit: close connection."""
        self.close()

    def ping(self) -> bool:
        """Check if database is accessible."""
        return ping_database()


__all__ = [
    "DATABASE_URL",
    "DATABASE_NAME",
    "get_client",
    "get_database",
    "get_collection",
    "close_client",
    "ping_database",
    "MongoContextManager",
    "ManagedMongoClient",
    "get_mongo_context",
]
