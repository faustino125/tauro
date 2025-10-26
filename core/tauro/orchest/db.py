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


__all__ = [
    "DATABASE_URL",
    "DATABASE_NAME",
    "get_client",
    "get_database",
    "get_collection",
    "close_client",
    "ping_database",
]
