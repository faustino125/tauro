from __future__ import annotations

import os
from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session

# Default DB URL (local file) but allow override via env var
DATABASE_URL = os.environ.get("ORCHESTRATOR_DATABASE_URL", "sqlite:///orchest.db")

# SQLite requires a special connect arg to allow cross-thread use
connect_args = {}
if DATABASE_URL.startswith("sqlite:"):
    connect_args = {"check_same_thread": False}

# Create the engine and a scoped session factory
engine = create_engine(DATABASE_URL, connect_args=connect_args)
_SessionFactory = scoped_session(
    sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
)


def get_session() -> Session:
    """Return a new SQLAlchemy Session from the scoped factory.

    The caller is responsible for closing the session (the usual pattern
    is using a contextmanager in higher-level code).
    """
    return _SessionFactory()


def close_session(session: Optional[Session]) -> None:
    try:
        if session is not None:
            session.close()
    except Exception:
        # Best-effort close; don't propagate
        pass


__all__ = ["engine", "get_session", "close_session", "DATABASE_URL"]
