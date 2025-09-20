"""Database session helpers."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


DEFAULT_DATABASE_URL = "postgresql://user:pass@db:5432/brief"


def create_engine_from_env() -> Engine:
    """Create an engine using the DATABASE_URL environment variable."""
    url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    return create_engine(url, pool_pre_ping=True, future=True)


@contextmanager
def session_scope(engine: Engine):
    """Provide a transactional scope around a series of operations."""
    from sqlalchemy.orm import Session

    session = Session(engine, future=True)
    try:
        yield session
        session.commit()
    except Exception:  # pragma: no cover - small helper
        session.rollback()
        raise
    finally:
        session.close()
