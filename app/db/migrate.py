"""Database migration helpers."""

from __future__ import annotations

import pathlib
import sys
from typing import Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.db.session import create_engine_from_env

SCHEMA_PATH = pathlib.Path(__file__).with_name("schema.sql")


def run_migrations(engine: Engine) -> None:
    """Apply schema.sql to the database."""
    statements = _load_statements(SCHEMA_PATH.read_text())
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


def _load_statements(sql: str) -> Iterable[str]:
    buffer: list[str] = []
    for line in sql.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            yield "\n".join(buffer)
            buffer.clear()
    if buffer:
        yield "\n".join(buffer)


def main() -> None:
    try:
        engine = create_engine_from_env()
    except KeyError as exc:  # pragma: no cover - env failure is user error
        print(f"Missing environment variable: {exc}", file=sys.stderr)
        sys.exit(1)
    try:
        run_migrations(engine)
    except SQLAlchemyError as exc:
        print(f"Migration failed: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
