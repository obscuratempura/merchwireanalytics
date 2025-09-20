"""Signed URL utilities."""

from __future__ import annotations

import os
from datetime import timedelta

from itsdangerous import URLSafeTimedSerializer

DEFAULT_EXPIRY = int(os.environ.get("SIGNED_URL_EXPIRY", 60 * 60 * 48))


def _serializer() -> URLSafeTimedSerializer:
    secret = os.environ.get("SIGNING_SECRET", "change-me")
    return URLSafeTimedSerializer(secret_key=secret)


def sign_path(path: str, *, expires_in: int = DEFAULT_EXPIRY) -> str:
    serializer = _serializer()
    token = serializer.dumps(path)
    return f"{path}?token={token}&expires={expires_in}"


def verify_token(token: str, max_age: int = DEFAULT_EXPIRY) -> str:
    serializer = _serializer()
    return serializer.loads(token, max_age=max_age)


def generate_token(payload: dict[str, object], purpose: str) -> str:
    serializer = _serializer()
    return serializer.dumps(payload, salt=purpose)


def load_token(token: str, purpose: str, max_age: int = DEFAULT_EXPIRY) -> dict[str, object]:
    serializer = _serializer()
    data = serializer.loads(token, max_age=max_age, salt=purpose)
    if not isinstance(data, dict):  # pragma: no cover - defensive
        raise TypeError("Invalid token payload")
    return data
