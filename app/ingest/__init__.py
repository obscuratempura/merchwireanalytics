"""Ingestion helpers."""

from __future__ import annotations

import pathlib
from typing import Iterable

import yaml

from app.ingest.models import Brand

BRANDS_PATH = pathlib.Path(__file__).with_name("brands.yml")


def load_brands(limit: int | None = None) -> list[Brand]:
    data = yaml.safe_load(BRANDS_PATH.read_text())
    brands = [Brand(**item) for item in data]
    if limit:
        return brands[:limit]
    return brands
