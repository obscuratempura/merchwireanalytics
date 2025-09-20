"""Ingestion data models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping


@dataclass(slots=True)
class Brand:
    name: str
    domain: str
    category: str
    facebook_page_id: str | None = None


@dataclass(slots=True)
class VariantRecord:
    brand_id: int
    product_id: int
    variant_id: int
    sku: str | None
    url: str
    title: str
    price_cents: int | None
    compare_at_cents: int | None
    currency: str
    available: bool
    ts_date: date
    metadata: Mapping[str, Any]


@dataclass(slots=True)
class AdSnapshot:
    brand_id: int
    ts_date: date
    active_ads: int
    new_ads_24h: int
