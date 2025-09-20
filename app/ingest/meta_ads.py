"""Meta Ads Library ingestion."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text

from app.ingest.models import Brand
from app.utils.dates import today_in_tz
from app.utils.retry import retry_async

logger = logging.getLogger(__name__)

META_ENDPOINT = "https://graph.facebook.com/v18.0/ads_archive"


@dataclass(slots=True)
class MetaAdSummary:
    active_ads: int
    new_ads_24h: int


class MetaAdsClient:
    def __init__(self, token: str, *, session: httpx.AsyncClient | None = None) -> None:
        self.token = token
        self.session = session or httpx.AsyncClient(timeout=30.0)

    async def close(self) -> None:
        await self.session.aclose()

    async def fetch_summary(self, page_id: str) -> MetaAdSummary:
        params = {
            "search_page_ids": page_id,
            "ad_type": "POLITICAL_AND_ISSUE_ADS",
            "access_token": self.token,
            "fields": "ad_creation_time",
            "limit": 500,
        }
        response = await retry_async(self.session.get)(META_ENDPOINT, params=params)
        response.raise_for_status()
        data = response.json()
        items = data.get("data", [])
        active_ads = len(items)
        new_ads = sum(1 for item in items if _is_recent(item.get("ad_creation_time")))
        return MetaAdSummary(active_ads=active_ads, new_ads_24h=new_ads)


def _is_recent(ts: str | None) -> bool:
    if not ts:
        return False
    try:
        created = date.fromisoformat(ts[:10])
    except ValueError:
        return False
    today = today_in_tz()
    return (today - created).days <= 1


class MetaAdsIngestor:
    def __init__(self, engine: Engine, client: MetaAdsClient) -> None:
        self.engine = engine
        self.client = client

    async def ingest(self, brand: Brand, *, as_of: date | None = None) -> None:
        if not brand.facebook_page_id:
            logger.info("Skipping %s: no facebook_page_id", brand.name)
            return
        summary = await self.client.fetch_summary(brand.facebook_page_id)
        ts_date = as_of or today_in_tz()
        await asyncio.get_running_loop().run_in_executor(
            None, self._persist, brand, summary, ts_date
        )

    def _persist(self, brand: Brand, summary: MetaAdSummary, ts_date: date) -> None:
        with self.engine.begin() as conn:
            brand_id = conn.execute(
                text("SELECT id FROM brands WHERE domain = :domain"),
                {"domain": brand.domain},
            ).scalar_one_or_none()
            if brand_id is None:
                result = conn.execute(
                    text(
                        """
                        INSERT INTO brands (name, domain, category, facebook_page_id)
                        VALUES (:name, :domain, :category, :facebook_page_id)
                        RETURNING id
                        """
                    ),
                    {
                        "name": brand.name,
                        "domain": brand.domain,
                        "category": brand.category,
                        "facebook_page_id": brand.facebook_page_id,
                    },
                )
                brand_id = int(result.scalar_one())
            conn.execute(
                text(
                    """
                    INSERT INTO ads_daily (brand_id, ts_date, active_ads, new_ads_24h)
                    VALUES (:brand_id, :ts_date, :active_ads, :new_ads)
                    ON CONFLICT (brand_id, ts_date) DO UPDATE SET
                      active_ads = EXCLUDED.active_ads,
                      new_ads_24h = EXCLUDED.new_ads_24h
                    """
                ),
                {
                    "brand_id": brand_id,
                    "ts_date": ts_date,
                    "active_ads": summary.active_ads,
                    "new_ads": summary.new_ads_24h,
                },
            )
