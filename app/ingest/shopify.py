"""Shopify ingestion pipeline."""

from __future__ import annotations

import asyncio
import json
import logging
import pathlib
import random
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx
from sqlalchemy import insert, select
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text

from app.ingest.models import Brand
from app.utils.dates import today_in_tz
from app.utils.rate_limit import RateLimiter
from app.utils.retry import retry_async

logger = logging.getLogger(__name__)


PRODUCT_HANDLE_RE = re.compile(r"/products/([^/?#]+)")
CACHE_PATH = pathlib.Path(".cache/etags.json")


@dataclass(slots=True)
class ShopifyProduct:
    handle: str
    title: str
    url: str
    variants: list[dict[str, Any]]


class ETagCache:
    def __init__(self, path: pathlib.Path = CACHE_PATH):
        self.path = path
        self._data: dict[str, str] = {}
        if path.exists():
            try:
                self._data = json.loads(path.read_text())
            except json.JSONDecodeError:
                logger.warning("Invalid ETag cache; resetting")
                self._data = {}

    def get(self, url: str) -> str | None:
        return self._data.get(url)

    def set(self, url: str, etag: str | None) -> None:
        if not etag:
            self._data.pop(url, None)
            return
        self._data[url] = etag
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._data))


class ShopifyClient:
    def __init__(
        self,
        *,
        concurrency: int = 5,
        session: httpx.AsyncClient | None = None,
        rate_limiter: RateLimiter | None = None,
        etag_cache: ETagCache | None = None,
    ) -> None:
        self._session = session or httpx.AsyncClient(timeout=30.0, headers={"User-Agent": "MerchwireBot/1.0"})
        self._semaphore = asyncio.Semaphore(concurrency)
        self._robot_cache: dict[str, RobotFileParser] = {}
        self._rate_limiter = rate_limiter or RateLimiter(rate=1.5)
        self._etag_cache = etag_cache or ETagCache()

    async def close(self) -> None:
        await self._session.aclose()

    async def fetch_products(self, brand: Brand) -> list[ShopifyProduct]:
        base_url = brand.domain.rstrip("/")
        logger.info("Fetching products for %s", brand.name)
        handles = await self._discover_product_handles(base_url)
        products = await asyncio.gather(*(self._fetch_product(base_url, handle) for handle in handles))
        return [p for p in products if p]

    async def _discover_product_handles(self, base_url: str) -> list[str]:
        sitemap_url = f"{base_url}/sitemap_products_1.xml"
        try:
            text = await self._get_text(sitemap_url)
        except httpx.HTTPError:
            logger.info("Sitemap fetch failed for %s, falling back to collections", base_url)
            return await self._discover_via_collections(base_url)
        handles = {match.group(1) for match in PRODUCT_HANDLE_RE.finditer(text)}
        return sorted(handles)

    async def _discover_via_collections(self, base_url: str) -> list[str]:
        handles: set[str] = set()
        for page in range(1, 11):
            page_url = f"{base_url}/collections/all?page={page}"
            try:
                text = await self._get_text(page_url)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    break
                raise
            matches = PRODUCT_HANDLE_RE.findall(text)
            handles.update(matches)
            if "Next" not in text:
                break
        return sorted(handles)

    async def _fetch_product(self, base_url: str, handle: str) -> ShopifyProduct | None:
        product_url = f"{base_url}/products/{handle}.js"
        try:
            data = await self._get_json(product_url)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 304:
                logger.info("Product %s not modified", product_url)
                return None
            if exc.response.status_code in {403, 404}:
                logger.warning("Product %s unavailable (%s)", product_url, exc.response.status_code)
                return None
            raise
        variants = data.get("variants", [])
        return ShopifyProduct(
            handle=handle,
            title=data.get("title", handle.replace("-", " ").title()),
            url=f"{base_url}/products/{handle}",
            variants=variants,
        )

    async def _respect_robots(self, url: str) -> None:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        parser = self._robot_cache.get(base)
        if parser is None:
            robots_url = f"{base}/robots.txt"
            parser = RobotFileParser()
            try:
                text = await self._fetch_robots(robots_url)
            except httpx.HTTPError:
                parser.parse("User-agent: *\nAllow: /".splitlines())
            else:
                parser.parse(text.splitlines())
            self._robot_cache[base] = parser
        if not parser.can_fetch("MerchwireBot", parsed.path):
            raise PermissionError(f"Blocked by robots.txt: {url}")

    async def _fetch_robots(self, url: str) -> str:
        async with self._semaphore:
            response = await self._session.get(url)
        response.raise_for_status()
        return response.text

    async def _get_text(self, url: str, *, allow_rate_limit: bool = True) -> str:
        response = await self._request("GET", url, allow_rate_limit=allow_rate_limit)
        response.raise_for_status()
        return response.text

    async def _get_json(self, url: str) -> dict[str, Any]:
        response = await self._request("GET", url)
        response.raise_for_status()
        return response.json()

    async def _request(self, method: str, url: str, *, allow_rate_limit: bool = True) -> httpx.Response:
        await self._respect_robots(url)
        parsed = urlparse(url)
        headers = {}
        etag = self._etag_cache.get(url)
        if etag:
            headers["If-None-Match"] = etag
        async with self._semaphore:
            if allow_rate_limit:
                await self._rate_limiter.wait_for_host(parsed.netloc)
            response = await retry_async(self._session.request)(method, url, headers=headers)
        if response.status_code == 304:
            raise httpx.HTTPStatusError("Not modified", request=response.request, response=response)
        new_etag = response.headers.get("ETag")
        if new_etag:
            self._etag_cache.set(url, new_etag)
        return response


class ShopifyIngestor:
    def __init__(self, engine: Engine, client: ShopifyClient | None = None) -> None:
        self.engine = engine
        self.client = client or ShopifyClient()

    async def ingest(self, brand: Brand, *, as_of: date | None = None) -> None:
        records = await self.client.fetch_products(brand)
        ts_date = as_of or today_in_tz()
        logger.info("Persisting %s products for %s", len(records), brand.name)
        async with asyncio.Semaphore(1):
            await asyncio.get_running_loop().run_in_executor(
                None, self._persist_records, brand, records, ts_date
            )

    def _persist_records(self, brand: Brand, products: list[ShopifyProduct], ts_date: date) -> None:
        with self.engine.begin() as conn:
            brand_id = self._ensure_brand(conn, brand)
            for product in products:
                product_id = self._ensure_product(conn, brand_id, product)
                for variant in product.variants:
                    variant_id = self._ensure_variant(conn, product_id, variant)
                    price_cents = _price_to_cents(variant.get("price"))
                    compare_cents = _price_to_cents(variant.get("compare_at_price"))
                    currency = variant.get("currency") or variant.get("currency_code") or "USD"
                    available = bool(variant.get("available", True))
                    conn.execute(
                        text(
                            """
                            INSERT INTO prices(variant_id, ts_date, currency, price_cents, compare_at_cents, available)
                            VALUES (:variant_id, :ts_date, :currency, :price_cents, :compare_at_cents, :available)
                            ON CONFLICT (variant_id, ts_date) DO UPDATE SET
                              price_cents = EXCLUDED.price_cents,
                              compare_at_cents = EXCLUDED.compare_at_cents,
                              available = EXCLUDED.available
                            """
                        ),
                        {
                            "variant_id": variant_id,
                            "ts_date": ts_date,
                            "currency": currency,
                            "price_cents": price_cents,
                            "compare_at_cents": compare_cents,
                            "available": available,
                        },
                    )

    def _ensure_brand(self, conn, brand: Brand) -> int:
        existing = conn.execute(
            text("SELECT id FROM brands WHERE domain = :domain"), {"domain": brand.domain}
        ).scalar_one_or_none()
        if existing:
            return existing
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
        return int(result.scalar_one())

    def _ensure_product(self, conn, brand_id: int, product: ShopifyProduct) -> int:
        existing = conn.execute(
            text("SELECT id FROM products WHERE brand_id = :brand_id AND handle = :handle"),
            {"brand_id": brand_id, "handle": product.handle},
        ).scalar_one_or_none()
        if existing:
            conn.execute(
                text("UPDATE products SET title = :title, url = :url WHERE id = :id"),
                {"title": product.title, "url": product.url, "id": existing},
            )
            return existing
        result = conn.execute(
            text(
                """
                INSERT INTO products (brand_id, handle, title, url)
                VALUES (:brand_id, :handle, :title, :url)
                RETURNING id
                """
            ),
            {
                "brand_id": brand_id,
                "handle": product.handle,
                "title": product.title,
                "url": product.url,
            },
        )
        return int(result.scalar_one())

    def _ensure_variant(self, conn, product_id: int, variant: dict[str, Any]) -> int:
        existing = conn.execute(
            text(
                "SELECT id FROM variants WHERE product_id = :product_id AND sku IS NOT DISTINCT FROM :sku"
            ),
            {"product_id": product_id, "sku": variant.get("sku")},
        ).scalar_one_or_none()
        options_data = json.dumps({k: v for k, v in variant.items() if k.startswith("option")})
        if existing:
            stmt = (
                "UPDATE variants SET options = :options WHERE id = :id"
                if conn.dialect.name == "sqlite"
                else "UPDATE variants SET options = CAST(:options AS JSONB) WHERE id = :id"
            )
            conn.execute(text(stmt), {"options": options_data, "id": existing})
            return existing
        insert_sql = (
            """
            INSERT INTO variants (product_id, sku, options)
            VALUES (:product_id, :sku, :options)
            RETURNING id
            """
            if conn.dialect.name == "sqlite"
            else """
            INSERT INTO variants (product_id, sku, options)
            VALUES (:product_id, :sku, CAST(:options AS JSONB))
            RETURNING id
            """
        )
        result = conn.execute(
            text(insert_sql),
            {
                "product_id": product_id,
                "sku": variant.get("sku"),
                "options": options_data,
            },
        )
        return int(result.scalar_one())


def _price_to_cents(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return round(float(value) * 100)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return None
