import asyncio
from datetime import date
from pathlib import Path

import httpx
import pytest
import respx
from sqlalchemy import text

from app.ingest.models import Brand
from app.ingest.shopify import ShopifyClient, ShopifyIngestor
from app.ingest.meta_ads import MetaAdsClient, MetaAdsIngestor

FIXTURES = Path(__file__).parent / "fixtures" / "http"


def load_fixture(path: str) -> str:
    return (FIXTURES / path).read_text()


@pytest.mark.asyncio
async def test_shopify_ingest(engine):
    sitemap = load_fixture("shopify/hexco_sitemap.xml")
    product = load_fixture("shopify/alpha-serum.js")
    async with respx.mock(assert_all_called=True) as router:
        router.get("https://hexco.com/robots.txt").mock(return_value=httpx.Response(200, text="User-agent: *\nAllow: /"))
        router.get("https://hexco.com/sitemap_products_1.xml").mock(return_value=httpx.Response(200, text=sitemap))
        router.get("https://hexco.com/products/alpha-serum.js").mock(return_value=httpx.Response(200, text=product))
        async with httpx.AsyncClient(transport=router.transport) as session:
            client = ShopifyClient(session=session)
            ingestor = ShopifyIngestor(engine, client=client)
            brand = Brand(name="HexCo", domain="https://hexco.com", category="skincare", facebook_page_id="123")
            await ingestor.ingest(brand, as_of=date.today())
    with engine.connect() as conn:
        price = conn.execute(text("SELECT price_cents FROM prices"))
        assert price.scalar() == 3900


@pytest.mark.asyncio
async def test_meta_ads_ingest(engine):
    response_json = load_fixture("meta/hexco.json")
    async with respx.mock(assert_all_called=True) as router:
        router.get("https://graph.facebook.com/robots.txt").mock(return_value=httpx.Response(200, text="User-agent: *\nAllow: /"))
        router.get("https://graph.facebook.com/v18.0/ads_archive").mock(return_value=httpx.Response(200, text=response_json))
        async with httpx.AsyncClient(transport=router.transport) as session:
            client = MetaAdsClient("token", session=session)
            ingestor = MetaAdsIngestor(engine, client)
            brand = Brand(name="HexCo", domain="https://hexco.com", category="skincare", facebook_page_id="123")
            await ingestor.ingest(brand, as_of=date.today())
    with engine.connect() as conn:
        row = conn.execute(text("SELECT active_ads, new_ads_24h FROM ads_daily"))
        active_ads, new_ads = row.fetchone()
        assert active_ads == 2
        assert new_ads >= 0
