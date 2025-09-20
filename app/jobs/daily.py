"""Daily job orchestration."""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime

from dotenv import load_dotenv
from sqlalchemy import text

from app.db.session import create_engine_from_env
from app.email.render import render_email
from app.ingest import load_brands
from app.ingest.meta_ads import MetaAdsClient, MetaAdsIngestor
from app.ingest.shopify import ShopifyIngestor
from app.logic.charts import ChartResult, category_discount_chart
from app.logic.daily import compute_daily_digest, persist_leaderboard
from app.logic.export_csv import generate_daily_csv
from app.utils.dates import format_date, today_in_tz
from app.utils.esp import EmailMessage, EmailProvider
from app.utils.urls import sign_path

logger = logging.getLogger(__name__)


async def run_daily(as_of: date | None = None) -> None:
    load_dotenv()
    engine = create_engine_from_env()
    brands = load_brands()
    target_date = as_of or today_in_tz()

    shopify = ShopifyIngestor(engine)
    meta_token = os.environ.get("META_ADS_TOKEN")
    meta_client = MetaAdsClient(meta_token) if meta_token else None
    meta_ingestor = MetaAdsIngestor(engine, meta_client) if meta_client else None

    try:
        for brand in brands:
            try:
                await shopify.ingest(brand, as_of=target_date)
            except PermissionError as exc:
                logger.warning("Skipping %s: %s", brand.domain, exc)
        if meta_ingestor:
            for brand in brands:
                try:
                    await meta_ingestor.ingest(brand, as_of=target_date)
                except Exception as exc:  # pragma: no cover - API errors
                    logger.warning("Meta API failed for %s: %s", brand.name, exc)
    finally:
        await shopify.client.close()
        if meta_client:
            await meta_client.close()

    digest = compute_daily_digest(engine, target_date)
    persist_leaderboard(engine, digest.leaderboard, target_date)

    charts = _generate_charts(engine)
    csv_path = generate_daily_csv(engine, target_date)
    csv_url = sign_path(f"/downloads/{csv_path.name}")

    await _send_digest(engine, digest, charts, csv_url, target_date)


def _generate_charts(engine) -> list[ChartResult]:
    charts: list[ChartResult] = []
    with engine.connect() as conn:
        categories = [row[0] for row in conn.execute("SELECT DISTINCT category FROM brands")] 
    for category in categories:
        try:
            charts.append(category_discount_chart(engine, category))
        except ValueError:
            continue
    return charts


async def _send_digest(engine, digest, charts: list[ChartResult], csv_url: str, as_of: date) -> None:
    provider = EmailProvider()
    chart_url = sign_path(f"/charts/{charts[0].path.name}") if charts else None
    users = _load_recipients(engine)
    subject = f"Daily Shopify Pricing & Ads Brief — {format_date(as_of)}"
    movers_payload = [
        {
            "brand": mover.brand_name,
            "product": mover.product_title,
            "sku": mover.sku,
            "new_price": _format_currency(mover.new_price),
            "old_price": _format_currency(mover.old_price),
            "delta_pct": f"{(mover.delta_pct_7d or 0)*100:.1f}%",
            "discount_pct": f"{mover.discount_pct*100:.1f}%",
        }
        for mover in digest.movers
    ]
    ads_payload = [
        {
            "brand": ad.brand,
            "summary": f"{ad.active_ads} active ads",
            "tagline": "Ad surge" if ad.surge else "Steady",
            "note": f"{ad.new_ads_24h} new in 24h",
        }
        for ad in digest.ads
    ]
    context = {
        "subject": subject,
        "intro": "Daily highlights from Shopify pricing and Meta ads movements.",
        "movers": movers_payload,
        "ads": ads_payload,
        "chart_url": chart_url,
        "csv_url": csv_url,
        "archive_url": sign_path("/archive"),
        "upgrade_url": "https://merchwire.com/brief",
        "unsubscribe_url": sign_path("/unsubscribe"),
    }
    subject, html = render_email("daily", context)
    for user in users:
        if user["tier"] not in {"daily", "pro"}:
            continue
        await provider.send(EmailMessage(to=user["email"], subject=subject, html=html))
        _record_send(engine, user["id"], kind="daily")


def _load_recipients(engine) -> list[dict[str, object]]:
    query = """
        SELECT id, email, tier
        FROM users
        WHERE verified = TRUE AND unsubscribed = FALSE
    """
    with engine.connect() as conn:
        result = conn.execute(query)
        return [dict(row) for row in result]


def _record_send(engine, user_id: int, kind: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO sends (ts, kind, user_id, status)
                VALUES (:ts, :kind, :user_id, 'sent')
                """
            ),
            {"ts": datetime.utcnow(), "kind": kind, "user_id": user_id},
        )


def _format_currency(cents: int | None) -> str:
    if cents is None:
        return "-"
    return f"${cents/100:.2f}"


if __name__ == "__main__":
    asyncio.run(run_daily())
