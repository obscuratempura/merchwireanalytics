"""Weekly summary job."""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from dotenv import load_dotenv

from app.db.session import create_engine_from_env
from app.email.render import render_email
from app.logic.daily import compute_daily_digest
from app.utils.dates import format_date, today_in_tz
from app.utils.esp import EmailMessage, EmailProvider
from app.utils.urls import sign_path

logger = logging.getLogger(__name__)


async def run_weekly(as_of: date | None = None) -> None:
    load_dotenv()
    engine = create_engine_from_env()
    target = as_of or today_in_tz()
    digest = compute_daily_digest(engine, target)
    top_movers = digest.movers[:3]
    chart_url = sign_path("/charts/weekly.png")

    subject = f"Weekly Shopify Pricing & Ads Recap — {format_date(target)}"
    context = {
        "subject": subject,
        "intro": "What moved on Shopify and Meta ads last week.",
        "movers": [
            {
                "brand": mover.brand_name,
                "product": mover.product_title,
                "sku": mover.sku,
                "new_price": mover.new_price,
                "old_price": mover.old_price,
                "delta_pct": f"{(mover.delta_pct_7d or 0)*100:.1f}%",
                "discount_pct": f"{mover.discount_pct*100:.1f}%",
            }
            for mover in top_movers
        ],
        "ads": [],
        "chart_url": chart_url,
        "csv_url": None,
        "archive_url": sign_path("/archive"),
        "upgrade_url": "https://merchwire.com/brief",
        "unsubscribe_url": sign_path("/unsubscribe"),
    }
    subject, html = render_email("weekly", context)
    users = _load_free_users(engine)
    provider = EmailProvider()
    for user in users:
        await provider.send(EmailMessage(to=user["email"], subject=subject, html=html))


def _load_free_users(engine):
    query = """
        SELECT id, email
        FROM users
        WHERE tier = 'free' AND verified = TRUE AND unsubscribed = FALSE
    """
    with engine.connect() as conn:
        result = conn.execute(query)
        return [dict(row) for row in result]


if __name__ == "__main__":
    asyncio.run(run_weekly())
