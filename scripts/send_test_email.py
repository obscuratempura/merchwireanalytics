"""Send a test digest email."""

from __future__ import annotations

import asyncio
import os

from dotenv import load_dotenv

from app.db.session import create_engine_from_env
from app.logic.daily import compute_daily_digest
from app.email.render import render_email
from app.utils.dates import today_in_tz, format_date
from app.utils.esp import EmailMessage, EmailProvider
from app.utils.urls import sign_path


async def main() -> None:
    load_dotenv()
    engine = create_engine_from_env()
    digest = compute_daily_digest(engine, today_in_tz())
    subject = f"Daily Shopify Pricing & Ads Brief — {format_date(today_in_tz())}"
    context = {
        "subject": subject,
        "intro": "Test daily digest",
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
            for mover in digest.movers
        ],
        "ads": [],
        "chart_url": None,
        "csv_url": sign_path("/downloads/daily.csv"),
        "archive_url": sign_path("/archive"),
        "upgrade_url": "https://merchwire.com/brief",
        "unsubscribe_url": sign_path("/unsubscribe"),
    }
    subject, html = render_email("daily", context)
    recipient = os.environ.get("TEST_RECIPIENT")
    if not recipient:
        raise SystemExit("TEST_RECIPIENT env var required")
    provider = EmailProvider()
    await provider.send(EmailMessage(to=recipient, subject=subject, html=html))
    print("Sent test email to", recipient)


if __name__ == "__main__":
    asyncio.run(main())
