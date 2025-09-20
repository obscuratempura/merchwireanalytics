"""FastAPI application for subscriptions and archive search."""

from __future__ import annotations

import logging
import os
from datetime import date
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.db.session import create_engine_from_env
from app.email.render import render_email
from app.utils.esp import EmailMessage, EmailProvider
from app.utils.urls import generate_token, load_token, sign_path

logger = logging.getLogger(__name__)

app = FastAPI(title="Merchwire Brief API")


class SubscribeRequest(BaseModel):
    email: EmailStr
    tier: str


class SubscribeResponse(BaseModel):
    message: str


class StripeEvent(BaseModel):
    type: str
    data: dict[str, Any] | None = None


class ESPEvent(BaseModel):
    email: EmailStr


class ArchiveResponse(BaseModel):
    rows: list[dict[str, Any]]
    csv_url: str


def get_engine() -> Engine:
    return create_engine_from_env()


@app.post("/subscribe", response_model=SubscribeResponse)
async def subscribe(payload: SubscribeRequest, engine: Engine = Depends(get_engine)) -> SubscribeResponse:
    if payload.tier not in {"free", "daily", "pro"}:
        raise HTTPException(status_code=400, detail="Invalid tier")
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO users (email, tier)
                VALUES (:email, :tier)
                ON CONFLICT (email) DO UPDATE SET tier = EXCLUDED.tier
                RETURNING id, verified
                """
            ),
            {"email": payload.email.lower(), "tier": payload.tier},
        )
        user_id, verified = result.one()
    token = generate_token({"email": payload.email}, "verify")
    verify_link = f"https://merchwire.com/verify?token={token}"
    subject, html = render_email(
        "verify",
        {
            "subject": "Verify your Merchwire Brief subscription",
            "intro": "Confirm your email to start receiving updates.",
            "movers": [],
            "ads": [],
            "chart_url": None,
            "csv_url": None,
            "archive_url": verify_link,
            "upgrade_url": "https://merchwire.com/brief",
            "unsubscribe_url": sign_path("/unsubscribe"),
        },
    )
    provider = EmailProvider()
    await provider.send(EmailMessage(to=payload.email, subject=subject, html=html))
    return SubscribeResponse(message="Verification email sent")


@app.get("/verify")
async def verify(token: str = Query(...), engine: Engine = Depends(get_engine)) -> JSONResponse:
    try:
        data = load_token(token, "verify")
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid token") from exc
    email = data.get("email")
    if not email:
        raise HTTPException(status_code=400, detail="Invalid token")
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE users SET verified = TRUE WHERE email = :email"),
            {"email": email},
        )
    return JSONResponse({"message": "Verified"})


@app.post("/webhooks/stripe")
async def stripe_webhook(event: StripeEvent, engine: Engine = Depends(get_engine)) -> JSONResponse:
    tier_map = {
        "subscription_created": "daily",
        "subscription_updated": "pro",
        "subscription_canceled": "free",
    }
    tier = tier_map.get(event.type)
    if not tier:
        return JSONResponse({"status": "ignored"})
    data = event.data or {}
    email = (data.get("object") or {}).get("customer_email")
    if not email:
        raise HTTPException(status_code=400, detail="Missing email")
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE users SET tier = :tier WHERE email = :email"),
            {"tier": tier, "email": email},
        )
    return JSONResponse({"status": "ok"})


@app.post("/webhooks/esp/unsubscribe")
async def esp_unsubscribe(event: ESPEvent, engine: Engine = Depends(get_engine)) -> JSONResponse:
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE users SET unsubscribed = TRUE WHERE email = :email"),
            {"email": event.email},
        )
    return JSONResponse({"status": "ok"})


@app.get("/archive/search", response_model=ArchiveResponse)
async def archive_search(
    user_email: EmailStr,
    brand: str | None = None,
    product: str | None = None,
    start: date | None = None,
    end: date | None = None,
    engine: Engine = Depends(get_engine),
) -> ArchiveResponse:
    with engine.connect() as conn:
        user = conn.execute(
            text(
                "SELECT tier, verified, unsubscribed FROM users WHERE email = :email"
            ),
            {"email": user_email},
        ).mappings().first()
    if not user or user["tier"] != "pro" or not user["verified"] or user["unsubscribed"]:
        raise HTTPException(status_code=403, detail="Not authorized")

    query = """
        SELECT pr.ts_date, b.name AS brand, p.title, v.sku, pr.price_cents, pr.compare_at_cents
        FROM prices pr
        JOIN variants v ON v.id = pr.variant_id
        JOIN products p ON p.id = v.product_id
        JOIN brands b ON b.id = p.brand_id
        WHERE 1=1
    """
    params: dict[str, Any] = {}
    if brand:
        query += " AND b.name ILIKE :brand"
        params["brand"] = f"%{brand}%"
    if product:
        query += " AND p.title ILIKE :product"
        params["product"] = f"%{product}%"
    if start:
        query += " AND pr.ts_date >= :start"
        params["start"] = start
    if end:
        query += " AND pr.ts_date <= :end"
        params["end"] = end
    query += " ORDER BY pr.ts_date DESC LIMIT 200"

    with engine.connect() as conn:
        rows = [dict(row) for row in conn.execute(text(query), params)]
    csv_url = sign_path("/downloads/archive-latest.csv")
    return ArchiveResponse(rows=rows, csv_url=csv_url)
