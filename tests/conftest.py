import asyncio
from datetime import date, timedelta

import pytest
from sqlalchemy import Boolean, Column, Date, DateTime, ForeignKey, Integer, JSON, MetaData, Numeric, String, Table, Text, create_engine
from sqlalchemy.orm import Session

metadata = MetaData()

brands = Table(
    "brands",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False),
    Column("domain", Text, nullable=False, unique=True),
    Column("category", Text, nullable=False),
    Column("facebook_page_id", Text),
)

products = Table(
    "products",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("brand_id", Integer, ForeignKey("brands.id")),
    Column("handle", Text),
    Column("title", Text),
    Column("url", Text),
)

variants = Table(
    "variants",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("product_id", Integer, ForeignKey("products.id")),
    Column("sku", Text),
    Column("options", JSON),
)

prices = Table(
    "prices",
    metadata,
    Column("variant_id", Integer, ForeignKey("variants.id"), primary_key=True),
    Column("ts_date", Date, primary_key=True),
    Column("currency", Text, nullable=False),
    Column("price_cents", Integer),
    Column("compare_at_cents", Integer),
    Column("available", Boolean),
)

ads_daily = Table(
    "ads_daily",
    metadata,
    Column("brand_id", Integer, ForeignKey("brands.id"), primary_key=True),
    Column("ts_date", Date, primary_key=True),
    Column("active_ads", Integer),
    Column("new_ads_24h", Integer),
)

leaders = Table(
    "leaders",
    metadata,
    Column("ts_date", Date, primary_key=True),
    Column("brand_id", Integer, ForeignKey("brands.id"), primary_key=True),
    Column("score", Numeric),
    Column("rank", Integer),
)

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("email", Text, unique=True, nullable=False),
    Column("tier", Text, nullable=False),
    Column("verified", Boolean, default=False),
    Column("unsubscribed", Boolean, default=False),
)

sends = Table(
    "sends",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ts", DateTime, nullable=False),
    Column("kind", Text),
    Column("user_id", Integer, ForeignKey("users.id")),
    Column("status", Text),
)


@pytest.fixture()
def engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture()
def session(engine):
    with Session(engine) as session:
        yield session


@pytest.fixture()
def seeded_engine(engine):
    with engine.begin() as conn:
        conn.execute(brands.insert(), [
            {"name": "HexCo", "domain": "https://hexco.com", "category": "skincare", "facebook_page_id": "123"},
            {"name": "Lumi Threads", "domain": "https://lumithreads.com", "category": "apparel", "facebook_page_id": "234"},
        ])
        conn.execute(users.insert(), [
            {"email": "pro@example.com", "tier": "pro", "verified": True, "unsubscribed": False},
        ])
        conn.execute(products.insert(), [
            {"brand_id": 1, "handle": "alpha-serum", "title": "Alpha Serum", "url": "https://hexco.com/products/alpha-serum"},
        ])
        conn.execute(variants.insert(), [
            {"product_id": 1, "sku": "ALPHA-1", "options": {}},
        ])
        today = date.today()
        conn.execute(prices.insert(), [
            {"variant_id": 1, "ts_date": today - timedelta(days=7), "currency": "USD", "price_cents": 4900, "compare_at_cents": 5900, "available": True},
            {"variant_id": 1, "ts_date": today, "currency": "USD", "price_cents": 3900, "compare_at_cents": 4900, "available": True},
        ])
        for idx in range(7):
            conn.execute(ads_daily.insert(), {
                "brand_id": 1,
                "ts_date": today - timedelta(days=6 - idx),
                "active_ads": 5 + idx,
                "new_ads_24h": 1,
            })
    return engine
