"""Seed database with brands and demo users."""

from __future__ import annotations

import os

from sqlalchemy import text

from app.db.session import create_engine_from_env
from app.ingest import load_brands


DEMO_USERS = [
    {"email": "free@example.com", "tier": "free", "verified": True},
    {"email": "daily@example.com", "tier": "daily", "verified": True},
    {"email": "pro@example.com", "tier": "pro", "verified": True},
]


def main() -> None:
    engine = create_engine_from_env()
    brands = load_brands(limit=2)
    with engine.begin() as conn:
        for brand in brands:
            conn.execute(
                text(
                    """
                    INSERT INTO brands (name, domain, category, facebook_page_id)
                    VALUES (:name, :domain, :category, :facebook_page_id)
                    ON CONFLICT (domain) DO NOTHING
                    """
                ),
                {
                    "name": brand.name,
                    "domain": brand.domain,
                    "category": brand.category,
                    "facebook_page_id": brand.facebook_page_id,
                },
            )
        for user in DEMO_USERS:
            conn.execute(
                text(
                    """
                    INSERT INTO users (email, tier, verified)
                    VALUES (:email, :tier, :verified)
                    ON CONFLICT (email) DO UPDATE SET tier = EXCLUDED.tier, verified = EXCLUDED.verified
                    """
                ),
                user,
            )
    print("Seed complete")


if __name__ == "__main__":
    main()
