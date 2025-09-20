"""Daily digest computation."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

from sqlalchemy.engine import Engine
from sqlalchemy.sql import text

from sqlalchemy.sql import text

from app.logic.ranking import BrandSignal, LeaderboardEntry, MoverEntry, rank_brands, top_sku_movers
from app.logic.signals import ad_surge, discount_percentage, discount_spike, is_mover, percent_change


@dataclass(slots=True)
class DailyMover:
    brand_id: int
    brand: str
    product: str
    sku: str | None
    url: str
    new_price: int | None
    old_price: int | None
    delta_pct_7d: float | None
    discount_pct: float


@dataclass(slots=True)
class DailyAd:
    brand: str
    active_ads: int
    new_ads_24h: int
    surge: bool


@dataclass(slots=True)
class DailyDigest:
    movers: list[DailyMover]
    leaderboard: list[LeaderboardEntry]
    ads: list[DailyAd]


def compute_daily_digest(engine: Engine, as_of: date) -> DailyDigest:
    rows = _load_price_rows(engine, as_of)
    ad_activity = _load_ad_activity(engine, as_of)
    movers: list[DailyMover] = []
    discount_counts: dict[int, int] = defaultdict(int)
    ad_signals: dict[int, DailyAd] = {}

    for row in rows:
        discount_pct = discount_percentage(row["price_cents"], row["compare_at_cents"])
        if discount_pct > 0:
            discount_counts[row["brand_id"]] += 1
        delta_7d = percent_change(row["price_cents"], row["price_7d"])
        mover_flag = is_mover(delta_7d)
        if mover_flag:
            movers.append(
                DailyMover(
                    brand_id=row["brand_id"],
                    brand=row["brand"],
                    product=row["title"],
                    sku=row["sku"],
                    url=row["url"],
                    new_price=row["price_cents"],
                    old_price=row["price_7d"],
                    delta_pct_7d=delta_7d,
                    discount_pct=discount_pct,
                )
            )
        ad_info = ad_activity.get(row["brand_id"])
        if ad_info and row["brand_id"] not in ad_signals:
            ad_signals[row["brand_id"]] = DailyAd(
                brand=row["brand"],
                active_ads=ad_info["today"],
                new_ads_24h=ad_info["new_ads"],
                surge=ad_info["surge"],
            )

    brand_signals: list[BrandSignal] = []
    for brand_id, count in discount_counts.items():
        ad_info = ad_activity.get(brand_id, {"surge": False, "today": 0, "new_ads": 0})
        row_example = next((row for row in rows if row["brand_id"] == brand_id), None)
        if not row_example:
            continue
        delta = percent_change(row_example["price_cents"], row_example["price_7d"])
        brand_signals.append(
            BrandSignal(
                brand_id=brand_id,
                brand_name=row_example["brand"],
                delta_pct_7d=delta or 0.0,
                discounted_sku_count=count,
                ad_surge=float(ad_info["surge"]),
            )
        )

    leaderboard = rank_brands(brand_signals)
    movers_top = [
        MoverEntry(
            brand_id=m.brand_id,
            brand_name=m.brand,
            product_title=m.product,
            sku=m.sku,
            new_price=m.new_price,
            old_price=m.old_price,
            delta_pct_7d=m.delta_pct_7d,
            discount_pct=m.discount_pct,
        )
        for m in movers
    ]

    top_movers = top_sku_movers(movers_top)

    notable_ads = [ad for ad in ad_signals.values() if ad.surge]
    notable_ads.sort(key=lambda ad: ad.active_ads, reverse=True)
    notable_ads = notable_ads[:3]

    return DailyDigest(movers=top_movers, leaderboard=leaderboard, ads=notable_ads)


def persist_leaderboard(engine: Engine, leaderboard: Iterable[LeaderboardEntry], as_of: date) -> None:
    with engine.begin() as conn:
        for entry in leaderboard:
            conn.execute(
                text(
                    """
                    INSERT INTO leaders (ts_date, brand_id, score, rank)
                    VALUES (:ts_date, :brand_id, :score, :rank)
                    ON CONFLICT (ts_date, brand_id) DO UPDATE SET
                      score = EXCLUDED.score,
                      rank = EXCLUDED.rank
                    """
                ),
                {
                    "ts_date": as_of,
                    "brand_id": entry.brand_id,
                    "score": entry.score,
                    "rank": entry.rank,
                },
            )


def _load_price_rows(engine: Engine, as_of: date) -> list[dict[str, object]]:
    query = text(
        """
        WITH base AS (
            SELECT pr.ts_date, b.id AS brand_id, b.name AS brand, b.category,
                   p.title, p.url, v.sku,
                   pr.price_cents, pr.compare_at_cents, pr.available,
                   LAG(pr.price_cents) OVER (PARTITION BY pr.variant_id ORDER BY pr.ts_date) AS price_1d,
                   LAG(pr.price_cents, 7) OVER (PARTITION BY pr.variant_id ORDER BY pr.ts_date) AS price_7d,
                   LAG(pr.compare_at_cents) OVER (PARTITION BY pr.variant_id ORDER BY pr.ts_date) AS compare_1d
            FROM prices pr
            JOIN variants v ON v.id = pr.variant_id
            JOIN products p ON p.id = v.product_id
            JOIN brands b ON b.id = p.brand_id
            WHERE pr.ts_date <= :date
        )
        SELECT * FROM base WHERE ts_date = :date
        """
    )
    with engine.connect() as conn:
        result = conn.execute(query, {"date": as_of})
        return [dict(row) for row in result]


def _load_ad_activity(engine: Engine, as_of: date) -> dict[int, dict[str, int | bool]]:
    start = as_of - timedelta(days=6)
    query = text(
        """
        SELECT brand_id, ts_date, active_ads, new_ads_24h
        FROM ads_daily
        WHERE ts_date BETWEEN :start AND :end
        ORDER BY brand_id, ts_date
        """
    )
    activity: dict[int, dict[str, int | bool]] = {}
    with engine.connect() as conn:
        result = conn.execute(query, {"start": start, "end": as_of})
        rows = result.fetchall()
    by_brand: dict[int, list[tuple[date, int]]] = defaultdict(list)
    for brand_id, ts_date, active_ads, new_ads in rows:
        by_brand[brand_id].append((ts_date, active_ads))
        activity[brand_id] = {"new_ads": new_ads, "today": active_ads, "surge": False}
    for brand_id, entries in by_brand.items():
        today_active = entries[-1][1]
        trailing = [value for _, value in entries[:-1]]
        surge = ad_surge(today_active, trailing)
        activity[brand_id]["surge"] = surge
    return activity
