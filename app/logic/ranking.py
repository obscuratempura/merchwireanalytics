"""Ranking logic for brands and SKUs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from app.logic.signals import normalized


@dataclass(slots=True)
class BrandSignal:
    brand_id: int
    brand_name: str
    delta_pct_7d: float
    discounted_sku_count: int
    ad_surge: float


@dataclass(slots=True)
class LeaderboardEntry:
    brand_id: int
    brand_name: str
    score: float
    rank: int


@dataclass(slots=True)
class MoverEntry:
    brand_id: int
    brand_name: str
    product_title: str
    sku: str | None
    new_price: int | None
    old_price: int | None
    delta_pct_7d: float | None
    discount_pct: float


WEIGHTS = {
    "delta": 0.5,
    "discount": 0.3,
    "ads": 0.2,
}


def rank_brands(signals: Sequence[BrandSignal]) -> list[LeaderboardEntry]:
    if not signals:
        return []
    delta_norm = normalized([abs(s.delta_pct_7d) for s in signals])
    discount_norm = normalized([s.discounted_sku_count for s in signals])
    ads_norm = normalized([s.ad_surge for s in signals])
    scores = []
    for idx, sig in enumerate(signals):
        score = (
            WEIGHTS["delta"] * delta_norm[idx]
            + WEIGHTS["discount"] * discount_norm[idx]
            + WEIGHTS["ads"] * ads_norm[idx]
        )
        scores.append((sig, score))
    sorted_scores = sorted(scores, key=lambda item: item[1], reverse=True)
    leaderboard: list[LeaderboardEntry] = []
    for rank, (sig, score) in enumerate(sorted_scores, start=1):
        leaderboard.append(
            LeaderboardEntry(
                brand_id=sig.brand_id,
                brand_name=sig.brand_name,
                score=round(score, 4),
                rank=rank,
            )
        )
        if rank == 10:
            break
    return leaderboard


def top_sku_movers(movers: Sequence[MoverEntry]) -> list[MoverEntry]:
    filtered = [m for m in movers if m.delta_pct_7d is not None]
    filtered.sort(key=lambda m: abs(m.delta_pct_7d or 0), reverse=True)
    return filtered[:10]
