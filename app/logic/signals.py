"""Business logic for computing pricing and ads signals."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Sequence

import numpy as np

MOVER_THRESHOLD = float(os.environ.get("MOVER_THRESHOLD", 0.10))
DISCOUNT_SPIKE_THRESHOLD = float(os.environ.get("DISCOUNT_SPIKE_THRESHOLD", 0.10))
AD_SURGE_MULTIPLIER = float(os.environ.get("AD_SURGE_MULTIPLIER", 2.0))
AD_SURGE_MIN_DELTA = int(os.environ.get("AD_SURGE_MIN_DELTA", 5))


@dataclass(slots=True)
class PriceWindow:
    variant_id: int
    currency: str
    prices: Sequence[int | None]


def percent_change(new: int | None, old: int | None) -> float | None:
    if new is None or old in (None, 0):
        return None
    return (new - old) / max(old, 1)


def discount_percentage(price: int | None, compare_at: int | None) -> float:
    if not price or not compare_at or compare_at <= 0 or price >= compare_at:
        return 0.0
    return (compare_at - price) / compare_at


def is_mover(delta_pct_7d: float | None) -> bool:
    if delta_pct_7d is None:
        return False
    return abs(delta_pct_7d) >= MOVER_THRESHOLD


def discount_spike(previous: float, current: float) -> bool:
    return (current - previous) >= DISCOUNT_SPIKE_THRESHOLD


def ad_surge(active_today: int, trailing_values: Iterable[int]) -> bool:
    trailing = [v for v in trailing_values if v is not None]
    if not trailing:
        return False
    median = float(np.median(trailing))
    if median == 0:
        return active_today >= AD_SURGE_MIN_DELTA
    return active_today >= AD_SURGE_MULTIPLIER * median and (active_today - median) >= AD_SURGE_MIN_DELTA


def normalized(values: Sequence[float]) -> list[float]:
    if not values:
        return []
    arr = np.array(values, dtype=float)
    if np.allclose(arr, arr[0]):
        return [0.0 for _ in arr]
    min_v = arr.min()
    max_v = arr.max()
    if math.isclose(min_v, max_v):
        return [0.0 for _ in arr]
    return ((arr - min_v) / (max_v - min_v)).tolist()


def trailing_window(values: Sequence[int | None], days: int) -> Sequence[int | None]:
    if len(values) <= days:
        return values
    return values[-days:]
