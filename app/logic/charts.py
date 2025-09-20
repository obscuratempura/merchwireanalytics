"""Chart generation utilities."""

from __future__ import annotations

import io
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text

from app.utils.dates import format_date

plt.switch_backend("Agg")

OUTPUT_DIR = Path(os.environ.get("CHART_OUTPUT_DIR", "artifacts/charts"))


@dataclass(slots=True)
class ChartResult:
    category: str
    path: Path


def category_discount_chart(engine: Engine, category: str, days: int = 7) -> ChartResult:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    frame = _load_category_frame(engine, category, days)
    if frame.empty:
        raise ValueError(f"No pricing data for category {category}")
    frame["ts_date"] = pd.to_datetime(frame["ts_date"]).dt.date
    frame["median_discount"] = frame["median_discount"].fillna(0)
    start_date = frame["ts_date"].min()
    end_date = frame["ts_date"].max()
    fig, ax1 = plt.subplots(figsize=(8, 4))
    frame_sorted = frame.sort_values("ts_date")
    ax1.plot(frame_sorted["ts_date"], frame_sorted["median_discount"], color="#2F55D4", marker="o")
    ax1.set_ylabel("Median discount %")
    ax1.set_ylim(0, max(0.5, frame_sorted["median_discount"].max() * 1.1))
    ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, pos: f"{x:.0%}"))

    ax2 = ax1.twinx()
    ax2.bar(frame_sorted["ts_date"], frame_sorted["discounted_count"], alpha=0.3, color="#F59E0B")
    ax2.set_ylabel("Discounted SKUs")

    ax1.set_title(f"{category.title()} discounts {format_date(start_date)} – {format_date(end_date)}")
    fig.autofmt_xdate()

    output_path = OUTPUT_DIR / f"{category}-discount.png"
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    return ChartResult(category=category, path=output_path)


def _load_category_frame(engine: Engine, category: str, days: int) -> pd.DataFrame:
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)
    with engine.connect() as conn:
        query = text(
            """
            SELECT pr.ts_date, b.category, pr.price_cents, pr.compare_at_cents
            FROM prices pr
            JOIN variants v ON v.id = pr.variant_id
            JOIN products p ON p.id = v.product_id
            JOIN brands b ON b.id = p.brand_id
            WHERE b.category = :category AND pr.ts_date BETWEEN :start AND :end
            """
        )
        rows = conn.execute(
            query, {"category": category, "start": start_date, "end": end_date}
        ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["ts_date", "median_discount", "discounted_count"])
    df = pd.DataFrame(rows, columns=["ts_date", "category", "price_cents", "compare_at_cents"])
    df["discount_pct"] = df.apply(
        lambda row: _discount_pct(row["price_cents"], row["compare_at_cents"]), axis=1
    )
    grouped = (
        df.groupby("ts_date")
        .agg(median_discount=("discount_pct", "median"), discounted_count=("discount_pct", lambda x: (x > 0).sum()))
        .reset_index()
    )
    grouped["discounted_count"] = grouped["discounted_count"].astype(int)
    return grouped


def _discount_pct(price: int | None, compare_at: int | None) -> float:
    if price is None or compare_at in (None, 0):
        return 0.0
    if price >= compare_at:
        return 0.0
    return (compare_at - price) / compare_at
