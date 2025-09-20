"""CSV export helpers."""

from __future__ import annotations

import csv
import os
from datetime import date
from pathlib import Path
from typing import Iterable

import boto3
from sqlalchemy.engine import Engine
from sqlalchemy.sql import text

from app.utils.dates import format_date

OUTPUT_DIR = Path(os.environ.get("CSV_OUTPUT_DIR", "artifacts/csv"))

CSV_COLUMNS = [
    "date",
    "brand",
    "category",
    "product",
    "sku",
    "url",
    "price_cents",
    "compare_at_cents",
    "discount_pct",
    "delta_pct_1d",
    "delta_pct_7d",
    "available",
    "ad_surge",
    "new_ads_24h",
    "leader_score",
    "rank",
]


def generate_daily_csv(engine: Engine, as_of: date, *, upload: bool = False) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    file_path = OUTPUT_DIR / f"daily-{format_date(as_of)}.csv"
    rows = _load_rows(engine, as_of)
    with file_path.open("w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    if upload:
        _upload_to_s3(file_path)
    return file_path


def _load_rows(engine: Engine, as_of: date) -> Iterable[dict[str, object]]:
    query = text(
        """
        SELECT pr.ts_date, b.name AS brand, b.category, p.title, v.sku, p.url,
               pr.price_cents, pr.compare_at_cents, pr.available,
               COALESCE(ad.active_ads, 0) AS active_ads, COALESCE(ad.new_ads_24h, 0) AS new_ads_24h,
               COALESCE(l.score, 0) AS leader_score, COALESCE(l.rank, 0) AS rank,
               LAG(pr.price_cents) OVER (PARTITION BY pr.variant_id ORDER BY pr.ts_date) AS prev_price,
               LAG(pr.price_cents, 7) OVER (PARTITION BY pr.variant_id ORDER BY pr.ts_date) AS price_7d
        FROM prices pr
        JOIN variants v ON v.id = pr.variant_id
        JOIN products p ON p.id = v.product_id
        JOIN brands b ON b.id = p.brand_id
        LEFT JOIN ads_daily ad ON ad.brand_id = b.id AND ad.ts_date = pr.ts_date
        LEFT JOIN leaders l ON l.brand_id = b.id AND l.ts_date = pr.ts_date
        WHERE pr.ts_date = :date
        """
    )
    with engine.connect() as conn:
        result = conn.execute(query, {"date": as_of})
        for row in result.mappings():
            price_cents = row["price_cents"]
            compare_at = row["compare_at_cents"]
            discount_pct = _discount_pct(price_cents, compare_at)
            delta_1d = _pct_change(price_cents, row["prev_price"])
            delta_7d = _pct_change(price_cents, row["price_7d"])
            yield {
                "date": format_date(row["ts_date"]),
                "brand": row["brand"],
                "category": row["category"],
                "product": row["title"],
                "sku": row["sku"],
                "url": row["url"],
                "price_cents": price_cents,
                "compare_at_cents": compare_at,
                "discount_pct": round(discount_pct, 4),
                "delta_pct_1d": round(delta_1d, 4) if delta_1d is not None else None,
                "delta_pct_7d": round(delta_7d, 4) if delta_7d is not None else None,
                "available": row["available"],
                "ad_surge": row["active_ads"],
                "new_ads_24h": row["new_ads_24h"],
                "leader_score": row["leader_score"],
                "rank": row["rank"],
            }


def _pct_change(current: int | None, previous: int | None) -> float | None:
    if current is None or previous in (None, 0):
        return None
    return (current - previous) / max(previous, 1)


def _discount_pct(price: int | None, compare_at: int | None) -> float:
    if not price or not compare_at or price >= compare_at:
        return 0.0
    return (compare_at - price) / compare_at


def _upload_to_s3(path: Path) -> None:
    bucket = os.environ.get("AWS_S3_BUCKET")
    if not bucket:
        return
    endpoint = os.environ.get("AWS_S3_ENDPOINT")
    session = boto3.session.Session()
    client = session.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )
    client.upload_file(str(path), bucket, path.name)
