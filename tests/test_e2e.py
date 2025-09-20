import os
from datetime import date

import pytest

from app.jobs import daily
from app.logic import export_csv, charts
from app.ingest.models import Brand


@pytest.mark.asyncio
async def test_run_daily_e2e(monkeypatch, tmp_path, seeded_engine):
    sent_messages = []

    # Override artifact directories
    export_csv.OUTPUT_DIR = tmp_path / "csv"
    charts.OUTPUT_DIR = tmp_path / "charts"

    monkeypatch.setenv("ESP_PROVIDER", "log")
    monkeypatch.setenv("SIGNING_SECRET", "secret")

    monkeypatch.setattr(daily, "create_engine_from_env", lambda: seeded_engine)
    monkeypatch.setattr(
        daily,
        "load_brands",
        lambda: [
            Brand(name="HexCo", domain="https://hexco.com", category="skincare", facebook_page_id="123"),
            Brand(name="Lumi Threads", domain="https://lumithreads.com", category="apparel", facebook_page_id="234"),
        ],
    )

    async def fake_ingest(self, brand, as_of=None):
        return None

    monkeypatch.setattr(daily.ShopifyIngestor, "ingest", fake_ingest, raising=False)

    class DummyEmailProvider:
        async def send(self, message):
            sent_messages.append(message)

    monkeypatch.setattr(daily, "EmailProvider", lambda: DummyEmailProvider())

    await daily.run_daily(as_of=date.today())

    assert sent_messages
    csv_files = list(export_csv.OUTPUT_DIR.glob("*.csv"))
    chart_files = list(charts.OUTPUT_DIR.glob("*.png"))
    assert csv_files
    assert chart_files
