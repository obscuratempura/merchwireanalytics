"""Celery configuration for scheduled jobs."""

from __future__ import annotations

import os

from celery import Celery
from celery.schedules import crontab

from app.utils.dates import timezone_name

broker_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")
backend_url = os.environ.get("REDIS_URL", "redis://redis:6379/0")

celery_app = Celery("merchwire", broker=broker_url, backend=backend_url, include=["app.jobs.daily", "app.jobs.weekly"])
celery_app.conf.timezone = timezone_name()
celery_app.conf.beat_schedule = {
    "daily-brief": {
        "task": "app.jobs.daily.run_daily",
        "schedule": crontab(hour=int(os.environ.get("SEND_HOUR", "7")), minute=int(os.environ.get("SEND_MINUTE", "30")), day_of_week="mon-fri"),
    },
    "weekly-brief": {
        "task": "app.jobs.weekly.run_weekly",
        "schedule": crontab(day_of_week="mon", hour=int(os.environ.get("SEND_HOUR", "7")), minute=int(os.environ.get("SEND_MINUTE", "30"))),
    },
}


@celery_app.task(name="app.jobs.daily.run_daily")
def run_daily_task():  # pragma: no cover - executed by worker
    import asyncio

    from app.jobs.daily import run_daily

    asyncio.run(run_daily())


@celery_app.task(name="app.jobs.weekly.run_weekly")
def run_weekly_task():  # pragma: no cover - executed by worker
    import asyncio

    from app.jobs.weekly import run_weekly

    asyncio.run(run_weekly())
