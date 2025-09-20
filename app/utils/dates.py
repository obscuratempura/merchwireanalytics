"""Datetime helpers."""

from __future__ import annotations

import os
from datetime import date, datetime

import pendulum

DEFAULT_TZ = "America/Los_Angeles"


def timezone_name() -> str:
    return os.environ.get("TIMEZONE", DEFAULT_TZ)


def now_in_tz() -> pendulum.DateTime:
    tz = pendulum.timezone(timezone_name())
    return pendulum.now(tz)


def today_in_tz() -> date:
    return now_in_tz().date()


def parse_iso_date(value: str) -> date:
    return pendulum.parse(value).date()


def format_date(value: date) -> str:
    return value.strftime("%Y-%m-%d")


def localized_send_time() -> datetime:
    hour = int(os.environ.get("SEND_HOUR", "7"))
    minute = int(os.environ.get("SEND_MINUTE", "30"))
    tz = pendulum.timezone(timezone_name())
    now = pendulum.now(tz)
    scheduled = tz.datetime(now.year, now.month, now.day, hour, minute)
    return scheduled
