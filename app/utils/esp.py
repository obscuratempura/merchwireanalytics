"""Email sending helpers."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Iterable

import httpx

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class EmailMessage:
    to: str
    subject: str
    html: str


class EmailProvider:
    def __init__(self) -> None:
        self.provider = os.environ.get("ESP_PROVIDER", "log")
        self.resend_api_key = os.environ.get("RESEND_API_KEY")
        self.ses_region = os.environ.get("AWS_REGION", "us-east-1")

    async def send(self, message: EmailMessage) -> None:
        if self.provider == "resend" and self.resend_api_key:
            await self._send_resend(message)
        elif self.provider == "ses":
            await self._send_ses(message)
        else:
            logger.info("Email (log) → %s: %s", message.to, message.subject)

    async def _send_resend(self, message: EmailMessage) -> None:
        url = "https://api.resend.com/emails"
        payload = {
            "from": "Merchwire Brief <brief@merchwire.com>",
            "to": [message.to],
            "subject": message.subject,
            "html": message.html,
        }
        headers = {"Authorization": f"Bearer {self.resend_api_key}"}
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()

    async def _send_ses(self, message: EmailMessage) -> None:  # pragma: no cover - network
        logger.warning("SES sending not implemented in dev; logging message")
        logger.info("SES Email → %s: %s", message.to, message.subject)
