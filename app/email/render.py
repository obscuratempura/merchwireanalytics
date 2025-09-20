"""Email rendering utilities."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

try:  # pragma: no cover - optional dependency
    from mjml import mjml_to_html
except Exception:  # pragma: no cover
    mjml_to_html = None  # type: ignore

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent
ENV = Environment(
    loader=FileSystemLoader(str(TEMPLATE_DIR)),
    autoescape=select_autoescape(enabled_extensions=("mjml", "html")),
)


class EmailRenderError(RuntimeError):
    pass


def render_email(kind: str, context: dict[str, Any]) -> tuple[str, str]:
    template = ENV.get_template("template.mjml")
    merged_context = {"subject": context.get("subject"), **context}
    mjml_markup = template.render(**merged_context)
    if mjml_to_html:
        result = mjml_to_html(mjml_markup)
        if result.errors:  # pragma: no cover - requires CLI
            logger.warning("MJML errors: %s", result.errors)
        html = result.html
    else:
        html = _naive_mjml_to_html(mjml_markup)
    subject = context.get("subject", "Merchwire Brief")
    return subject, html


def _naive_mjml_to_html(mjml_markup: str) -> str:
    """Simplistic MJML → HTML conversion for development environments."""
    body = mjml_markup.replace("<mjml>", "").replace("</mjml>", "")
    replacements = {
        "<mj-body": "<div",
        "</mj-body>": "</div>",
        "<mj-section": "<section",
        "</mj-section>": "</section>",
        "<mj-column": "<div",
        "</mj-column>": "</div>",
        "<mj-text": "<p",
        "</mj-text>": "</p>",
        "<mj-table": "<table",
        "</mj-table>": "</table>",
        "<mj-button": "<a class=\"btn\"",
        "</mj-button>": "</a>",
        "<mj-image": "<img",
        "</mj-image>": "",
    }
    for src, dest in replacements.items():
        body = body.replace(src, dest)
    return "<html><body>" + body + "</body></html>"
