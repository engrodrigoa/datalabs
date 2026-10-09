"""
Minimal, dependency-free alert fan-out.

Configure in .env (all optional; without them alerts are only logged as `alert` events):
  ALERT_WEBHOOK_URL      Slack / Discord / Teams / any webhook
  ALERT_WEBHOOK_FORMAT   slack (default) | discord | generic
  TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID
  ALERT_MIN_SEVERITY     info | warning | error (default: warning)
"""
from __future__ import annotations

import logging
import os

import requests

from pipelines.commons.logger import get_logger, log_event

logger = get_logger("observability.alerting")

_SEVERITY_RANK = {"info": 0, "warning": 1, "error": 2, "critical": 3}
_ICON = {"info": "ℹ️", "warning": "⚠️", "error": "🔴", "critical": "🚨"}


def _format(title: str, message: str, severity: str, fields: dict | None) -> str:
    lines = [f"{_ICON.get(severity, '')} [datalabs] {title}"]
    if message:
        lines.append(message.strip())
    for key, value in (fields or {}).items():
        if value not in (None, ""):
            lines.append(f"• {key}: {value}")
    return "\n".join(lines)


def send_alert(title: str, message: str = "", severity: str = "error", fields: dict | None = None) -> bool:
    """Send an alert to every configured channel. Never raises. Returns True if at least one channel accepted it."""
    min_sev = os.getenv("ALERT_MIN_SEVERITY", "warning").lower()
    level = logging.ERROR if _SEVERITY_RANK.get(severity, 2) >= 2 else logging.WARNING
    log_event(logger, "alert", title, level=level, severity=severity, alert_message=message[:500],
              **{f"f_{k}": v for k, v in (fields or {}).items()})

    if _SEVERITY_RANK.get(severity, 2) < _SEVERITY_RANK.get(min_sev, 1):
        return False

    text = _format(title, message, severity, fields)
    delivered = False

    webhook = os.getenv("ALERT_WEBHOOK_URL")
    if webhook:
        fmt = os.getenv("ALERT_WEBHOOK_FORMAT", "slack").lower()
        payload = {"content": text[:1900]} if fmt == "discord" else (
            {"title": title, "message": message, "severity": severity, "fields": fields or {}} if fmt == "generic"
            else {"text": text})
        delivered |= _post(webhook, payload, channel=f"webhook:{fmt}")

    token, chat_id = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        delivered |= _post(f"https://api.telegram.org/bot{token}/sendMessage",
                           {"chat_id": chat_id, "text": text[:4000], "disable_web_page_preview": True},
                           channel="telegram")
    return delivered


def _post(url: str, payload: dict, channel: str) -> bool:
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as exc:  # never break a pipeline because the alert channel is down
        log_event(logger, "alert_delivery_failed", f"{channel}: {exc}", level=logging.WARNING, channel=channel)
        return False
