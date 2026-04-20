from __future__ import annotations

from typing import Any

import httpx

from app.core.config import settings


def _telegram_enabled() -> bool:
    return bool(settings.monitoring_telegram_bot_token and settings.monitoring_telegram_chat_id)


def _slack_enabled() -> bool:
    return bool(settings.monitoring_slack_webhook_url)


def dispatch_alert_to_channels(message: str) -> dict[str, Any]:
    """
    Push alert message to configured external channels.

    Returns delivery report for observability without raising to caller.
    """
    report: dict[str, Any] = {
        "telegram": {"enabled": _telegram_enabled(), "delivered": False, "error": None},
        "slack": {"enabled": _slack_enabled(), "delivered": False, "error": None},
    }

    timeout = settings.monitoring_alert_dispatch_timeout_seconds
    with httpx.Client(timeout=timeout) as client:
        if report["telegram"]["enabled"]:
            telegram_url = f"https://api.telegram.org/bot{settings.monitoring_telegram_bot_token}/sendMessage"
            telegram_payload = {
                "chat_id": settings.monitoring_telegram_chat_id,
                "text": message,
                "disable_web_page_preview": True,
            }
            try:
                response = client.post(telegram_url, json=telegram_payload)
                response.raise_for_status()
                body = response.json()
                if not bool(body.get("ok")):
                    report["telegram"]["error"] = f"telegram_api_not_ok: {body}"
                else:
                    report["telegram"]["delivered"] = True
            except Exception as exc:
                report["telegram"]["error"] = str(exc)

        if report["slack"]["enabled"]:
            slack_payload = {"text": message}
            try:
                response = client.post(settings.monitoring_slack_webhook_url, json=slack_payload)
                response.raise_for_status()
                report["slack"]["delivered"] = True
            except Exception as exc:
                report["slack"]["error"] = str(exc)

    return report
