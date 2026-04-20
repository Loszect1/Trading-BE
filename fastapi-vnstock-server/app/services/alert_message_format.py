from __future__ import annotations

from typing import Any


def format_alert_plain_text(
    *,
    rule_id: str,
    severity: str,
    account_mode: str | None,
    title: str,
    details: dict[str, Any],
) -> str:
    """Plain-text alert body for logs and future channels (no icons)."""
    lines = [
        f"severity={severity}",
        f"rule_id={rule_id}",
        f"account_mode={account_mode or 'ALL'}",
        f"title={title}",
    ]
    for key in sorted(details.keys()):
        val = details[key]
        lines.append(f"{key}={val}")
    return "\n".join(lines)
