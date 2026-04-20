"""
VN equities holiday calendar for scheduling and T+2 settlement (local, deterministic).

Loads a merged set from, in order:
  1) Packaged JSON (optional, default on) — curated closure dates.
  2) Optional local JSON file path (`VN_MARKET_HOLIDAYS_JSON_PATH`) — ops-maintained.
  3) Legacy CSV env (`VN_MARKET_HOLIDAYS_CSV`) — comma-separated YYYY-MM-DD.

No runtime network calls. Invalid file rows are skipped with warnings in logs.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_BUILTIN_REL = Path(__file__).resolve().parent.parent / "data" / "vn_market_holidays_builtin.json"


def parse_vn_market_holidays_csv(raw_csv: str) -> set[date]:
    """Parse comma-separated YYYY-MM-DD tokens (invalid tokens skipped)."""
    holidays: set[date] = set()
    raw = (raw_csv or "").strip()
    if not raw:
        return holidays
    for token in raw.split(","):
        day_text = token.strip()
        if not day_text:
            continue
        try:
            holidays.add(date.fromisoformat(day_text))
        except ValueError:
            logger.warning("vn_holiday_csv_invalid_token", extra={"token": day_text})
    return holidays


def _parse_iso_date_list(items: Iterable[Any]) -> set[date]:
    out: set[date] = set()
    for item in items:
        if item is None:
            continue
        text = str(item).strip()
        if not text:
            continue
        try:
            out.add(date.fromisoformat(text))
        except ValueError:
            logger.warning("vn_holiday_json_invalid_date", extra={"value": item})
    return out


def _dates_from_json_payload(payload: Any) -> set[date]:
    """Accept either [\"YYYY-MM-DD\", ...] or {\"dates\": [...]} / {\"holidays\": [...]}."""
    if payload is None:
        return set()
    if isinstance(payload, list):
        return _parse_iso_date_list(payload)
    if isinstance(payload, dict):
        for key in ("dates", "holidays", "closure_dates"):
            if key in payload and isinstance(payload[key], list):
                return _parse_iso_date_list(payload[key])
        logger.warning("vn_holiday_json_unexpected_shape", extra={"keys": list(payload.keys())})
    return set()


def load_vn_market_holidays_json_file(path: str) -> set[date]:
    """
    Read holidays from a local JSON file. Returns empty set if path is blank or unreadable.
    """
    raw_path = (path or "").strip()
    if not raw_path:
        return set()
    p = Path(raw_path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning(
            "vn_holiday_json_file_unreadable",
            extra={"path": raw_path, "error": str(exc)},
        )
        return set()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.warning(
            "vn_holiday_json_file_invalid_json",
            extra={"path": raw_path, "error": str(exc)},
        )
        return set()
    return _dates_from_json_payload(payload)


def load_builtin_vn_market_holidays() -> set[date]:
    """Load packaged builtin calendar; empty set on failure."""
    try:
        text = _BUILTIN_REL.read_text(encoding="utf-8")
    except OSError as exc:
        logger.error("vn_holiday_builtin_unreadable", extra={"path": str(_BUILTIN_REL), "error": str(exc)})
        return set()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.error("vn_holiday_builtin_invalid_json", extra={"error": str(exc)})
        return set()
    return _dates_from_json_payload(payload)


@lru_cache(maxsize=4)
def _merged_holidays_frozen(
    builtin_enabled: bool,
    json_path: str,
    csv_normalized: str,
) -> frozenset[date]:
    merged: set[date] = set()
    if builtin_enabled:
        merged |= load_builtin_vn_market_holidays()
    merged |= load_vn_market_holidays_json_file(json_path)
    merged |= parse_vn_market_holidays_csv(csv_normalized)
    return frozenset(merged)


def invalidate_vn_market_holiday_cache() -> None:
    """Clear resolver cache (e.g. after tests hot-reload settings)."""
    _merged_holidays_frozen.cache_clear()


def resolve_vn_market_holiday_dates(
    *,
    builtin_enabled: bool,
    json_path: str,
    csv: str,
) -> frozenset[date]:
    """
    Union of all configured holiday sources. Cached on (builtin flag, json path, csv string).
    """
    return _merged_holidays_frozen(bool(builtin_enabled), (json_path or "").strip(), csv or "")


def vn_market_timezone() -> ZoneInfo:
    """Market timezone from settings (short-term scan TZ)."""
    from app.core.config import settings

    return ZoneInfo(settings.short_term_scan_timezone)


def vn_market_local_today() -> date:
    """Current calendar date in configured VN market timezone."""
    return datetime.now(tz=vn_market_timezone()).date()


def is_vn_market_trading_day(on: date, holidays: frozenset[date] | set[date] | None = None) -> bool:
    """True if `on` is Mon–Fri and not in the holiday set."""
    if on.weekday() >= 5:
        return False
    if holidays and on in holidays:
        return False
    return True


def add_vn_trading_days(start_date: date, trading_days: int, holidays: frozenset[date] | set[date]) -> date:
    """
    Add `trading_days` VN trading days (skips weekends and dates in `holidays`).
    If trading_days is 0, returns start_date without requiring it to be a trading day.
    """
    remain = max(0, int(trading_days))
    cursor = start_date
    while remain > 0:
        cursor += timedelta(days=1)
        if is_vn_market_trading_day(cursor, holidays):
            remain -= 1
    return cursor
