from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, ValidationError

from app.core.config import get_vn_market_holiday_dates, settings
from app.services.claude_service import ClaudeService
from app.services.demo_trading_service import (
    get_active_scheduler_demo_session_id_from_db,
    get_demo_session_cash_balance,
    get_demo_strategy_remaining_cash,
)
from app.services.gmail_service import GmailFetchService
from app.services.redis_cache import RedisCacheService
from app.services.short_term_scan_schedule import is_instant_on_short_term_scan_grid
from app.services.trading_core_service import evaluate_risk, place_order
from app.services.vnstock_api_service import VNStockApiService

logger = logging.getLogger(__name__)

_task: asyncio.Task | None = None
_entry_task: asyncio.Task | None = None
_run_markers: set[str] = set()


class _SignalPick(BaseModel):
    symbol: str = Field(min_length=1, max_length=20)
    entry: float = Field(gt=0)
    take_profit: float = Field(gt=0)
    stop_loss: float = Field(gt=0)
    confidence: float = Field(default=0.5, ge=0, le=1)
    reason: str = Field(default="", max_length=1000)


_gmail = GmailFetchService()
_claude = ClaudeService()
_redis = RedisCacheService()
_vnstock = VNStockApiService()


def _normalize_vn_price_unit(price: float) -> float:
    """
    Normalize Vietnam stock price unit to VND.
    Business rule: many signal sources return prices in "ngan dong" (e.g. 2.8 => 2,800 VND).
    """
    value = float(price)
    if value <= 0:
        return value
    # Heuristic: values below 1000 are interpreted as "thousand VND" unit.
    if value < 1000:
        return value * 1000.0
    return value


def _normalize_board_lot_qty(quantity: int, lot_size: int = 100) -> int:
    lot = max(1, int(lot_size))
    qty = max(0, int(quantity))
    return (qty // lot) * lot


def _mail_signal_allocated_nav() -> float:
    total = float(settings.strategy_total_cash_vnd)
    pct = float(settings.strategy_alloc_mail_signal_pct)
    nav = total * pct
    return max(1_000_000.0, nav)


def _mail_signal_allocated_nav_for_mode(account_mode: str) -> float:
    if str(account_mode).upper() == "DEMO":
        active_demo_session_id = get_active_scheduler_demo_session_id_from_db()
        strategy_cash = get_demo_strategy_remaining_cash(str(active_demo_session_id or ""), "MAIL_SIGNAL")
        if strategy_cash > 0:
            return max(1_000_000.0, strategy_cash)
        cash = get_demo_session_cash_balance(active_demo_session_id)
        if cash is not None and cash > 0:
            alloc = cash * float(settings.strategy_alloc_mail_signal_pct)
            return max(1_000_000.0, alloc)
    return _mail_signal_allocated_nav()


def _build_prompt(mail_rows: list[dict[str, str]]) -> str:
    chunks: list[str] = []
    for idx, row in enumerate(mail_rows, start=1):
        chunks.append(
            "\n".join(
                [
                    f"[MAIL {idx}]",
                    f"subject: {row.get('subject', '')}",
                    f"gmail_message_id: {row.get('gmail_message_id', '')}",
                    "content:",
                    row.get("text", "")[:8000],
                ]
            )
        )
    merged = "\n\n".join(chunks)
    return (
        "Phan tich cac email tin hieu chung khoan ben duoi va tra ve DUY NHAT JSON hop le.\n"
        "Schema bat buoc:\n"
        "{\n"
        '  "items": [\n'
        "    {\n"
        '      "symbol": "AAA",\n'
        '      "entry": 0.0,\n'
        '      "take_profit": 0.0,\n'
        '      "stop_loss": 0.0,\n'
        '      "confidence": 0.0,\n'
        '      "reason": "..." \n'
        "    }\n"
        "  ]\n"
        "}\n"
        "Chi lay ma co kha nang mua theo tin hieu ro rang, bo qua ma mo ho.\n"
        "Khong them markdown, khong text ngoai JSON.\n\n"
        f"{merged}"
    )


def _parse_claude_json(raw_text: str) -> list[dict[str, Any]]:
    text = raw_text.strip()
    if not text:
        return []
    if "```" in text:
        text = text.replace("```json", "```").strip()
        parts = [p.strip() for p in text.split("```") if p.strip()]
        if parts:
            text = parts[-1]
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        text = text[start : end + 1]
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Claude JSON parse failed: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Claude JSON root must be object.")
    raw_items = payload.get("items")
    if not isinstance(raw_items, list):
        return []
    parsed: list[dict[str, Any]] = []
    for row in raw_items:
        if not isinstance(row, dict):
            continue
        try:
            item = _SignalPick.model_validate(row)
        except ValidationError:
            continue
        normalized_entry = _normalize_vn_price_unit(float(item.entry))
        normalized_take_profit = _normalize_vn_price_unit(float(item.take_profit))
        normalized_stop_loss = _normalize_vn_price_unit(float(item.stop_loss))
        if normalized_stop_loss >= normalized_entry or normalized_take_profit <= normalized_entry:
            continue
        parsed.append(
            {
                **item.model_dump(),
                "entry": normalized_entry,
                "take_profit": normalized_take_profit,
                "stop_loss": normalized_stop_loss,
            }
        )
    return parsed


def run_mail_signal_pipeline() -> dict[str, Any]:
    query = str(settings.mail_signal_gmail_query or "").strip() or "Tín hiệu Cạn Cung"
    max_results = int(settings.mail_signal_gmail_max_results)
    mail_rows = _gmail.fetch_today_message_texts(query=query, max_results=max_results)
    if not mail_rows:
        result = {
            "success": True,
            "query": query,
            "mail_count": 0,
            "items": [],
            "note": "no_today_mail",
            "generated_at": datetime.now(tz=ZoneInfo(settings.mail_signal_scheduler_timezone)).isoformat(),
        }
        _redis.set_json(
            _daily_key(),
            result,
            ttl_seconds=int(settings.mail_signal_redis_ttl_seconds),
        )
        return result

    prompt = _build_prompt(mail_rows)
    response_text = _claude.generate_text_with_resilience(
        prompt=prompt,
        system_prompt="Ban la chuyen gia swing trading VN, output chuan JSON va uu tien quan tri rui ro.",
        model=settings.claude_model,
        max_tokens=900,
        temperature=0.1,
        cache_namespace="mail_signal_daily",
        cache_ttl_seconds=900,
    )
    items = _parse_claude_json(response_text)
    result = {
        "success": True,
        "query": query,
        "mail_count": len(mail_rows),
        "items": items,
        "generated_at": datetime.now(tz=ZoneInfo(settings.mail_signal_scheduler_timezone)).isoformat(),
        "source_message_ids": [row.get("gmail_message_id", "") for row in mail_rows],
    }
    _redis.set_json(
        _daily_key(),
        result,
        ttl_seconds=int(settings.mail_signal_redis_ttl_seconds),
    )
    return result


def _daily_key() -> str:
    now_local = datetime.now(tz=ZoneInfo(settings.mail_signal_scheduler_timezone))
    return f"signals:mail:daily:{now_local.strftime('%Y-%m-%d')}"


def _daily_key_for(dt_local: datetime) -> str:
    return f"signals:mail:daily:{dt_local.strftime('%Y-%m-%d')}"


def get_today_mail_signals() -> dict[str, Any] | None:
    return _redis.get_json(_daily_key())


def get_latest_mail_signals() -> dict[str, Any] | None:
    keys = _redis.scan_keys("signals:mail:daily:*", limit=2000)
    if not keys:
        return None
    dated: list[tuple[str, str]] = []
    for key in keys:
        token = key.rsplit(":", 1)[-1].strip()
        if len(token) == 10 and token[4] == "-" and token[7] == "-":
            dated.append((token, key))
    if not dated:
        return None
    dated.sort(key=lambda row: row[0], reverse=True)
    latest_key = dated[0][1]
    payload = _redis.get_json(latest_key)
    if payload is None:
        return None
    return {"redis_key": latest_key, **payload}


def get_latest_mail_signal_entry_run() -> dict[str, Any] | None:
    rows = get_recent_mail_signal_entry_runs(limit=1)
    if not rows:
        return None
    return rows[0]


def get_recent_mail_signal_entry_runs(limit: int = 10, demo_session_id: str | None = None) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 100))
    demo_sid = str(demo_session_id or "").strip()
    keys = _redis.scan_keys("signals:mail:entry-run:*", limit=2000)
    if not keys:
        return []
    dated: list[tuple[str, str]] = []
    for key in keys:
        token = key.rsplit(":", 1)[-1].strip()
        if len(token) == 10 and token[4] == "-" and token[7] == "-":
            dated.append((token, key))
        elif len(token) == 15 and token[8] == "-" and token[:8].isdigit() and token[9:].isdigit():
            # New format: YYYYMMDD-HHMMSS keeps lexical sort aligned with time.
            dated.append((token, key))
    if not dated:
        return []
    dated.sort(key=lambda row: row[0], reverse=True)
    recent_rows: list[dict[str, Any]] = []
    for _, key in dated:
        payload = _redis.get_json(key)
        if payload is None:
            continue
        row = {"redis_key": key, **payload}
        if demo_sid:
            if str(row.get("account_mode", "")).upper() != "DEMO":
                continue
            if str(row.get("demo_session_id") or "").strip() != demo_sid:
                continue
        recent_rows.append(row)
        if len(recent_rows) >= safe_limit:
            break
    return recent_rows


def _extract_latest_price(symbol: str) -> float | None:
    try:
        rows = _vnstock.call_quote(
            "intraday",
            source="VCI",
            symbol=symbol,
            method_kwargs={},
        )
        if isinstance(rows, list):
            for row in reversed(rows):
                if not isinstance(row, dict):
                    continue
                for key in ("match_price", "price", "close", "last_price"):
                    value = row.get(key)
                    if value is None:
                        continue
                    price = float(value)
                    if price > 0:
                        return price
    except Exception:
        pass

    try:
        rows = _vnstock.call_quote(
            "history",
            source="VCI",
            symbol=symbol,
            method_kwargs={"interval": "1D", "count_back": 3},
        )
        if isinstance(rows, list):
            for row in reversed(rows):
                if not isinstance(row, dict):
                    continue
                for key in ("close", "close_price", "price"):
                    value = row.get(key)
                    if value is None:
                        continue
                    price = float(value)
                    if price > 0:
                        return price
    except Exception:
        pass
    return None


def _entry_hit(side: str, market_price: float, entry: float) -> bool:
    # BUY flow: allow buy when market <= entry to avoid chasing above intended level.
    if side.upper() == "BUY":
        return market_price <= entry
    return market_price >= entry


def run_prev_day_entry_auto_trading_once(
    account_mode_override: str | None = None,
    demo_session_id_override: str | None = None,
    nav_override: float | None = None,
) -> dict[str, Any]:
    tz = ZoneInfo(settings.mail_signal_scheduler_timezone)
    now_local = datetime.now(tz=tz)
    prev_day = now_local - timedelta(days=1)
    prev_key = _daily_key_for(prev_day)
    payload = _redis.get_json(prev_key) or {}
    raw_items = payload.get("items") if isinstance(payload.get("items"), list) else []
    requested_mode = str(account_mode_override or "").strip().upper()
    account_mode = requested_mode if requested_mode in {"REAL", "DEMO"} else str(settings.mail_signal_entry_account_mode).upper()
    if account_mode == "DEMO":
        override_sid = str(demo_session_id_override or "").strip()
        active_demo_session_id = override_sid or get_active_scheduler_demo_session_id_from_db()
    else:
        active_demo_session_id = None
    nav = float(nav_override) if nav_override is not None else _mail_signal_allocated_nav_for_mode(account_mode)
    nav = max(1_000_000.0, nav)
    risk_per_trade = float(settings.mail_signal_entry_risk_per_trade)
    max_qty = int(settings.mail_signal_entry_max_quantity)

    executed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    run_success = True
    run_error: str | None = None

    try:
        for row in raw_items:
            if not isinstance(row, dict):
                continue
            try:
                item = _SignalPick.model_validate(row)
            except ValidationError:
                continue

            symbol = item.symbol.upper().strip()
            if not symbol:
                continue
            try:
                if account_mode == "DEMO" and not str(active_demo_session_id or "").strip():
                    skipped.append({"symbol": symbol, "reason": "missing_demo_session_id"})
                    continue
                execution_marker_key = f"signals:mail:executed:{prev_day.strftime('%Y-%m-%d')}:{symbol}:{account_mode}"
                if _redis.get_json(execution_marker_key) is not None:
                    skipped.append({"symbol": symbol, "reason": "already_executed"})
                    continue

                market_price = _extract_latest_price(symbol)
                if market_price is None or market_price <= 0:
                    skipped.append({"symbol": symbol, "reason": "missing_market_price"})
                    continue
                if not _entry_hit("BUY", market_price, float(item.entry)):
                    skipped.append({"symbol": symbol, "reason": "entry_not_hit", "market_price": market_price, "entry": item.entry})
                    continue

                risk = evaluate_risk(
                    {
                        "stoploss_price": float(item.stop_loss),
                        "entry_price": float(item.entry),
                        "nav": nav,
                        "risk_per_trade": risk_per_trade,
                        "daily_new_orders": 0,
                        "max_daily_new_orders": 10_000,
                    }
                )
                raw_qty = min(max_qty, int(risk.get("suggested_size") or 0))
                qty = _normalize_board_lot_qty(raw_qty, lot_size=100)
                if qty < 100:
                    skipped.append({"symbol": symbol, "reason": "risk_size_zero"})
                    continue

                idem_key = f"mail-entry-{prev_day.strftime('%Y%m%d')}-{account_mode}-{symbol}"
                placed = place_order(
                    {
                        "account_mode": account_mode,
                        "symbol": symbol,
                        "side": "BUY",
                        "quantity": qty,
                        "price": float(item.entry),
                        "auto_process": True,
                        "idempotency_key": idem_key,
                        "metadata": {
                            "source": "mail_signal_entry_scheduler",
                            "strategy_code": "MAIL_SIGNAL",
                            "demo_session_id": active_demo_session_id if account_mode == "DEMO" else None,
                            "signal_day": prev_day.strftime("%Y-%m-%d"),
                            "confidence": float(item.confidence),
                            "take_profit": float(item.take_profit),
                            "stop_loss": float(item.stop_loss),
                            "reason": item.reason,
                            "market_price_at_check": market_price,
                        },
                    }
                )
                _redis.set_json(
                    execution_marker_key,
                    {
                        "executed_at": now_local.isoformat(),
                        "symbol": symbol,
                        "order_id": placed.get("id"),
                        "status": placed.get("status"),
                    },
                    ttl_seconds=max(3600, int(settings.mail_signal_redis_ttl_seconds)),
                )
                executed.append({"symbol": symbol, "order_id": placed.get("id"), "status": placed.get("status"), "quantity": qty})
            except Exception as symbol_exc:
                logger.exception(
                    "mail_signal_entry_symbol_failed",
                    extra={"symbol": symbol, "account_mode": account_mode},
                )
                skipped.append({"symbol": symbol, "reason": "symbol_processing_error", "error": str(symbol_exc)})
    except Exception as run_exc:
        run_success = False
        run_error = str(run_exc)
        logger.exception("mail_signal_entry_run_failed", extra={"account_mode": account_mode})
    finally:
        result = {
            "success": run_success,
            "source_key": prev_key,
            "account_mode": account_mode,
            "demo_session_id": active_demo_session_id if account_mode == "DEMO" else None,
            "scanned": len(raw_items),
            "executed": executed,
            "skipped": skipped,
            "ran_at": now_local.isoformat(),
            "error": run_error,
        }
        _redis.set_json(
            f"signals:mail:entry-run:{now_local.strftime('%Y-%m-%d')}",
            result,
            ttl_seconds=max(3600, int(settings.mail_signal_redis_ttl_seconds)),
        )
        _redis.set_json(
            f"signals:mail:entry-run:{now_local.strftime('%Y%m%d-%H%M%S')}",
            result,
            ttl_seconds=max(3600, int(settings.mail_signal_redis_ttl_seconds)),
        )
    return result


async def _scheduler_loop() -> None:
    poll_seconds = 30
    tz = ZoneInfo(settings.mail_signal_scheduler_timezone)
    while True:
        try:
            now_local = datetime.now(tz=tz)
            marker = now_local.strftime("%Y-%m-%d")
            should_run = (
                now_local.weekday() < 5
                and now_local.hour == int(settings.mail_signal_scheduler_hour)
                and now_local.minute == int(settings.mail_signal_scheduler_minute)
                and marker not in _run_markers
            )
            if should_run:
                result = await asyncio.to_thread(run_mail_signal_pipeline)
                _run_markers.add(marker)
                logger.warning(
                    "mail_signal_scheduler_completed",
                    extra={
                        "marker": marker,
                        "mail_count": int(result.get("mail_count", 0)),
                        "signal_count": len(result.get("items", [])),
                    },
                )
            if len(_run_markers) > 10:
                _run_markers.clear()
        except Exception as exc:
            logger.warning("mail_signal_scheduler_failed", extra={"error": str(exc)})
        await asyncio.sleep(poll_seconds)


async def _entry_scheduler_loop() -> None:
    poll_seconds = 30
    tz = ZoneInfo(settings.short_term_scan_timezone)
    run_markers: set[str] = set()
    while True:
        try:
            now_local = datetime.now(tz=tz)
            on_grid = is_instant_on_short_term_scan_grid(
                now_local,
                interval_minutes=int(settings.short_term_scan_interval_minutes),
                holiday_dates=get_vn_market_holiday_dates(),
            )
            marker = now_local.strftime("%Y-%m-%d %H:%M")
            if on_grid and marker not in run_markers:
                result = await asyncio.to_thread(run_prev_day_entry_auto_trading_once)
                run_markers.add(marker)
                logger.warning(
                    "mail_signal_entry_scheduler_completed",
                    extra={
                        "marker": marker,
                        "scanned": int(result.get("scanned", 0)),
                        "executed": len(result.get("executed", [])),
                    },
                )
            if len(run_markers) > 400:
                run_markers.clear()
        except Exception as exc:
            logger.warning("mail_signal_entry_scheduler_failed", extra={"error": str(exc)})
        await asyncio.sleep(poll_seconds)


async def start_mail_signal_scheduler() -> None:
    global _task, _entry_task
    if not bool(settings.mail_signal_scheduler_enabled):
        logger.info("mail_signal_scheduler_disabled")
        return
    if _task and not _task.done():
        return
    _task = asyncio.create_task(_scheduler_loop(), name="mail-signal-scheduler")
    logger.warning("mail_signal_scheduler_started")
    if bool(settings.mail_signal_entry_scheduler_enabled):
        if _entry_task is None or _entry_task.done():
            _entry_task = asyncio.create_task(_entry_scheduler_loop(), name="mail-signal-entry-scheduler")
        logger.warning("mail_signal_entry_scheduler_started")


async def stop_mail_signal_scheduler() -> None:
    global _task, _entry_task
    if _task and not _task.done():
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
    _task = None
    if _entry_task and not _entry_task.done():
        _entry_task.cancel()
        try:
            await _entry_task
        except asyncio.CancelledError:
            pass
    _entry_task = None
    logger.warning("mail_signal_scheduler_stopped")
