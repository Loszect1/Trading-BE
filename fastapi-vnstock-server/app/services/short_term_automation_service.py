"""
Production-oriented short-term automation: scanner → BUY signals → risk → execution.

Scheduler safety: PostgreSQL advisory lock (single session) for the whole cycle so
overlapping cron ticks do not double-run. Optional VN session grid guard uses
`short_term_scan_schedule` and `AppSettings` cadence.
"""

from __future__ import annotations

import logging
import multiprocessing as mp
import json
import queue
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID, uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import get_vn_market_holiday_dates, settings
from app.services.claude_service import ClaudeService
from app.services.experience_service import create_experience_from_trade, ensure_experience_table
from app.services.short_term_scan_schedule import is_now_on_short_term_scan_grid
from app.services.signal_engine_service import _get_close_and_volume, run_short_term_scan_batch, run_short_term_scan_batch_light
from app.services.trading_core_service import ensure_trading_core_tables, evaluate_risk, get_positions, place_order

logger = logging.getLogger(__name__)

# Distinct advisory lock keys (two-argument form); avoid collision with other features.
_ADVISORY_LOCK_SPACE = 582_901
_ADVISORY_LOCK_KEY_REAL = 771_002
_ADVISORY_LOCK_KEY_DEMO = 771_003
_ASYNC_RUNS_LOCK = threading.Lock()
_ASYNC_RUNS: dict[str, dict[str, Any]] = {}
_automation_claude_service = ClaudeService()


def _is_scanner_universe_empty_error(exc: Exception) -> bool:
    message = str(exc or "")
    return "scanner_universe_empty" in message


def _resolve_advisory_lock_key(account_mode: str) -> int:
    mode = str(account_mode or "").strip().upper()
    if mode == "DEMO":
        return _ADVISORY_LOCK_KEY_DEMO
    return _ADVISORY_LOCK_KEY_REAL


def _extract_json_object(raw_text: str) -> dict[str, Any] | None:
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        parsed = json.loads(raw_text[start : end + 1])
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _refine_buy_levels_with_claude(sig: dict[str, Any]) -> dict[str, Any] | None:
    if not bool(settings.ai_claude_automation_levels_enabled):
        return None
    symbol = str(sig.get("symbol") or "").strip().upper()
    entry = float(sig.get("entry_price") or 0.0)
    tp = float(sig.get("take_profit_price") or 0.0)
    sl = float(sig.get("stoploss_price") or 0.0)
    if not symbol or entry <= 0 or sl <= 0:
        return None
    prompt = (
        "You are a VN equity short-term trading assistant.\n"
        "Return ONLY valid JSON (no markdown) with keys:\n"
        '{"entry_price": number, "take_profit_price": number, "stoploss_price": number, "rationale": "string"}\n'
        "Constraints:\n"
        "- stoploss_price < entry_price\n"
        "- take_profit_price >= entry_price\n"
        "- keep values realistic and close to input levels\n"
        f"Input signal: symbol={symbol}, entry_price={entry}, take_profit_price={tp}, stoploss_price={sl}, metadata={sig.get('metadata') or {}}"
    )
    raw = _automation_claude_service.generate_text_with_resilience(
        prompt=prompt,
        system_prompt="Return strict JSON only. No prose outside JSON.",
        max_tokens=settings.ai_claude_automation_levels_max_tokens,
        temperature=0.1,
        cache_namespace="automation-levels",
        cache_ttl_seconds=settings.ai_claude_automation_levels_cache_ttl_seconds,
    )
    parsed = _extract_json_object(raw)
    if not parsed:
        return None
    try:
        refined_entry = float(parsed.get("entry_price") or entry)
        refined_tp = float(parsed.get("take_profit_price") or tp or entry)
        refined_sl = float(parsed.get("stoploss_price") or sl)
    except (TypeError, ValueError):
        return None
    if refined_entry <= 0 or refined_sl <= 0:
        return None
    if refined_sl >= refined_entry:
        return None
    if refined_tp < refined_entry:
        refined_tp = refined_entry
    return {
        "entry_price": refined_entry,
        "take_profit_price": refined_tp,
        "stoploss_price": refined_sl,
        "rationale": str(parsed.get("rationale") or "").strip(),
    }


def _buy_score(sig: dict[str, Any]) -> float:
    score = 0.0
    try:
        score = float(sig.get("confidence") or 0.0)
    except (TypeError, ValueError):
        score = 0.0
    meta = sig.get("metadata")
    if isinstance(meta, dict):
        sq = meta.get("signal_quality")
        if isinstance(sq, dict):
            try:
                score = max(score, float(sq.get("composite_score_0_100") or 0.0))
            except (TypeError, ValueError):
                pass
    return max(1.0, score)


def _allocate_quantities_by_score(
    buy_rows: list[dict[str, Any]],
    nav_total: float,
    *,
    lot_size: int = 100,
) -> dict[str, int]:
    lot = max(1, int(lot_size))
    total_nav = max(0.0, float(nav_total))
    if total_nav <= 0 or not buy_rows:
        return {}
    scored: list[tuple[str, float, float]] = []
    for sig in buy_rows:
        sid = str(sig.get("id") or "").strip()
        entry = float(sig.get("entry_price") or 0.0)
        if not sid or entry <= 0:
            continue
        scored.append((sid, entry, _buy_score(sig)))
    if not scored:
        return {}
    sum_score = sum(s for _sid, _entry, s in scored) or float(len(scored))
    allocations: dict[str, int] = {}
    remaining_nav = total_nav
    for idx, (sid, entry, score) in enumerate(scored):
        if remaining_nav < entry * lot:
            allocations[sid] = 0
            continue
        if idx == len(scored) - 1:
            budget = remaining_nav
        else:
            budget = total_nav * (score / sum_score)
            budget = min(budget, remaining_nav)
        raw_qty = int(budget / entry)
        qty = (raw_qty // lot) * lot
        if qty <= 0 and remaining_nav >= entry * lot:
            qty = lot
        notional = float(qty) * entry
        if notional > remaining_nav and entry > 0:
            qty = int((remaining_nav / entry) // lot) * lot
            notional = float(qty) * entry
        allocations[sid] = max(0, int(qty))
        remaining_nav = max(0.0, remaining_nav - notional)
    return allocations


def _build_claude_buy_decision_prompt(
    *,
    buy_rows: list[dict[str, Any]],
    strategy_remaining_cash: float,
    lot_size: int = 100,
) -> str:
    compact_rows: list[dict[str, Any]] = []
    for row in buy_rows:
        compact_rows.append(
            {
                "signal_id": str(row.get("id") or "").strip(),
                "symbol": str(row.get("symbol") or "").strip().upper(),
                "entry_price": float(row.get("entry_price") or 0.0),
                "take_profit_price": float(row.get("take_profit_price") or 0.0),
                "stoploss_price": float(row.get("stoploss_price") or 0.0),
                "confidence": float(row.get("confidence") or 0.0),
                "metadata": row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
            }
        )
    payload = {
        "strategy_remaining_cash": float(strategy_remaining_cash),
        "lot_size": int(max(1, lot_size)),
        "candidates": compact_rows,
    }
    return (
        "You are a VN equities short-term trading decision engine.\n"
        "Return only valid JSON with this exact schema:\n"
        "{\n"
        '  "items": [\n'
        "    {\n"
        '      "signal_id": "string",\n'
        '      "symbol": "string",\n'
        '      "quantity": 0,\n'
        '      "entry_price": 0,\n'
        '      "take_profit_price": 0,\n'
        '      "stoploss_price": 0,\n'
        '      "reason": "string"\n'
        "    }\n"
        "  ]\n"
        "}\n"
        "Hard constraints:\n"
        "- Total notional (sum(quantity * entry_price)) must be <= strategy_remaining_cash.\n"
        "- quantity must be multiple of lot_size and >= 0.\n"
        "- stoploss_price < entry_price <= take_profit_price.\n"
        "- Select only symbols from candidates.\n"
        "- Prefer higher quality setups and avoid over-concentration.\n"
        f"Input: {json.dumps(payload, ensure_ascii=True)}"
    )


def _apply_claude_buy_decision(
    *,
    buy_rows: list[dict[str, Any]],
    strategy_remaining_cash: float,
    lot_size: int = 100,
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, Any]]:
    if not buy_rows:
        return [], {}, {"source": "none", "selected": 0}

    safe_lot = max(1, int(lot_size))
    fallback_qty = _allocate_quantities_by_score(buy_rows, float(strategy_remaining_cash), lot_size=safe_lot)
    fallback_rows = [dict(row) for row in buy_rows]
    prompt = _build_claude_buy_decision_prompt(
        buy_rows=buy_rows,
        strategy_remaining_cash=float(strategy_remaining_cash),
        lot_size=safe_lot,
    )
    try:
        raw = _automation_claude_service.generate_text_with_resilience(
            prompt=prompt,
            system_prompt="Return strict JSON only. No markdown or prose.",
            model=settings.claude_model,
            max_tokens=1200,
            temperature=0.1,
            cache_namespace="short-term-buy-decision",
            cache_ttl_seconds=120,
        )
        parsed = _extract_json_object(raw)
        if not isinstance(parsed, dict):
            raise ValueError("claude_buy_decision_invalid_json")
        raw_items = parsed.get("items")
        if not isinstance(raw_items, list):
            raise ValueError("claude_buy_decision_items_missing")

        row_by_signal_id: dict[str, dict[str, Any]] = {
            str(row.get("id") or "").strip(): dict(row)
            for row in buy_rows
            if str(row.get("id") or "").strip()
        }
        planned_qty_by_signal_id: dict[str, int] = {}
        selected_rows: list[dict[str, Any]] = []
        spent = 0.0

        for item in raw_items:
            if not isinstance(item, dict):
                continue
            signal_id = str(item.get("signal_id") or "").strip()
            symbol = str(item.get("symbol") or "").strip().upper()
            base_row = row_by_signal_id.get(signal_id)
            if base_row is None:
                continue
            if symbol and symbol != str(base_row.get("symbol") or "").strip().upper():
                continue
            try:
                qty = int(item.get("quantity") or 0)
                entry = float(item.get("entry_price") or base_row.get("entry_price") or 0.0)
                tp = float(item.get("take_profit_price") or base_row.get("take_profit_price") or 0.0)
                sl = float(item.get("stoploss_price") or base_row.get("stoploss_price") or 0.0)
            except (TypeError, ValueError):
                continue
            if entry <= 0 or sl <= 0 or sl >= entry or tp < entry:
                continue
            qty = max(0, (qty // safe_lot) * safe_lot)
            if qty <= 0:
                continue
            next_spent = spent + float(qty) * entry
            if next_spent > float(strategy_remaining_cash):
                continue
            spent = next_spent
            row = dict(base_row)
            row["entry_price"] = entry
            row["take_profit_price"] = tp
            row["stoploss_price"] = sl
            row["claude_decision_reason"] = str(item.get("reason") or "").strip()
            selected_rows.append(row)
            planned_qty_by_signal_id[signal_id] = qty

        if selected_rows:
            return selected_rows, planned_qty_by_signal_id, {
                "source": "claude",
                "selected": len(selected_rows),
                "spent": round(spent, 2),
                "strategy_remaining_cash": float(strategy_remaining_cash),
            }
    except Exception as exc:
        logger.warning("short_term_claude_buy_decision_failed", extra={"error": str(exc)})

    return fallback_rows, fallback_qty, {
        "source": "score_weighted_fallback",
        "selected": len(fallback_rows),
        "strategy_remaining_cash": float(strategy_remaining_cash),
    }


def _resolve_sell_trigger(*, last_price: float, take_profit: float, stoploss: float) -> str | None:
    if last_price <= 0 or take_profit <= 0 or stoploss <= 0:
        return None
    if last_price >= take_profit:
        return "take_profit_hit"
    if last_price <= stoploss:
        return "stoploss_hit"
    return None


def _get_position_exit_levels(*, account_mode: str, symbol: str, avg_price: float) -> tuple[float, float]:
    fallback_tp = float(avg_price) * 1.04 if avg_price > 0 else 0.0
    fallback_sl = float(avg_price) * 0.97 if avg_price > 0 else 0.0
    try:
        with connect(settings.database_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT order_metadata
                    FROM orders_core
                    WHERE account_mode = %(account_mode)s
                      AND symbol = %(symbol)s
                      AND side = 'BUY'
                      AND status = 'FILLED'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    {"account_mode": account_mode, "symbol": symbol},
                )
                row = cur.fetchone() or {}
        meta = row.get("order_metadata") if isinstance(row, dict) else {}
        if not isinstance(meta, dict):
            return fallback_tp, fallback_sl
        tp = float(meta.get("take_profit_price") or fallback_tp)
        sl = float(meta.get("stoploss_price") or fallback_sl)
        if tp <= 0 or sl <= 0:
            return fallback_tp, fallback_sl
        return tp, sl
    except Exception:
        return fallback_tp, fallback_sl


def _build_sell_candidates_from_positions(account_mode: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for pos in get_positions(account_mode):
        symbol = str(pos.get("symbol") or "").strip().upper()
        available_qty = int(pos.get("available_qty") or 0)
        avg_price = float(pos.get("avg_price") or 0.0)
        qty = (available_qty // 100) * 100
        if not symbol or qty <= 0 or avg_price <= 0:
            continue
        closes, _ = _get_close_and_volume(symbol=symbol, bars=3, exchange="")
        if not closes:
            continue
        last_price = float(closes[-1] or 0.0)
        if last_price <= 0:
            continue
        take_profit, stoploss = _get_position_exit_levels(account_mode=account_mode, symbol=symbol, avg_price=avg_price)
        trigger = _resolve_sell_trigger(last_price=last_price, take_profit=take_profit, stoploss=stoploss)
        if not trigger:
            continue
        candidates.append(
            {
                "symbol": symbol,
                "quantity": qty,
                "price": last_price,
                "avg_price": avg_price,
                "trigger": trigger,
                "take_profit_price": take_profit,
                "stoploss_price": stoploss,
            }
        )
    return candidates


def _handle_one_sell_candidate(*, candidate: dict[str, Any], account_mode: str, run_id: UUID) -> dict[str, Any]:
    symbol = str(candidate.get("symbol") or "").strip().upper()
    qty = int(candidate.get("quantity") or 0)
    price = float(candidate.get("price") or 0.0)
    trigger = str(candidate.get("trigger") or "").strip()
    if not symbol or qty <= 0 or price <= 0:
        return {
            "executed": 0,
            "execution_rejected": 1,
            "errors": 0,
            "detail": {"symbol": symbol, "outcome": "execution_rejected", "reason": "invalid_sell_payload"},
        }
    idempotency_key = f"short-term-sell:{symbol}:{trigger}:{str(run_id)}"
    order_row = place_order(
        {
            "account_mode": account_mode,
            "symbol": symbol,
            "side": "SELL",
            "quantity": qty,
            "price": price,
            "idempotency_key": idempotency_key,
            "auto_process": True,
            "metadata": {
                "source": "short_term_schedule_exit",
                "strategy_code": "SHORT_TERM",
                "trigger": trigger,
                "take_profit_price": float(candidate.get("take_profit_price") or 0.0),
                "stoploss_price": float(candidate.get("stoploss_price") or 0.0),
            },
        }
    )
    resolved_order = order_row.get("order") if isinstance(order_row.get("order"), dict) else order_row
    status = str((resolved_order or {}).get("status", "")).upper()
    if status == "FILLED":
        if trigger == "stoploss_hit":
            try:
                ensure_experience_table()
                avg_price = float(candidate.get("avg_price") or 0.0)
                pnl_value = (price - avg_price) * float(qty)
                pnl_percent = ((price / avg_price) - 1.0) * 100.0 if avg_price > 0 else 0.0
                create_experience_from_trade(
                    {
                        "trade_id": str((resolved_order or {}).get("id") or f"short-term-stoploss:{symbol}:{str(run_id)}"),
                        "account_mode": account_mode,
                        "symbol": symbol,
                        "strategy_type": "SHORT_TERM",
                        "pnl_value": pnl_value,
                        "pnl_percent": pnl_percent,
                        "market_context": {
                            "trigger": trigger,
                            "exit_price": price,
                            "entry_proxy_price": avg_price,
                            "quantity": qty,
                            "take_profit_price": float(candidate.get("take_profit_price") or 0.0),
                            "stoploss_price": float(candidate.get("stoploss_price") or 0.0),
                            "source": "short_term_schedule_exit",
                        },
                    }
                )
            except Exception as exc:
                logger.warning(
                    "short_term_stoploss_experience_failed",
                    extra={"symbol": symbol, "account_mode": account_mode, "error": str(exc)},
                )
        return {
            "executed": 1,
            "execution_rejected": 0,
            "errors": 0,
            "detail": {
                "symbol": symbol,
                "side": "SELL",
                "outcome": "executed",
                "order_id": (resolved_order or {}).get("id"),
                "quantity": qty,
                "price": price,
                "trigger": trigger,
            },
        }
    return {
        "executed": 0,
        "execution_rejected": 1,
        "errors": 0,
        "detail": {
            "symbol": symbol,
            "side": "SELL",
            "outcome": "execution_rejected",
            "reason": (resolved_order or {}).get("reason") or f"unexpected_order_status:{status}",
            "quantity": qty,
            "price": price,
            "trigger": trigger,
        },
    }


def _handle_one_buy_signal(
    *,
    sig: dict[str, Any],
    account_mode: str,
    available_cash: float,
    risk_per_trade: float,
    max_daily_new_orders: int,
    daily_new_orders: int,
    planned_quantity: int = 0,
) -> dict[str, Any]:
    symbol = str(sig.get("symbol", "")).strip().upper()
    sid = str(sig.get("id", ""))
    entry = float(sig.get("entry_price") or 0.0)
    tp = float(sig.get("take_profit_price") or 0.0)
    stoploss = float(sig.get("stoploss_price") or 0.0)
    level_source = "signal"
    try:
        refined = _refine_buy_levels_with_claude(sig)
    except Exception as exc:
        logger.warning("short_term_claude_levels_failed", extra={"symbol": symbol, "error": str(exc)})
        refined = None
    if isinstance(refined, dict):
        entry = float(refined.get("entry_price") or entry)
        tp = float(refined.get("take_profit_price") or tp)
        stoploss = float(refined.get("stoploss_price") or stoploss)
        level_source = "claude"
    if entry <= 0 or stoploss <= 0:
        return {
            "risk_rejected": 0,
            "executed": 0,
            "execution_rejected": 1,
            "errors": 0,
            "daily_new_orders_delta": 0,
            "cash_spent": 0.0,
            "detail": {
                "symbol": symbol,
                "outcome": "rejected",
                "reason": "invalid_prices",
                "entry_price": entry,
                "take_profit_price": tp,
                "stoploss_price": stoploss,
                "level_source": level_source,
            },
        }
    safe_available_cash = max(0.0, float(available_cash))
    if safe_available_cash < (entry * 100.0):
        return {
            "risk_rejected": 1,
            "executed": 0,
            "execution_rejected": 0,
            "errors": 0,
            "daily_new_orders_delta": 0,
            "cash_spent": 0.0,
            "detail": {
                "symbol": symbol,
                "outcome": "risk_rejected",
                "reason": "insufficient_cash_for_lot100_runtime",
                "entry_price": entry,
                "take_profit_price": tp,
                "stoploss_price": stoploss,
                "level_source": level_source,
                "available_cash": safe_available_cash,
                "required_cash_for_lot100": round(entry * 100.0, 6),
            },
        }

    risk_payload = {
        "stoploss_price": stoploss,
        "entry_price": entry,
        "nav": safe_available_cash,
        "risk_per_trade": float(risk_per_trade),
        "daily_new_orders": int(daily_new_orders),
        "max_daily_new_orders": int(max_daily_new_orders),
    }
    risk_result = evaluate_risk(risk_payload)
    if not risk_result.get("pass"):
        return {
            "risk_rejected": 1,
            "executed": 0,
            "execution_rejected": 0,
            "errors": 0,
            "daily_new_orders_delta": 0,
            "cash_spent": 0.0,
            "detail": {
                "symbol": symbol,
                "outcome": "risk_rejected",
                "reason": risk_result.get("reason"),
                "entry_price": entry,
                "take_profit_price": tp,
                "stoploss_price": stoploss,
                "level_source": level_source,
            },
        }

    risk_cap_qty = int(risk_result.get("suggested_size") or 0)
    risk_cap_lot_qty = (risk_cap_qty // 100) * 100
    planned_qty = max(0, int(planned_quantity))
    planned_lot_qty = (planned_qty // 100) * 100
    qty = risk_cap_lot_qty
    if planned_lot_qty > 0:
        qty = min(risk_cap_lot_qty if risk_cap_lot_qty > 0 else planned_lot_qty, planned_lot_qty)
    if qty <= 0:
        return {
            "risk_rejected": 1,
            "executed": 0,
            "execution_rejected": 0,
            "errors": 0,
            "daily_new_orders_delta": 0,
            "cash_spent": 0.0,
            "detail": {
                "symbol": symbol,
                "outcome": "risk_rejected",
                "reason": "size_zero",
                "entry_price": entry,
                "take_profit_price": tp,
                "stoploss_price": stoploss,
                "level_source": level_source,
                "planned_quantity": planned_lot_qty,
                "risk_cap_quantity": risk_cap_lot_qty,
            },
        }
    max_cash_lot_qty = int((safe_available_cash / entry) // 100) * 100 if entry > 0 else 0
    if qty > max_cash_lot_qty:
        qty = max_cash_lot_qty
    if qty <= 0:
        return {
            "risk_rejected": 1,
            "executed": 0,
            "execution_rejected": 0,
            "errors": 0,
            "daily_new_orders_delta": 0,
            "cash_spent": 0.0,
            "detail": {
                "symbol": symbol,
                "outcome": "risk_rejected",
                "reason": "insufficient_cash_after_risk_sizing",
                "entry_price": entry,
                "take_profit_price": tp,
                "stoploss_price": stoploss,
                "level_source": level_source,
                "available_cash": safe_available_cash,
                "planned_quantity": planned_lot_qty,
                "risk_cap_quantity": risk_cap_lot_qty,
            },
        }

    idempotency_key = f"short-term:{sid}"
    order_row = place_order(
        {
            "account_mode": account_mode,
            "symbol": symbol,
            "side": "BUY",
            "quantity": qty,
            "price": entry,
            "idempotency_key": idempotency_key,
            "auto_process": True,
            "metadata": {
                "source": "short_term_schedule_entry",
                "strategy_code": "SHORT_TERM",
                "entry_price": entry,
                "take_profit_price": tp,
                "stoploss_price": stoploss,
                "level_source": level_source,
            },
        }
    )
    resolved_order = order_row.get("order") if isinstance(order_row.get("order"), dict) else order_row
    status = str((resolved_order or {}).get("status", "")).upper()
    if status == "FILLED":
        filled_price = float((resolved_order or {}).get("price") or entry)
        cash_spent = max(0.0, float(qty) * filled_price)
        return {
            "risk_rejected": 0,
            "executed": 1,
            "execution_rejected": 0,
            "errors": 0,
            "daily_new_orders_delta": 1,
            "cash_spent": cash_spent,
            "detail": {
                "symbol": symbol,
                "outcome": "executed",
                "order_id": (resolved_order or {}).get("id"),
                "entry_price": entry,
                "take_profit_price": tp,
                "stoploss_price": stoploss,
                "level_source": level_source,
                "planned_quantity": planned_lot_qty,
                "risk_cap_quantity": risk_cap_lot_qty,
                "cash_spent": cash_spent,
            },
        }
    if status == "REJECTED":
        return {
            "risk_rejected": 0,
            "executed": 0,
            "execution_rejected": 1,
            "errors": 0,
            "daily_new_orders_delta": 0,
            "cash_spent": 0.0,
            "detail": {
                "symbol": symbol,
                "outcome": "execution_rejected",
                "reason": (resolved_order or {}).get("reason"),
                "entry_price": entry,
                "take_profit_price": tp,
                "stoploss_price": stoploss,
                "level_source": level_source,
                "planned_quantity": planned_lot_qty,
                "risk_cap_quantity": risk_cap_lot_qty,
            },
        }
    return {
        "risk_rejected": 0,
        "executed": 0,
        "execution_rejected": 1,
        "errors": 0,
        "daily_new_orders_delta": 0,
        "cash_spent": 0.0,
        "detail": {
            "symbol": symbol,
            "outcome": "execution_rejected",
            "reason": f"unexpected_order_status:{status}",
            "order_id": (resolved_order or {}).get("id"),
            "entry_price": entry,
            "take_profit_price": tp,
            "stoploss_price": stoploss,
            "level_source": level_source,
            "planned_quantity": planned_lot_qty,
            "risk_cap_quantity": risk_cap_lot_qty,
        },
    }


def _scan_batch_worker(limit_symbols: int, exchange_scope: str, fallback_light: bool, out_queue: Any) -> None:
    """Worker process: run scan batch and return result/error via queue."""
    try:
        if fallback_light:
            cap = int(settings.automation_short_term_scan_fallback_light_max_symbols)
            safe_limit = min(int(limit_symbols), cap) if int(limit_symbols) > 0 else 0
            batch = run_short_term_scan_batch_light(limit_symbols=safe_limit, exchange_scope=exchange_scope)
        else:
            batch = run_short_term_scan_batch(limit_symbols, exchange_scope=exchange_scope)
        out_queue.put({"ok": True, "batch": batch})
    except BaseException as exc:
        try:
            out_queue.put(
                {
                    "ok": False,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "traceback": traceback.format_exc(),
                }
            )
        except Exception:
            # Last-resort: worker is about to exit and queue pipe may be broken.
            pass


def _dispose_scan_worker_queue(out_queue: Any) -> None:
    """Always drain feeder threads for ctx.Queue on Windows spawn to avoid deadlocks on reuse."""
    try:
        out_queue.close()
        out_queue.join_thread()
    except Exception:
        logger.warning("scan_worker_queue_dispose_failed", exc_info=True)


def _run_short_term_scan_batch_with_timeout(
    limit_symbols: int,
    timeout_seconds: int,
    *,
    exchange_scope: str = "ALL",
    fallback_light: bool = False,
) -> dict[str, Any]:
    """
    Runs scan batch in isolated process with hard timeout.
    This prevents one stuck upstream API call from blocking scheduler lock forever.
    """
    safe_timeout = max(20, int(timeout_seconds))
    ctx = mp.get_context("spawn")
    out_queue = ctx.Queue(maxsize=1)
    proc = ctx.Process(
        target=_scan_batch_worker,
        args=(limit_symbols, str(exchange_scope).upper(), fallback_light, out_queue),
    )
    proc.start()
    proc.join(timeout=safe_timeout)

    if proc.is_alive():
        proc.terminate()
        proc.join(timeout=5)
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=2)
        _dispose_scan_worker_queue(out_queue)
        raise TimeoutError(f"short_term_scan_batch timed out after {safe_timeout}s")

    try:
        # get_nowait() is race-prone right after proc exit on Windows spawn.
        # Allow a short grace window for feeder thread flush.
        message = out_queue.get(timeout=3)
    except queue.Empty as exc:
        _dispose_scan_worker_queue(out_queue)
        exit_code = proc.exitcode
        raise RuntimeError(
            "short_term_scan_batch exited without result payload "
            f"(exit_code={exit_code}, possible_rate_limit_or_fatal_worker_exit)"
        ) from exc

    _dispose_scan_worker_queue(out_queue)

    if not isinstance(message, dict):
        raise RuntimeError("short_term_scan_batch returned invalid payload")

    if message.get("ok") is True:
        batch = message.get("batch")
        if isinstance(batch, dict):
            return batch
        raise RuntimeError("short_term_scan_batch payload missing batch dict")

    error_type = str(message.get("error_type") or "ScanError")
    error_text = str(message.get("error") or "unknown error")
    tb = str(message.get("traceback") or "").strip()
    if tb:
        logger.error("short_term_scan_worker_failed_traceback\n%s", tb)
    raise RuntimeError(f"{error_type}: {error_text}")


def _run_scan_with_timeout_fallback(
    *,
    run_id: UUID,
    limit_symbols: int,
    exchange_scope: str,
    timeout_seconds: int | None = None,
    fallback_timeout_seconds: int | None = None,
) -> dict[str, Any]:
    hard_timeout = (
        max(20, int(timeout_seconds))
        if timeout_seconds is not None
        else int(settings.automation_short_term_scan_timeout_seconds)
    )
    hard_fallback_timeout = (
        max(30, int(fallback_timeout_seconds))
        if fallback_timeout_seconds is not None
        else int(settings.automation_short_term_scan_fallback_light_timeout_seconds)
    )
    retry_attempts = max(0, int(settings.automation_short_term_scan_rate_limit_retry_attempts))
    retry_backoff = max(1.0, float(settings.automation_short_term_scan_rate_limit_retry_backoff_seconds))
    attempt = 0
    while True:
        attempt += 1
        try:
            return _run_short_term_scan_batch_with_timeout(
                limit_symbols=limit_symbols,
                timeout_seconds=hard_timeout,
                exchange_scope=exchange_scope,
            )
        except RuntimeError as exc:
            text = str(exc)
            lowered = text.lower()
            likely_rate_limit = "rate limit exceeded" in lowered or "gioi han api" in lowered
            if likely_rate_limit and attempt <= retry_attempts:
                delay_seconds = retry_backoff * attempt
                logger.warning(
                    "short_term_scan_rate_limit_retry",
                    extra={
                        "run_id": str(run_id),
                        "attempt": attempt,
                        "retry_attempts": retry_attempts,
                        "delay_seconds": delay_seconds,
                        "exchange_scope": str(exchange_scope).upper(),
                    },
                )
                time.sleep(delay_seconds)
                continue
            lowered = text.lower()
            likely_worker_exit = "exited without result payload" in lowered or "possible_rate_limit_or_fatal_worker_exit" in lowered
            if not likely_worker_exit and not likely_rate_limit:
                raise
            logger.warning(
                "short_term_scan_worker_exit_fallback_light",
                extra={
                    "run_id": str(run_id),
                    "limit_symbols": limit_symbols,
                    "exchange_scope": str(exchange_scope).upper(),
                    "error": text,
                    "rate_limited": likely_rate_limit,
                },
            )
            fallback_cap = int(settings.automation_short_term_scan_fallback_light_max_symbols)
            fallback_limit = fallback_cap if int(limit_symbols) <= 0 else min(int(limit_symbols), fallback_cap)
            return _run_short_term_scan_batch_with_timeout(
                limit_symbols=fallback_limit,
                timeout_seconds=hard_fallback_timeout,
                exchange_scope=exchange_scope,
                fallback_light=True,
            )
        except TimeoutError:
            break

    # full scan timed out: fallback to light
    try:
        logger.warning(
            "short_term_scan_timeout_fallback_light",
            extra={
                "run_id": str(run_id),
                "limit_symbols": limit_symbols,
                "exchange_scope": str(exchange_scope).upper(),
                "timeout_seconds": hard_timeout,
            },
        )
        fallback_cap = int(settings.automation_short_term_scan_fallback_light_max_symbols)
        # Unlimited mode is too expensive for fallback: cap aggressively to guarantee completion.
        fallback_limit = fallback_cap if int(limit_symbols) <= 0 else min(int(limit_symbols), fallback_cap)
        return _run_short_term_scan_batch_with_timeout(
            limit_symbols=fallback_limit,
            timeout_seconds=hard_fallback_timeout,
            exchange_scope=exchange_scope,
            fallback_light=True,
        )
    except Exception:
        raise


def ensure_short_term_automation_runs_table() -> None:
    """Aligns runtime with migration `005_create_short_term_automation_runs.sql`."""
    ddl = """
    CREATE TABLE IF NOT EXISTS short_term_automation_runs (
        id UUID PRIMARY KEY,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        run_status VARCHAR(32) NOT NULL,
        scanned INTEGER NOT NULL DEFAULT 0 CHECK (scanned >= 0),
        buy_candidates INTEGER NOT NULL DEFAULT 0 CHECK (buy_candidates >= 0),
        risk_rejected INTEGER NOT NULL DEFAULT 0 CHECK (risk_rejected >= 0),
        executed INTEGER NOT NULL DEFAULT 0 CHECK (executed >= 0),
        execution_rejected INTEGER NOT NULL DEFAULT 0 CHECK (execution_rejected >= 0),
        errors INTEGER NOT NULL DEFAULT 0 CHECK (errors >= 0),
        detail JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_short_term_automation_runs_started
    ON short_term_automation_runs(started_at DESC);
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _count_short_term_new_orders_today(account_mode: str) -> int:
    ensure_trading_core_tables()
    tz = settings.short_term_scan_timezone
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::int AS c
                FROM orders_core
                WHERE account_mode = %(mode)s
                  AND side = 'BUY'
                  AND COALESCE(order_metadata->>'source', '') = 'short_term_schedule_entry'
                  AND (created_at AT TIME ZONE %(tz)s)::date
                      = (NOW() AT TIME ZONE %(tz)s)::date
                """,
                {"mode": account_mode, "tz": tz},
            )
            row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _persist_run_row(
    *,
    run_id: UUID,
    started_at: datetime,
    finished_at: datetime,
    run_status: str,
    scanned: int,
    buy_candidates: int,
    risk_rejected: int,
    executed: int,
    execution_rejected: int,
    errors: int,
    detail: dict[str, Any],
) -> None:
    ensure_short_term_automation_runs_table()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO short_term_automation_runs (
                    id, started_at, finished_at, run_status,
                    scanned, buy_candidates, risk_rejected, executed, execution_rejected, errors, detail
                ) VALUES (
                    %(id)s, %(started_at)s, %(finished_at)s, %(run_status)s,
                    %(scanned)s, %(buy_candidates)s, %(risk_rejected)s, %(executed)s, %(execution_rejected)s, %(errors)s, %(detail)s
                )
                """,
                {
                    "id": run_id,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "run_status": run_status,
                    "scanned": scanned,
                    "buy_candidates": buy_candidates,
                    "risk_rejected": risk_rejected,
                    "executed": executed,
                    "execution_rejected": execution_rejected,
                    "errors": errors,
                    "detail": Json(detail),
                },
            )
        conn.commit()


def get_last_short_term_automation_run() -> Optional[dict[str, Any]]:
    ensure_short_term_automation_runs_table()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text AS id, started_at, finished_at, run_status,
                       scanned, buy_candidates, risk_rejected, executed, execution_rejected, errors, detail
                FROM short_term_automation_runs
                ORDER BY started_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
    return dict(row) if row else None


def list_recent_short_term_automation_runs(
    *,
    limit: int = 20,
    account_mode: str | None = None,
) -> list[dict[str, Any]]:
    ensure_short_term_automation_runs_table()
    safe_limit = max(1, min(limit, 200))
    mode = str(account_mode).upper() if account_mode else None
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            if mode in {"REAL", "DEMO"}:
                cur.execute(
                    """
                    SELECT id::text AS id, started_at, finished_at, run_status,
                           scanned, buy_candidates, risk_rejected, executed, execution_rejected, errors, detail
                    FROM short_term_automation_runs
                    WHERE detail->>'account_mode' = %(account_mode)s
                    ORDER BY started_at DESC
                    LIMIT %(limit)s
                    """,
                    {"account_mode": mode, "limit": safe_limit},
                )
            else:
                cur.execute(
                    """
                    SELECT id::text AS id, started_at, finished_at, run_status,
                           scanned, buy_candidates, risk_rejected, executed, execution_rejected, errors, detail
                    FROM short_term_automation_runs
                    ORDER BY started_at DESC
                    LIMIT %(limit)s
                    """,
                    {"limit": safe_limit},
                )
            rows = cur.fetchall()
    return [dict(row) for row in rows]


def run_short_term_production_cycle(
    *,
    limit_symbols: int = 0,
    exchange_scope: str = "ALL",
    account_mode: str = "DEMO",
    nav: float = 100_000_000.0,
    risk_per_trade: float = 0.01,
    max_daily_new_orders: int = 10,
    enforce_vn_scan_schedule: bool = False,
    scheduler_trigger_batch_id: str | None = None,
    scheduler_sequence_index: int | None = None,
    scheduler_sequence_total: int | None = None,
    manual_trigger_id: str | None = None,
    demo_session_id: str | None = None,
) -> dict[str, Any]:
    """
    One full cycle under a single DB advisory lock.

    When `enforce_vn_scan_schedule` is True, skips unless the current instant lies on the
    configured interval grid within VN regular sessions (weekdays; configured market holidays excluded).
    """
    normalized_account_mode = str(account_mode or "").strip().upper()
    started_at = _utc_now()
    run_id = uuid4()
    base_detail: dict[str, Any] = {
        "account_mode": normalized_account_mode,
        "limit_symbols": limit_symbols,
        "exchange_scope": str(exchange_scope).upper(),
        "enforce_vn_scan_schedule": enforce_vn_scan_schedule,
    }
    if scheduler_trigger_batch_id:
        base_detail["scheduler_trigger_batch_id"] = str(scheduler_trigger_batch_id)
    if scheduler_sequence_index is not None:
        base_detail["scheduler_sequence_index"] = int(scheduler_sequence_index)
    if scheduler_sequence_total is not None:
        base_detail["scheduler_sequence_total"] = int(scheduler_sequence_total)
    if manual_trigger_id:
        base_detail["manual_trigger_id"] = str(manual_trigger_id)
    if normalized_account_mode == "DEMO" and demo_session_id:
        base_detail["demo_session_id"] = str(demo_session_id).strip()
    advisory_lock_key = _resolve_advisory_lock_key(normalized_account_mode)
    logger.warning(
        "short_term_cycle_started",
        extra={
            "run_id": str(run_id),
            "account_mode": normalized_account_mode,
            "limit_symbols": limit_symbols,
            "exchange_scope": str(exchange_scope).upper(),
            "risk_per_trade": risk_per_trade,
            "max_daily_new_orders": max_daily_new_orders,
            "enforce_vn_scan_schedule": enforce_vn_scan_schedule,
        },
    )

    lock_conn = connect(settings.database_url)
    try:
        with lock_conn.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(%(k1)s, %(k2)s) AS locked",
                {"k1": _ADVISORY_LOCK_SPACE, "k2": advisory_lock_key},
            )
            locked_row = cur.fetchone()
        # Advisory lock is session-scoped, so we can commit immediately to avoid
        # leaving an open transaction in "idle in transaction" state.
        lock_conn.commit()
        acquired = bool(locked_row and locked_row[0])
        if not acquired:
            finished_at = _utc_now()
            detail = {**base_detail, "skip_reason": "advisory_lock_busy"}
            _persist_run_row(
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                run_status="SKIPPED_LOCK",
                scanned=0,
                buy_candidates=0,
                risk_rejected=0,
                executed=0,
                execution_rejected=0,
                errors=0,
                detail=detail,
            )
            logger.info("short_term_cycle_skipped_lock", extra={"run_id": str(run_id)})
            return {
                "success": True,
                "run_id": str(run_id),
                "run_status": "SKIPPED_LOCK",
                "scanned": 0,
                "buy_candidates": 0,
                "risk_rejected": 0,
                "executed": 0,
                "execution_rejected": 0,
                "errors": 0,
                "detail": detail,
            }

        try:
            if enforce_vn_scan_schedule:
                batch_continuation = bool(
                    scheduler_trigger_batch_id
                    and scheduler_sequence_index is not None
                    and int(scheduler_sequence_index) > 1
                )
                try:
                    on_grid = is_now_on_short_term_scan_grid(
                        settings.short_term_scan_interval_minutes,
                        settings.short_term_scan_timezone,
                        holiday_dates=get_vn_market_holiday_dates(),
                    )
                except ValueError as exc:
                    finished_at = _utc_now()
                    detail = {**base_detail, "error": f"schedule_config: {exc}"}
                    _persist_run_row(
                        run_id=run_id,
                        started_at=started_at,
                        finished_at=finished_at,
                        run_status="FAILED",
                        scanned=0,
                        buy_candidates=0,
                        risk_rejected=0,
                        executed=0,
                        execution_rejected=0,
                        errors=1,
                        detail=detail,
                    )
                    return {
                        "success": False,
                        "run_id": str(run_id),
                        "run_status": "FAILED",
                        "scanned": 0,
                        "buy_candidates": 0,
                        "risk_rejected": 0,
                        "executed": 0,
                        "execution_rejected": 0,
                        "errors": 1,
                        "detail": detail,
                    }
                if not batch_continuation and not on_grid:
                    finished_at = _utc_now()
                    detail = {**base_detail, "skip_reason": "outside_vn_scan_slot"}
                    _persist_run_row(
                        run_id=run_id,
                        started_at=started_at,
                        finished_at=finished_at,
                        run_status="SKIPPED_SCHEDULE",
                        scanned=0,
                        buy_candidates=0,
                        risk_rejected=0,
                        executed=0,
                        execution_rejected=0,
                        errors=0,
                        detail=detail,
                    )
                    logger.info("short_term_cycle_skipped_schedule", extra={"run_id": str(run_id)})
                    return {
                        "success": True,
                        "run_id": str(run_id),
                        "run_status": "SKIPPED_SCHEDULE",
                        "scanned": 0,
                        "buy_candidates": 0,
                        "risk_rejected": 0,
                        "executed": 0,
                        "execution_rejected": 0,
                        "errors": 0,
                        "detail": detail,
                    }

            try:
                normalized_scope = str(exchange_scope or "ALL").strip().upper()
                if normalized_scope == "ALL":
                    scope_batches: list[dict[str, Any]] = []
                    scope_summary: list[dict[str, Any]] = []
                    phased_scopes = ("HOSE", "HNX", "UPCOM")
                    cooldown_seconds = max(
                        0.0,
                        float(settings.automation_short_term_scan_exchange_cooldown_seconds),
                    )
                    phase_timeout = max(
                        20,
                        min(
                            int(settings.automation_short_term_scan_timeout_seconds),
                            int(settings.automation_short_term_scan_all_phase_timeout_seconds),
                        ),
                    )
                    phase_fallback_timeout = max(
                        30,
                        min(
                            int(settings.automation_short_term_scan_fallback_light_timeout_seconds),
                            int(settings.automation_short_term_scan_all_phase_timeout_seconds),
                        ),
                    )
                    for idx, scope_name in enumerate(phased_scopes):
                        try:
                            scoped_batch = _run_scan_with_timeout_fallback(
                                run_id=run_id,
                                limit_symbols=limit_symbols,
                                exchange_scope=scope_name,
                                timeout_seconds=phase_timeout,
                                fallback_timeout_seconds=phase_fallback_timeout,
                            )
                            scope_batches.append(scoped_batch)
                            scope_summary.append(
                                {
                                    "exchange_scope": scope_name,
                                    "scan_mode": str(scoped_batch.get("scan_mode") or "full"),
                                    "scanned": int(scoped_batch.get("scanned") or 0),
                                    "skipped_insufficient_data": int(
                                        scoped_batch.get("skipped_insufficient_data") or 0
                                    ),
                                }
                            )
                        except Exception as scoped_exc:
                            if _is_scanner_universe_empty_error(scoped_exc):
                                logger.warning(
                                    "short_term_scan_scope_skipped_empty_universe",
                                    extra={"run_id": str(run_id), "exchange_scope": scope_name},
                                )
                                scope_summary.append(
                                    {
                                        "exchange_scope": scope_name,
                                        "scan_mode": "skipped_empty_universe",
                                        "scanned": 0,
                                        "skipped_insufficient_data": 0,
                                    }
                                )
                                continue
                            raise
                        if idx < len(phased_scopes) - 1 and cooldown_seconds > 0:
                            time.sleep(cooldown_seconds)

                    merged_signals: list[dict[str, Any]] = []
                    merged_scanned = 0
                    merged_skipped = 0
                    seen_signal_ids: set[str] = set()
                    for scoped in scope_batches:
                        merged_scanned += int(scoped.get("scanned") or 0)
                        merged_skipped += int(scoped.get("skipped_insufficient_data") or 0)
                        for sig in list(scoped.get("signals") or []):
                            sid = str(sig.get("id") or "")
                            if sid and sid in seen_signal_ids:
                                continue
                            if sid:
                                seen_signal_ids.add(sid)
                            merged_signals.append(sig)

                    batch = {
                        "signals": merged_signals,
                        "scanned": merged_scanned,
                        "skipped_insufficient_data": merged_skipped,
                        "exchange_scope": "ALL",
                        "scan_mode": "phased_by_exchange",
                        "exchange_scope_summary": scope_summary,
                    }
                else:
                    scoped_timeout = int(settings.automation_short_term_scan_timeout_seconds)
                    scoped_fallback_timeout = int(settings.automation_short_term_scan_fallback_light_timeout_seconds)
                    # Single-exchange runs should fail fast enough for operations visibility.
                    if int(limit_symbols) > 0:
                        scoped_timeout = min(scoped_timeout, 180)
                        scoped_fallback_timeout = min(scoped_fallback_timeout, 120)
                    batch = _run_scan_with_timeout_fallback(
                        run_id=run_id,
                        limit_symbols=limit_symbols,
                        exchange_scope=normalized_scope,
                        timeout_seconds=scoped_timeout,
                        fallback_timeout_seconds=scoped_fallback_timeout,
                    )
            except Exception as exc:
                if _is_scanner_universe_empty_error(exc):
                    finished_at = _utc_now()
                    detail = {
                        **base_detail,
                        "skip_reason": "scope_empty_universe",
                        "scanner_error": str(exc),
                    }
                    _persist_run_row(
                        run_id=run_id,
                        started_at=started_at,
                        finished_at=finished_at,
                        run_status="SKIPPED_SCOPE_EMPTY",
                        scanned=0,
                        buy_candidates=0,
                        risk_rejected=0,
                        executed=0,
                        execution_rejected=0,
                        errors=0,
                        detail=detail,
                    )
                    logger.info(
                        "short_term_cycle_skipped_scope_empty",
                        extra={"run_id": str(run_id), "exchange_scope": str(exchange_scope).upper()},
                    )
                    return {
                        "success": True,
                        "run_id": str(run_id),
                        "run_status": "SKIPPED_SCOPE_EMPTY",
                        "scanned": 0,
                        "buy_candidates": 0,
                        "risk_rejected": 0,
                        "executed": 0,
                        "execution_rejected": 0,
                        "errors": 0,
                        "detail": detail,
                    }
                logger.exception("short_term_scan_failed", extra={"run_id": str(run_id)})
                finished_at = _utc_now()
                detail = {**base_detail, "scanner_error": str(exc)}
                _persist_run_row(
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=finished_at,
                    run_status="FAILED",
                    scanned=0,
                    buy_candidates=0,
                    risk_rejected=0,
                    executed=0,
                    execution_rejected=0,
                    errors=1,
                    detail=detail,
                )
                return {
                    "success": False,
                    "run_id": str(run_id),
                    "run_status": "FAILED",
                    "scanned": 0,
                    "buy_candidates": 0,
                    "risk_rejected": 0,
                    "executed": 0,
                    "execution_rejected": 0,
                    "errors": 1,
                    "detail": detail,
                }

            signals: list[dict[str, Any]] = list(batch.get("signals") or [])
            scanned = int(batch.get("scanned") or 0)
            risk_rejected = 0
            executed = 0
            execution_rejected = 0
            errors = 0
            executions_detail: list[dict[str, Any]] = []
            buy_step_timeout_seconds = max(
                5,
                int(settings.automation_short_term_buy_step_timeout_seconds),
            )

            daily_new_orders = _count_short_term_new_orders_today(account_mode)
            sell_candidates = _build_sell_candidates_from_positions(account_mode)
            remaining_cash_runtime = max(0.0, float(nav))
            buy_rows_all = [s for s in signals if str(s.get("action", "")).upper() == "BUY"]
            sell_trigger_symbols = {str(row.get("symbol") or "").strip().upper() for row in sell_candidates}
            buy_rows_filtered = [
                row for row in buy_rows_all if str(row.get("symbol") or "").strip().upper() not in sell_trigger_symbols
            ]
            strategy_remaining_cash = float(remaining_cash_runtime)
            buy_rows_decided, planned_qty_by_signal_id, decision_meta = _apply_claude_buy_decision(
                buy_rows=buy_rows_filtered,
                strategy_remaining_cash=strategy_remaining_cash,
                lot_size=100,
            )
            buy_rows: list[dict[str, Any]] = []
            for row in buy_rows_decided:
                sid = str(row.get("id") or "")
                symbol = str(row.get("symbol") or "").strip().upper()
                entry_price = float(row.get("entry_price") or 0.0)
                planned_qty = int(planned_qty_by_signal_id.get(sid, 0))
                if entry_price <= 0 or planned_qty < 100:
                    executions_detail.append(
                        {
                            "symbol": symbol,
                            "side": "BUY",
                            "outcome": "skipped",
                            "reason": "insufficient_cash_for_lot100",
                            "entry_price": entry_price,
                            "planned_quantity": planned_qty,
                        }
                    )
                    continue
                buy_rows.append(row)
            buy_candidates = len(buy_rows)
            planned_spent_total = 0.0
            for row in buy_rows:
                sid = str(row.get("id") or "")
                entry_price = float(row.get("entry_price") or 0.0)
                planned_qty = int(planned_qty_by_signal_id.get(sid, 0))
                if entry_price > 0 and planned_qty > 0:
                    planned_spent_total += float(entry_price) * float(planned_qty)
            actual_buy_cash_spent = 0.0

            for sell_candidate in sell_candidates:
                try:
                    one = _handle_one_sell_candidate(
                        candidate=sell_candidate,
                        account_mode=account_mode,
                        run_id=run_id,
                    )
                    executed += int(one.get("executed") or 0)
                    execution_rejected += int(one.get("execution_rejected") or 0)
                    errors += int(one.get("errors") or 0)
                    detail_row = one.get("detail")
                    if isinstance(detail_row, dict):
                        executions_detail.append(detail_row)
                except Exception as exc:
                    errors += 1
                    execution_rejected += 1
                    executions_detail.append(
                        {
                            "symbol": str(sell_candidate.get("symbol") or "").strip().upper(),
                            "side": "SELL",
                            "outcome": "error",
                            "error": str(exc),
                        }
                    )

            for sig in buy_rows:
                symbol = str(sig.get("symbol", "")).strip().upper()
                try:
                    result_q: queue.Queue = queue.Queue(maxsize=1)

                    def _buy_worker() -> None:
                        try:
                            result_q.put(
                                (
                                    "ok",
                                    _handle_one_buy_signal(
                                        sig=sig,
                                        account_mode=account_mode,
                                        available_cash=float(remaining_cash_runtime),
                                        risk_per_trade=float(risk_per_trade),
                                        max_daily_new_orders=int(max_daily_new_orders),
                                        daily_new_orders=int(daily_new_orders),
                                        planned_quantity=int(planned_qty_by_signal_id.get(str(sig.get("id") or ""), 0)),
                                    ),
                                )
                            )
                        except Exception as worker_exc:
                            result_q.put(("err", worker_exc))

                    threading.Thread(
                        target=_buy_worker,
                        name=f"short-term-buy-step-{symbol or 'unknown'}",
                        daemon=True,
                    ).start()
                    kind, payload = result_q.get(timeout=buy_step_timeout_seconds)
                    if kind == "err":
                        raise payload
                    one = payload
                    risk_rejected += int(one.get("risk_rejected") or 0)
                    executed += int(one.get("executed") or 0)
                    execution_rejected += int(one.get("execution_rejected") or 0)
                    errors += int(one.get("errors") or 0)
                    daily_new_orders += int(one.get("daily_new_orders_delta") or 0)
                    actual_buy_cash_spent += float(one.get("cash_spent") or 0.0)
                    remaining_cash_runtime = max(
                        0.0,
                        float(remaining_cash_runtime) - float(one.get("cash_spent") or 0.0),
                    )
                    detail_row = one.get("detail")
                    if isinstance(detail_row, dict):
                        detail_row["remaining_cash_after_order"] = round(float(remaining_cash_runtime), 6)
                        executions_detail.append(detail_row)
                    else:
                        executions_detail.append({"symbol": symbol, "outcome": "error", "error": "invalid_result"})
                except queue.Empty:
                    errors += 1
                    execution_rejected += 1
                    logger.warning(
                        "short_term_buy_handling_timeout",
                        extra={
                            "run_id": str(run_id),
                            "symbol": symbol,
                            "timeout_seconds": buy_step_timeout_seconds,
                        },
                    )
                    executions_detail.append(
                        {
                            "symbol": symbol,
                            "outcome": "error",
                            "error": f"buy_step_timeout:{buy_step_timeout_seconds}s",
                        }
                    )
                except Exception as exc:
                    errors += 1
                    logger.warning(
                        "short_term_buy_handling_failed",
                        extra={"run_id": str(run_id), "symbol": symbol, "error": str(exc)},
                    )
                    executions_detail.append({"symbol": symbol, "outcome": "error", "error": str(exc)})

            finished_at = _utc_now()
            decision_meta_enriched = dict(decision_meta or {})
            decision_meta_enriched["planned_spent"] = round(float(planned_spent_total), 2)
            decision_meta_enriched["actual_spent"] = round(float(actual_buy_cash_spent), 2)
            decision_meta_enriched["initial_strategy_cash"] = round(float(nav), 2)
            decision_meta_enriched["remaining_cash_after_cycle"] = round(float(remaining_cash_runtime), 2)
            decision_meta_enriched["execution_spent_gap"] = round(
                float(planned_spent_total) - float(actual_buy_cash_spent),
                2,
            )
            detail = {
                **base_detail,
                "skipped_insufficient_data": int(batch.get("skipped_insufficient_data") or 0),
                "scan_mode": str(batch.get("scan_mode") or "full"),
                "executions": executions_detail,
                "allocation_method": "claude_decision_with_score_fallback",
                "allocation_nav_total": float(nav),
                "remaining_cash_after_cycle": round(float(remaining_cash_runtime), 6),
                "buy_decision_meta": decision_meta_enriched,
                "sell_candidates": len(sell_candidates),
            }
            for obs_key in (
                "listing_target_symbols",
                "listing_candidates_total",
                "listing_per_exchange_cap",
                "exchange_scope_summary",
                "skipped_low_liquidity",
                "skipped_no_volume_spike",
                "experience_threshold_source_claude",
                "experience_threshold_source_heuristic",
            ):
                if obs_key in batch and batch.get(obs_key) is not None:
                    detail[obs_key] = batch.get(obs_key)
            run_status = "COMPLETED" if errors == 0 else "PARTIAL"
            _persist_run_row(
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                run_status=run_status,
                scanned=scanned,
                buy_candidates=buy_candidates,
                risk_rejected=risk_rejected,
                executed=executed,
                execution_rejected=execution_rejected,
                errors=errors,
                detail=detail,
            )
            logger.warning(
                "short_term_cycle_finished",
                extra={
                    "run_id": str(run_id),
                    "run_status": run_status,
                    "account_mode": account_mode,
                    "scanned": scanned,
                    "buy_candidates": buy_candidates,
                    "risk_rejected": risk_rejected,
                    "executed": executed,
                    "execution_rejected": execution_rejected,
                    "errors": errors,
                },
            )
            return {
                "success": errors == 0,
                "run_id": str(run_id),
                "run_status": run_status,
                "scanned": scanned,
                "buy_candidates": buy_candidates,
                "risk_rejected": risk_rejected,
                "executed": executed,
                "execution_rejected": execution_rejected,
                "errors": errors,
                "detail": detail,
            }
        finally:
            with lock_conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_unlock(%(k1)s, %(k2)s) AS unlocked",
                            {"k1": _ADVISORY_LOCK_SPACE, "k2": advisory_lock_key},
                )
            lock_conn.commit()
    finally:
        lock_conn.close()


def get_short_term_async_job(job_id: str) -> dict[str, Any] | None:
    safe_id = str(job_id or "").strip()
    if not safe_id:
        return None
    with _ASYNC_RUNS_LOCK:
        job = _ASYNC_RUNS.get(safe_id)
        return dict(job) if isinstance(job, dict) else None


def launch_short_term_production_cycle_async(
    *,
    limit_symbols: int,
    exchange_scope: str,
    account_mode: str,
    nav: float,
    risk_per_trade: float,
    max_daily_new_orders: int,
    enforce_vn_scan_schedule: bool,
    demo_session_id: str | None = None,
) -> dict[str, Any]:
    """
    Launches one short-term production cycle in a background thread and returns immediately.
    This is used for heavy manual runs (notably ALL scope) to avoid request timeout at FE/BE edge.
    """
    manual_trigger_id = str(uuid4())
    started_at = _utc_now().isoformat()
    with _ASYNC_RUNS_LOCK:
        _ASYNC_RUNS[manual_trigger_id] = {
            "job_id": manual_trigger_id,
            "status": "QUEUED",
            "started_at": started_at,
            "finished_at": None,
            "result": None,
            "error": None,
            "exchange_scope": str(exchange_scope).upper(),
            "account_mode": str(account_mode).upper(),
            "limit_symbols": int(limit_symbols),
        }

    def _worker() -> None:
        with _ASYNC_RUNS_LOCK:
            if manual_trigger_id in _ASYNC_RUNS:
                _ASYNC_RUNS[manual_trigger_id]["status"] = "RUNNING"
        try:
            result = run_short_term_production_cycle(
                limit_symbols=limit_symbols,
                exchange_scope=exchange_scope,
                account_mode=account_mode,
                nav=nav,
                risk_per_trade=risk_per_trade,
                max_daily_new_orders=max_daily_new_orders,
                enforce_vn_scan_schedule=enforce_vn_scan_schedule,
                manual_trigger_id=manual_trigger_id,
                demo_session_id=demo_session_id,
            )
            with _ASYNC_RUNS_LOCK:
                if manual_trigger_id in _ASYNC_RUNS:
                    _ASYNC_RUNS[manual_trigger_id]["status"] = "FINISHED"
                    _ASYNC_RUNS[manual_trigger_id]["finished_at"] = _utc_now().isoformat()
                    _ASYNC_RUNS[manual_trigger_id]["result"] = result
        except Exception as exc:
            logger.exception("short_term_async_cycle_failed", extra={"manual_trigger_id": manual_trigger_id})
            with _ASYNC_RUNS_LOCK:
                if manual_trigger_id in _ASYNC_RUNS:
                    _ASYNC_RUNS[manual_trigger_id]["status"] = "FAILED"
                    _ASYNC_RUNS[manual_trigger_id]["finished_at"] = _utc_now().isoformat()
                    _ASYNC_RUNS[manual_trigger_id]["error"] = str(exc)

    threading.Thread(
        target=_worker,
        name=f"short-term-manual-{manual_trigger_id[:8]}",
        daemon=True,
    ).start()
    return {
        "success": True,
        "run_id": manual_trigger_id,
        "run_status": "ACCEPTED",
        "scanned": 0,
        "buy_candidates": 0,
        "risk_rejected": 0,
        "executed": 0,
        "execution_rejected": 0,
        "errors": 0,
        "detail": {
            "mode": "async",
            "manual_trigger_id": manual_trigger_id,
            "exchange_scope": str(exchange_scope).upper(),
            "account_mode": str(account_mode).upper(),
            "limit_symbols": int(limit_symbols),
            "demo_session_id": str(demo_session_id).strip() if demo_session_id else None,
            "message": "Cycle queued in background. Poll /automation/short-term/runs or /automation/short-term/async-job/{job_id}.",
        },
    }
