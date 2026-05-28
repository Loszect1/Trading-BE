from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.services.mail_signal_scheduler_service import run_mail_signal_pipeline
from app.services.real_recommendation_service import (
    build_recommendation_from_signal,
    normalize_real_recommendation_rows,
)
from app.services.redis_cache import RedisCacheService
from app.services.short_term_automation_service import run_short_term_scan_batch_resilient
from app.services.signal_engine_service import extract_short_term_scan_diagnostics, get_cached_symbol_liquidity_status

logger = logging.getLogger(__name__)

REAL_RECOMMENDATIONS_REDIS_KEY = "signals:real:short-term:recommendations:latest"
REAL_RECOMMENDATIONS_REDIS_LOG_PREFIX = "signals:real:short-term:recommendations:run:"
REAL_RECOMMENDATIONS_TTL_SECONDS = 86_400

_redis_cache = RedisCacheService()


def _real_recommendations_run_key(now_local: datetime | None = None) -> str:
    ts = now_local or datetime.now(tz=ZoneInfo(settings.short_term_scan_timezone))
    return f"{REAL_RECOMMENDATIONS_REDIS_LOG_PREFIX}{ts.strftime('%Y%m%d-%H%M%S')}"


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def _event_name(source: str, suffix: str) -> str:
    if str(source or "").lower() == "scheduler":
        return f"real_scan_only_{suffix}"
    return f"automation.real_recommendations_{suffix}"


def persist_real_recommendations_payload(payload: dict[str, Any]) -> str:
    run_key = _real_recommendations_run_key()
    _redis_cache.set_json(REAL_RECOMMENDATIONS_REDIS_KEY, payload, ttl_seconds=REAL_RECOMMENDATIONS_TTL_SECONDS)
    _redis_cache.set_json(run_key, payload, ttl_seconds=REAL_RECOMMENDATIONS_TTL_SECONDS)
    return run_key


def extract_mail_signal_recommendations(mail_out: Any) -> list[dict[str, Any]]:
    if not isinstance(mail_out, dict):
        return []
    raw_items = mail_out.get("items")
    if not isinstance(raw_items, list):
        return []
    rows: list[dict[str, Any]] = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        confidence = _to_float(row.get("confidence"))
        if confidence is not None and 0.0 <= confidence <= 1.0:
            row["confidence"] = confidence * 100.0
        row.setdefault("source_strategy", "MAIL_SIGNAL")
        symbol = str(row.get("symbol") or "").strip().upper()
        liquidity = get_cached_symbol_liquidity_status(symbol)
        if not bool(liquidity.get("eligible_liquidity", False)):
            logger.warning(
                "real_recommendations_mail_signal_filtered_low_liquidity",
                extra={"symbol": symbol, "reason": liquidity.get("reason")},
            )
            continue
        row["liquidity"] = liquidity
        rows.append(row)
    return normalize_real_recommendation_rows(rows)


def _watch_reason(scan_reason: str) -> str:
    if scan_reason == "dynamic_buy_floor":
        return "Passed entry gate but composite score is below dynamic BUY floor."
    if scan_reason == "news_spike_risk":
        return "News-spike setup kept on watch until R/R and extension risk improve."
    return "Liquidity/spike/data passed, but entry gate still needs confirmation."


def build_watch_candidates_from_scan(scan_result: dict[str, Any], *, limit: int = 40) -> list[dict[str, Any]]:
    rejected = scan_result.get("rejected_candidates")
    if not isinstance(rejected, list):
        return []
    allowed_reasons = {"entry_gate_failed", "dynamic_buy_floor", "news_spike_risk"}
    priority = {"dynamic_buy_floor": 0, "news_spike_risk": 1, "entry_gate_failed": 2}
    rows: list[dict[str, Any]] = []
    for raw in rejected:
        if not isinstance(raw, dict):
            continue
        scan_reason = str(raw.get("reason") or "").strip()
        if scan_reason not in allowed_reasons:
            continue
        symbol = str(raw.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        detail = raw.get("detail") if isinstance(raw.get("detail"), dict) else {}
        levels = detail.get("trade_levels") if isinstance(detail.get("trade_levels"), dict) else {}
        row: dict[str, Any] = {
            "symbol": symbol,
            "exchange": str(raw.get("exchange") or "").strip().upper(),
            "watch_status": "WATCH",
            "scan_reason": scan_reason,
            "reason": _watch_reason(scan_reason),
            "detail": detail,
        }
        setup_type = detail.get("setup_type")
        if setup_type is not None:
            row["setup_type"] = setup_type
        for metric in (
            "momentum_5d_pct",
            "rsi14",
            "distance_from_ema20_pct",
            "spike_ratio",
            "composite",
            "floor",
            "daily_return_pct",
        ):
            if metric in detail:
                row[metric] = detail.get(metric)
        if levels:
            for source_key, target_key in (
                ("entry", "entry"),
                ("take_profit", "take_profit"),
                ("stop_loss", "stop_loss"),
                ("target_rr", "reward_risk"),
            ):
                if levels.get(source_key) is not None:
                    row[target_key] = levels.get(source_key)
        rows.append(row)
    rows.sort(key=lambda item: priority.get(str(item.get("scan_reason") or ""), 99))
    return rows[: max(0, int(limit))]


def _scan_only_recommendation_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    recommendations: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for item in normalize_real_recommendation_rows(rows):
        liquidity = item.get("liquidity") if isinstance(item.get("liquidity"), dict) else {}
        if liquidity and not bool(liquidity.get("eligible_liquidity", True)):
            rejected.append(
                {
                    **item,
                    "risk_status": "REJECTED",
                    "risk_reason": str(liquidity.get("reason") or "low_or_irregular_liquidity"),
                    "risk_result": {
                        "pass": False,
                        "reason": str(liquidity.get("reason") or "low_or_irregular_liquidity"),
                        "liquidity": liquidity,
                    },
                    "suggested_quantity": 0,
                    "suggested_notional": 0.0,
                }
            )
            continue
        entry = float(item["entry"])
        take_profit = float(item["take_profit"])
        stop_loss = float(item["stop_loss"])
        risk_per_share = entry - stop_loss
        reward_per_share = take_profit - entry
        if entry <= 0 or risk_per_share <= 0 or reward_per_share <= 0:
            rejected.append(
                {
                    **item,
                    "risk_status": "REJECTED",
                    "risk_reason": "invalid_trade_geometry",
                    "risk_result": {"pass": False, "reason": "invalid_trade_geometry"},
                    "suggested_quantity": 0,
                    "suggested_notional": 0.0,
                }
            )
            continue
        reward_risk = reward_per_share / risk_per_share
        recommendations.append(
            {
                **item,
                "reward_risk": round(reward_risk, 4),
                "risk_status": "RECOMMENDATION_ONLY",
                "risk_reason": "cash_preflight_skipped",
                "risk_result": {
                    "pass": True,
                    "reason": "scan_only_recommendation",
                    "cash_preflight": "skipped",
                },
                "account_preflight": {
                    "available": False,
                    "source": "scan_only",
                    "cash_preflight": "skipped",
                    "reason": "scan_only_does_not_size_or_fetch_cash",
                },
                "suggested_quantity": 0,
                "suggested_notional": 0.0,
            }
        )
    return recommendations, rejected


def normalize_real_recommendations_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    row = payload if isinstance(payload, dict) else {}
    recommendations = normalize_real_recommendation_rows(
        [item for item in row.get("recommendations") or [] if isinstance(item, dict)]
    )
    short_term_recommendations = normalize_real_recommendation_rows(
        [item for item in row.get("short_term_recommendations") or recommendations if isinstance(item, dict)]
    )
    rejected_recommendations = normalize_real_recommendation_rows(
        [item for item in row.get("rejected_recommendations") or [] if isinstance(item, dict)]
    )
    mail_signal_recommendations = normalize_real_recommendation_rows(
        [item for item in row.get("mail_signal_recommendations") or [] if isinstance(item, dict)]
    )
    all_short_term_recommendations = normalize_real_recommendation_rows(
        [
            item
            for item in row.get("all_short_term_recommendations") or short_term_recommendations
            if isinstance(item, dict)
        ]
    )
    watch_candidates = [dict(item) for item in row.get("watch_candidates") or [] if isinstance(item, dict)]
    diag = row.get("scan_diagnostics")
    if not isinstance(diag, dict):
        diag = {}
    return {
        "generated_at": row.get("generated_at"),
        "exchange_scope": row.get("exchange_scope"),
        "limit_symbols": row.get("limit_symbols"),
        "scanned": int(row.get("scanned") or 0),
        "recommendations": short_term_recommendations,
        "count": len(short_term_recommendations),
        "short_term_recommendations": short_term_recommendations,
        "short_term_count": len(short_term_recommendations),
        "all_short_term_recommendations": all_short_term_recommendations,
        "rejected_recommendations": rejected_recommendations,
        "rejected_count": len(rejected_recommendations),
        "watch_candidates": watch_candidates,
        "watch_count": len(watch_candidates),
        "mail_signal_recommendations": mail_signal_recommendations,
        "mail_signal_count": len(mail_signal_recommendations),
        "scan_diagnostics": diag,
    }


def get_latest_real_recommendations_payload() -> dict[str, Any]:
    return normalize_real_recommendations_payload(_redis_cache.get_json(REAL_RECOMMENDATIONS_REDIS_KEY) or {})


def list_recent_real_recommendations_payloads(limit: int = 10) -> list[dict[str, Any]]:
    keys = _redis_cache.scan_keys(f"{REAL_RECOMMENDATIONS_REDIS_LOG_PREFIX}*", limit=5000)
    runs: list[dict[str, Any]] = []
    for key in sorted(keys, reverse=True):
        payload = _redis_cache.get_json(key)
        if not isinstance(payload, dict):
            continue
        runs.append({"redis_key": key, **normalize_real_recommendations_payload(payload)})
        if len(runs) >= int(limit):
            break
    return runs


def run_real_recommendations_scan(
    *,
    exchange_scope: str = "ALL",
    limit_symbols: int = 0,
    available_cash_vnd: float | None = None,
    slot_marker: str | None = None,
    source: str = "manual",
    persist: bool = True,
) -> dict[str, Any]:
    normalized_scope = str(exchange_scope or "ALL").strip().upper()
    safe_limit = int(limit_symbols or 0)
    logger.warning(
        _event_name(source, "scan_started"),
        extra={
            "slot_marker": slot_marker,
            "exchange_scope": normalized_scope,
            "limit_symbols": safe_limit,
            "has_request_cash": available_cash_vnd is not None,
        },
    )

    mail_recommendations: list[dict[str, Any]] = []
    try:
        mail_out = run_mail_signal_pipeline()
        mail_recommendations = extract_mail_signal_recommendations(mail_out)
        logger.warning(
            _event_name(source, "mail_pipeline_completed"),
            extra={
                "slot_marker": slot_marker,
                "mail_success": bool(mail_out.get("success")) if isinstance(mail_out, dict) else False,
                "mail_count": int(mail_out.get("mail_count") or 0) if isinstance(mail_out, dict) else 0,
                "items": len(mail_out.get("items") or []) if isinstance(mail_out, dict) and isinstance(mail_out.get("items"), list) else 0,
            },
        )
    except Exception as exc:
        logger.warning(
            _event_name(source, "mail_pipeline_failed"),
            extra={"slot_marker": slot_marker, "error": str(exc)},
        )

    scan_result = run_short_term_scan_batch_resilient(
        run_id=uuid4(),
        limit_symbols=safe_limit,
        exchange_scope=normalized_scope,
    )
    raw_recommendations: list[dict[str, Any]] = []
    for signal in scan_result.get("signals") or []:
        if not isinstance(signal, dict) or str(signal.get("action") or "").upper() != "BUY":
            continue
        raw_recommendations.append(build_recommendation_from_signal(signal))

    recommendations, rejected_recommendations = _scan_only_recommendation_rows(raw_recommendations)
    watch_candidates = build_watch_candidates_from_scan(scan_result)
    payload = {
        "generated_at": scan_result.get("scan_finished_at"),
        "exchange_scope": normalized_scope,
        "limit_symbols": safe_limit,
        "scanned": int(scan_result.get("scanned") or 0),
        "recommendations": recommendations,
        "count": len(recommendations),
        "short_term_recommendations": recommendations,
        "short_term_count": len(recommendations),
        "all_short_term_recommendations": recommendations + rejected_recommendations,
        "rejected_recommendations": rejected_recommendations,
        "rejected_count": len(rejected_recommendations),
        "watch_candidates": watch_candidates,
        "watch_count": len(watch_candidates),
        "mail_signal_recommendations": mail_recommendations,
        "mail_signal_count": len(mail_recommendations),
        "scan_diagnostics": extract_short_term_scan_diagnostics(scan_result),
    }
    run_key = persist_real_recommendations_payload(payload) if persist else None
    logger.warning(
        _event_name(source, "scan_persisted" if persist else "scan_completed"),
        extra={
            "slot_marker": slot_marker,
            "redis_key": run_key,
            "generated_at": payload.get("generated_at"),
            "exchange_scope": payload.get("exchange_scope"),
            "limit_symbols": payload.get("limit_symbols"),
            "scanned": payload.get("scanned"),
            "short_term_count": payload.get("short_term_count"),
            "rejected_count": payload.get("rejected_count"),
            "watch_count": payload.get("watch_count"),
            "mail_signal_count": payload.get("mail_signal_count"),
        },
    )
    return payload
