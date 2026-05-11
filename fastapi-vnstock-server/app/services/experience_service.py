from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings
from app.services.claude_service import ClaudeService


TradeMode = Literal["REAL", "DEMO"]
StrategyType = Literal["SHORT_TERM", "LONG_TERM", "TECHNICAL", "MAIL_SIGNAL"]
_claude_service = ClaudeService()


@dataclass(frozen=True)
class ExperienceRecord:
    id: UUID
    trade_id: str
    account_mode: TradeMode
    symbol: str
    strategy_type: StrategyType
    entry_time: datetime
    exit_time: datetime
    pnl_value: float
    pnl_percent: float
    market_context: dict[str, Any]
    root_cause: str
    mistake_tags: list[str]
    improvement_action: str
    confidence_after_review: float
    reviewed_by: Literal["BOT", "USER"] = "BOT"


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper()


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _context_float(ctx: dict[str, Any], key: str, default: float) -> float:
    try:
        value = ctx.get(key, default)
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _guess_root_cause(record: ExperienceRecord) -> tuple[str, list[str], str]:
    tags: list[str] = []
    root_cause = "execution_noise"
    improvement_action = "Giam khoi luong lenh, tang nguong xac nhan volume truoc khi vao lenh."

    trend = str(record.market_context.get("trend", "")).lower()
    market_regime = str(record.market_context.get("market_regime", "")).lower()
    sentiment = str(record.market_context.get("sentiment", "")).lower()
    volume_spike = _context_float(record.market_context, "volume_spike", 1.0)
    rsi14 = _context_float(record.market_context, "rsi14", 50.0)
    distance_from_ema20_pct = _context_float(record.market_context, "distance_from_ema20_pct", 0.0)
    liquidity_value = _context_float(
        record.market_context,
        "liquidity_value",
        _context_float(record.market_context, "avg_value_20d", 0.0),
    )
    rr_realized = _context_float(record.market_context, "rr_realized", 0.0)
    trigger = str(record.market_context.get("trigger") or "").strip().lower()

    if trigger == "stoploss_hit" or record.pnl_percent <= -2:
        tags.append("stoploss_hit")
    if market_regime == "risk_off":
        tags.append("market_regime_risk_off")
        root_cause = "market_regime_filter_missed"
        improvement_action = "Khi VNINDEX risk-off, tang nguong composite va chi mua setup co breakout kem volume/RS ro rang."
    if volume_spike < 1.2:
        tags.append("weak_volume_confirmation")
        root_cause = "false_breakout"
        improvement_action = "Tang nguong volume spike toi thieu 1.8x va bo qua setup thanh khoan yeu."
    if trend in {"down", "sideway"}:
        tags.append("counter_trend_entry")
        root_cause = "wrong_trend_filter"
        improvement_action = "Bat buoc filter EMA20 > EMA50 truoc khi mo vi the moi."
    if sentiment in {"negative", "bad"}:
        tags.append("news_risk_missed")
        root_cause = "news_filter_missed"
        improvement_action = "Tang trong so news filter va loai bo ma co sentiment am trong 24h."
    if rsi14 >= 78:
        tags.append("overbought_chase")
        root_cause = "late_entry_after_momentum"
        improvement_action = "Khong mua duoi khi RSI >= 78; doi pullback ve EMA20 hoac nen tich luy vol giam."
    if distance_from_ema20_pct >= 10:
        tags.append("overextended_from_ema20")
        root_cause = "overextended_entry"
        improvement_action = "Gioi han khoang cach gia voi EMA20 duoi 8% truoc khi mo lenh moi."
    if 0 < liquidity_value < 5_000_000_000:
        tags.append("thin_liquidity")
        root_cause = "liquidity_risk"
        improvement_action = "Loai bo ma co gia tri giao dich 20 phien duoi 5 ty hoac giam ty trong vi the."
    if rr_realized < 1.0:
        tags.append("low_rr")
        if root_cause == "execution_noise":
            root_cause = "risk_reward_unfavorable"
            improvement_action = "Chi mo lenh khi RR ky vong >= 1:2 va dat trailing stop theo ATR."

    if not tags:
        tags.append("unclassified")

    return root_cause, tags, improvement_action


def _cmt_stoploss_playbook(record: ExperienceRecord, tags: list[str]) -> dict[str, Any]:
    """
    CMT-style deterministic lessons for a stop-loss close.
    These fields are consumed later as market_context evidence by signal_engine_service.
    """
    ctx = record.market_context if isinstance(record.market_context, dict) else {}
    tag_set = {str(tag).strip().lower() for tag in tags if str(tag).strip()}
    entry_price = _context_float(ctx, "entry_price", _context_float(ctx, "entry_proxy_price", 0.0))
    exit_price = _context_float(ctx, "exit_price", 0.0)
    stoploss_price = _context_float(ctx, "stoploss_price", 0.0)
    take_profit_price = _context_float(ctx, "take_profit_price", 0.0)
    rsi14 = _context_float(ctx, "rsi14", 50.0)
    volume_spike = _context_float(ctx, "volume_spike", 1.0)
    distance_from_ema20_pct = _context_float(ctx, "distance_from_ema20_pct", 0.0)
    rr_planned = ((take_profit_price - entry_price) / max(1e-9, entry_price - stoploss_price)) if (
        take_profit_price > entry_price > stoploss_price > 0
    ) else 0.0

    lessons: list[str] = [
        "Sau stop-loss, khong re-enter cung ma neu chua co confirm moi ve xu huong, volume va relative strength.",
        "Dung stop-loss nhu invalidation cua thesis, khong coi la nhieu gia re de mua lai ngay.",
    ]
    next_buy_filters: dict[str, Any] = {
        "cooldown_days": 3,
        "require_price_reclaim_entry_zone": True,
        "require_close_above_stoploss": True,
        "require_volume_confirmation": True,
        "min_volume_spike_ratio": 1.8,
        "min_reward_risk": 2.0,
        "max_distance_from_ema20_pct": 8.0,
        "position_size_multiplier": 0.5,
    }

    if {"weak_volume_confirmation", "false_breakout"} & tag_set or volume_spike < 1.2:
        lessons.append("Breakout/can cung that bai khi thieu participation; lan sau can volume xac nhan ro hon.")
        next_buy_filters["min_volume_spike_ratio"] = 2.2
    if {"overbought_chase", "overextended_from_ema20", "late_entry_after_momentum", "overextended_entry"} & tag_set:
        lessons.append("Stoploss sau entry qua extended: doi pullback/tich luy thay vi chase momentum.")
        next_buy_filters["max_distance_from_ema20_pct"] = 5.0
        next_buy_filters["avoid_rsi_above"] = 78
    if "market_regime_risk_off" in tag_set:
        lessons.append("Trong regime risk-off, chi mua setup co RS va volume manh, giam size mac dinh.")
        next_buy_filters["min_volume_spike_ratio"] = max(float(next_buy_filters["min_volume_spike_ratio"]), 2.4)
        next_buy_filters["position_size_multiplier"] = 0.35
        next_buy_filters["require_market_regime_not_risk_off"] = True
    if rr_planned and rr_planned < 2.0:
        lessons.append("R/R planned khong du dem; lan sau nang nguong RR truoc khi vao lenh.")
        next_buy_filters["min_reward_risk"] = 2.2
    if record.pnl_percent <= -5.0:
        lessons.append("Loss lon hon muc tactical thong thuong; lan sau giam size va yeu cau confirmation nhieu hon.")
        next_buy_filters["cooldown_days"] = 5
        next_buy_filters["position_size_multiplier"] = min(float(next_buy_filters["position_size_multiplier"]), 0.35)

    return {
        "framework": "CMT_price_volume_risk_review",
        "trigger": str(ctx.get("trigger") or "stoploss_hit"),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "stoploss_price": stoploss_price,
        "take_profit_price": take_profit_price,
        "planned_reward_risk": round(rr_planned, 4) if rr_planned else None,
        "observed_rsi14": rsi14,
        "observed_volume_spike": volume_spike,
        "observed_distance_from_ema20_pct": distance_from_ema20_pct,
        "lessons": lessons,
        "next_buy_filters": next_buy_filters,
    }


def _heuristic_market_adaptation(tags: list[str], market_context: dict[str, Any]) -> dict[str, Any]:
    """
    Deterministic fallback for entry-gate adaptation when Claude is disabled or unavailable.
    Values mirror the scan gate fields consumed by signal_engine_service.
    """
    tag_set = {str(tag).strip().lower() for tag in tags if str(tag).strip()}
    rollup = market_context.get("experience_rollup") if isinstance(market_context, dict) else None
    stoploss_hits = int((rollup or {}).get("stoploss_hits") or 0) if isinstance(rollup, dict) else 0
    samples = int((rollup or {}).get("samples") or 1) if isinstance(rollup, dict) else 1
    stoploss_ratio = float(stoploss_hits) / float(max(1, samples))

    min_spike = 1.8
    min_momentum = 1.0
    max_distance = 8.0
    if stoploss_ratio >= 0.5 or {"weak_volume_confirmation", "false_breakout"} & tag_set:
        min_spike = 2.2
        min_momentum = 1.5
        max_distance = 6.5
    if {"overbought_chase", "overextended_from_ema20", "late_entry_after_momentum", "overextended_entry"} & tag_set:
        max_distance = min(max_distance, 5.0)
        min_momentum = max(min_momentum, 1.8)
    if "market_regime_risk_off" in tag_set:
        min_spike = max(min_spike, 2.4)
        min_momentum = max(min_momentum, 2.0)
        max_distance = min(max_distance, 5.0)

    return {
        "risk_on": {
            "min_spike_ratio": round(max(1.6, min_spike - 0.2), 4),
            "min_momentum_5d_pct": round(max(0.8, min_momentum - 0.3), 4),
            "max_distance_from_ema20_pct": round(min(8.0, max_distance + 1.0), 4),
        },
        "neutral": {
            "min_spike_ratio": round(min_spike, 4),
            "min_momentum_5d_pct": round(min_momentum, 4),
            "max_distance_from_ema20_pct": round(max_distance, 4),
        },
        "risk_off": {
            "min_spike_ratio": round(max(2.2, min_spike + 0.2), 4),
            "min_momentum_5d_pct": round(max(1.8, min_momentum + 0.4), 4),
            "max_distance_from_ema20_pct": round(min(5.0, max_distance), 4),
        },
    }


def _extract_json_object(raw_text: str) -> dict[str, Any] | None:
    fenced = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", raw_text, flags=re.IGNORECASE)
    candidates = [fenced.group(1)] if fenced else []
    start = raw_text.find("{")
    end = raw_text.rfind("}")
    if start >= 0 and end > start:
        candidates.append(raw_text[start : end + 1])
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _analyze_experience_with_claude(record: ExperienceRecord) -> tuple[str, list[str], str, float, dict[str, Any]]:
    if not settings.use_claude:
        raise RuntimeError("claude_experience_disabled")

    prompt = (
        "Phan tich trade khong hieu qua va tra ve JSON hop le, khong them markdown.\n"
        "Schema:\n"
        "{\n"
        '  "root_cause": "string",\n'
        '  "mistake_tags": ["string"],\n'
        '  "improvement_action": "string",\n'
        '  "confidence_after_review": number,\n'
        '  "market_adaptation_by_regime": {\n'
        '    "risk_on": {"min_spike_ratio": number, "min_momentum_5d_pct": number, "max_distance_from_ema20_pct": number},\n'
        '    "neutral": {"min_spike_ratio": number, "min_momentum_5d_pct": number, "max_distance_from_ema20_pct": number},\n'
        '    "risk_off": {"min_spike_ratio": number, "min_momentum_5d_pct": number, "max_distance_from_ema20_pct": number}\n'
        "  }\n"
        "}\n"
        "Input trade:\n"
        f"- trade_id: {record.trade_id}\n"
        f"- account_mode: {record.account_mode}\n"
        f"- symbol: {record.symbol}\n"
        f"- strategy_type: {record.strategy_type}\n"
        f"- pnl_value: {record.pnl_value}\n"
        f"- pnl_percent: {record.pnl_percent}\n"
        f"- market_context: {record.market_context}\n"
    )
    raw = _claude_service.generate_text_with_resilience(
        prompt=prompt,
        system_prompt=(
            "Ban la chuyen gia danh gia giao dich production. "
            "Tra ve JSON dung schema, de xuat hanh dong cu the, ro rang."
        ),
        max_tokens=settings.ai_claude_experience_max_tokens,
        temperature=0.1,
        cache_namespace="experience-analysis",
        cache_ttl_seconds=settings.ai_claude_experience_cache_ttl_seconds,
    )
    parsed = _extract_json_object(raw)
    if not parsed:
        raise RuntimeError("claude_experience_parse_failed")

    root_cause = str(parsed.get("root_cause") or "").strip() or "execution_noise"
    tags_raw = parsed.get("mistake_tags")
    tags = [str(item).strip() for item in tags_raw if str(item).strip()] if isinstance(tags_raw, list) else []
    if not tags:
        tags = ["unclassified"]
    improvement_action = str(parsed.get("improvement_action") or "").strip()
    if not improvement_action:
        improvement_action = "Raf soat lai dieu kien vao lenh va tang bo loc xac nhan setup."
    confidence = float(parsed.get("confidence_after_review") or record.confidence_after_review)
    confidence = max(0.0, min(100.0, confidence))
    raw_adapt = parsed.get("market_adaptation_by_regime")
    adaptation: dict[str, Any] = {}
    if isinstance(raw_adapt, dict):
        for regime_key in ("risk_on", "neutral", "risk_off"):
            regime_row = raw_adapt.get(regime_key)
            if not isinstance(regime_row, dict):
                continue
            try:
                min_spike = float(regime_row.get("min_spike_ratio"))
                min_mom = float(regime_row.get("min_momentum_5d_pct"))
                max_dist = float(regime_row.get("max_distance_from_ema20_pct"))
            except (TypeError, ValueError):
                continue
            adaptation[regime_key] = {
                "min_spike_ratio": max(1.0, min(4.0, min_spike)),
                "min_momentum_5d_pct": max(0.0, min(5.0, min_mom)),
                "max_distance_from_ema20_pct": max(2.0, min(15.0, max_dist)),
            }
    return root_cause, tags, improvement_action, confidence, adaptation


def _merge_experience_context(
    *,
    current_market_context: dict[str, Any],
    previous_rows: list[dict[str, Any]],
    current_pnl_value: float,
    current_pnl_percent: float,
) -> tuple[dict[str, Any], float, float]:
    total_pnl_value = float(current_pnl_value)
    total_pnl_percent = float(current_pnl_percent)
    samples = 1
    total_stoploss_hits = 1 if str(current_market_context.get("trigger") or "").strip().lower() == "stoploss_hit" else 0
    last_root_causes: list[str] = []
    last_tags: list[str] = []
    for row in previous_rows:
        try:
            total_pnl_value += float(row.get("pnl_value") or 0.0)
            total_pnl_percent += float(row.get("pnl_percent") or 0.0)
            samples += 1
            root_cause = str(row.get("root_cause") or "").strip()
            if root_cause:
                last_root_causes.append(root_cause)
            tags = row.get("mistake_tags")
            if isinstance(tags, list):
                last_tags.extend([str(t).strip() for t in tags if str(t).strip()])
            prev_ctx = row.get("market_context")
            if isinstance(prev_ctx, dict) and str(prev_ctx.get("trigger") or "").strip().lower() == "stoploss_hit":
                total_stoploss_hits += 1
        except Exception:
            continue
    avg_pnl_percent = total_pnl_percent / float(max(1, samples))
    merged_context: dict[str, Any] = dict(current_market_context)
    merged_context["experience_rollup"] = {
        "samples": int(samples),
        "total_pnl_value": round(total_pnl_value, 6),
        "average_pnl_percent": round(avg_pnl_percent, 6),
        "stoploss_hits": int(total_stoploss_hits),
        "recent_root_causes": last_root_causes[:10],
        "recent_tags": sorted(set(last_tags))[:20],
    }
    return merged_context, total_pnl_value, avg_pnl_percent


def ensure_experience_table() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS experience (
        id UUID PRIMARY KEY,
        trade_id TEXT NOT NULL,
        account_mode VARCHAR(10) NOT NULL CHECK (account_mode IN ('REAL', 'DEMO')),
        symbol VARCHAR(20) NOT NULL,
        strategy_type VARCHAR(20) NOT NULL CHECK (strategy_type IN ('SHORT_TERM', 'LONG_TERM', 'TECHNICAL')),
        entry_time TIMESTAMPTZ NOT NULL,
        exit_time TIMESTAMPTZ NOT NULL,
        pnl_value DOUBLE PRECISION NOT NULL,
        pnl_percent DOUBLE PRECISION NOT NULL,
        market_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        root_cause TEXT NOT NULL,
        mistake_tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
        improvement_action TEXT NOT NULL,
        confidence_after_review NUMERIC(5,2) NOT NULL,
        reviewed_by VARCHAR(10) NOT NULL CHECK (reviewed_by IN ('BOT', 'USER')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_experience_symbol ON experience (symbol);
    CREATE INDEX IF NOT EXISTS idx_experience_mode ON experience (account_mode);
    CREATE INDEX IF NOT EXISTS idx_experience_created_at ON experience (created_at DESC);
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cur.execute("ALTER TABLE experience DROP CONSTRAINT IF EXISTS experience_strategy_type_check")
            cur.execute(
                """
                ALTER TABLE experience
                ADD CONSTRAINT experience_strategy_type_check
                CHECK (strategy_type IN ('SHORT_TERM', 'LONG_TERM', 'TECHNICAL', 'MAIL_SIGNAL'))
                """
            )
        conn.commit()


def create_experience_from_trade(payload: dict[str, Any]) -> dict[str, Any]:
    symbol = _normalize_symbol(str(payload["symbol"]))
    account_mode = str(payload["account_mode"]).upper()
    strategy_type = str(payload["strategy_type"]).upper()
    market_context = payload.get("market_context") or {}

    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM experience
                WHERE account_mode = %(account_mode)s
                  AND symbol = %(symbol)s
                  AND strategy_type = %(strategy_type)s
                ORDER BY created_at DESC
                """,
                {
                    "account_mode": account_mode,
                    "symbol": symbol,
                    "strategy_type": strategy_type,
                },
            )
            previous_rows = [dict(row) for row in cur.fetchall()]

    merged_market_context, merged_pnl_value, merged_pnl_percent = _merge_experience_context(
        current_market_context=market_context if isinstance(market_context, dict) else {},
        previous_rows=previous_rows,
        current_pnl_value=float(payload["pnl_value"]),
        current_pnl_percent=float(payload["pnl_percent"]),
    )

    record = ExperienceRecord(
        id=uuid4(),
        trade_id=str(payload["trade_id"]),
        account_mode=account_mode,  # type: ignore[arg-type]
        symbol=symbol,
        strategy_type=strategy_type,  # type: ignore[arg-type]
        entry_time=payload.get("entry_time") or _utc_now(),
        exit_time=payload.get("exit_time") or _utc_now(),
        pnl_value=merged_pnl_value,
        pnl_percent=merged_pnl_percent,
        market_context=merged_market_context,
        root_cause="pending_analysis",
        mistake_tags=[],
        improvement_action="pending_analysis",
        confidence_after_review=float(payload.get("confidence_after_review", 70.0)),
        reviewed_by="BOT",
    )

    root_cause, tags, improvement_action = _guess_root_cause(record)
    confidence_after_review = record.confidence_after_review
    market_adaptation = _heuristic_market_adaptation(tags, record.market_context)
    analysis_source = "heuristic_fallback"
    analysis_error = ""
    try:
        root_cause, tags, improvement_action, confidence_after_review, claude_market_adaptation = _analyze_experience_with_claude(
            record
        )
        if claude_market_adaptation:
            market_adaptation = claude_market_adaptation
        analysis_source = "claude"
    except Exception as exc:
        # Keep heuristic fallback to avoid blocking trade-close pipeline.
        analysis_error = str(exc)
    cmt_playbook = _cmt_stoploss_playbook(record, tags) if "stoploss_hit" in {str(t).lower() for t in tags} else {}
    if isinstance(record.market_context, dict):
        record.market_context["experience_analysis_source"] = analysis_source
        if analysis_error:
            record.market_context["experience_analysis_error"] = analysis_error
        if market_adaptation:
            record.market_context["claude_market_adaptation"] = market_adaptation
            record.market_context["experience_market_adaptation"] = market_adaptation
        if cmt_playbook:
            record.market_context["cmt_stoploss_playbook"] = cmt_playbook

    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # Keep exactly one consolidated experience row per account+symbol+strategy.
            cur.execute(
                """
                DELETE FROM experience
                WHERE account_mode = %(account_mode)s
                  AND symbol = %(symbol)s
                  AND strategy_type = %(strategy_type)s
                """,
                {
                    "account_mode": record.account_mode,
                    "symbol": record.symbol,
                    "strategy_type": record.strategy_type,
                },
            )
            cur.execute(
                """
                INSERT INTO experience (
                    id, trade_id, account_mode, symbol, strategy_type,
                    entry_time, exit_time, pnl_value, pnl_percent, market_context,
                    root_cause, mistake_tags, improvement_action, confidence_after_review, reviewed_by
                ) VALUES (
                    %(id)s, %(trade_id)s, %(account_mode)s, %(symbol)s, %(strategy_type)s,
                    %(entry_time)s, %(exit_time)s, %(pnl_value)s, %(pnl_percent)s, %(market_context)s,
                    %(root_cause)s, %(mistake_tags)s, %(improvement_action)s, %(confidence_after_review)s, %(reviewed_by)s
                )
                RETURNING *;
                """,
                {
                    "id": record.id,
                    "trade_id": record.trade_id,
                    "account_mode": record.account_mode,
                    "symbol": record.symbol,
                    "strategy_type": record.strategy_type,
                    "entry_time": record.entry_time,
                    "exit_time": record.exit_time,
                    "pnl_value": record.pnl_value,
                    "pnl_percent": record.pnl_percent,
                    "market_context": Json(record.market_context),
                    "root_cause": root_cause,
                    "mistake_tags": tags,
                    "improvement_action": improvement_action,
                    "confidence_after_review": confidence_after_review,
                    "reviewed_by": record.reviewed_by,
                },
            )
            inserted = cur.fetchone()
        conn.commit()

    return dict(inserted or {})


def list_experience(
    *,
    account_mode: str | None,
    symbol: str | None,
    strategy_type: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"limit": max(1, min(limit, 200))}

    if account_mode:
        filters.append("account_mode = %(account_mode)s")
        params["account_mode"] = account_mode.upper()
    if symbol:
        filters.append("symbol = %(symbol)s")
        params["symbol"] = symbol.upper()
    if strategy_type:
        filters.append("strategy_type = %(strategy_type)s")
        params["strategy_type"] = strategy_type.upper()

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = f"""
    SELECT *
    FROM experience
    {where_clause}
    ORDER BY created_at DESC
    LIMIT %(limit)s
    """

    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return [dict(row) for row in rows]


def get_experience_claude_runtime_metrics() -> dict[str, Any]:
    return _claude_service.get_runtime_metrics()
