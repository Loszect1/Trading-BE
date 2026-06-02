from __future__ import annotations

import logging
import threading
from collections import defaultdict
from datetime import date
from statistics import median
from typing import Any
from uuid import uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings

logger = logging.getLogger(__name__)

_SCHEMA_LOCK_CLASS_ID = 20260602
_SCHEMA_LOCK_OBJECT_ID = 9
_schema_ready = False
_schema_lock = threading.Lock()
_MIN_SAMPLES_TO_APPLY = 5
_MAX_ADJUSTMENT = 8.0


def _jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (date,)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


def _as_json(value: Any) -> Json:
    return Json(_jsonable(value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def sentiment_bucket(sentiment_label: str | None = None, sentiment_score: float | None = None) -> str:
    label = str(sentiment_label or "").strip().lower()
    if label in {"positive", "negative", "neutral", "mixed"}:
        return label
    score = _safe_float(sentiment_score, 0.0)
    if score >= 20.0:
        return "positive"
    if score <= -20.0:
        return "negative"
    return "neutral"


def sentiment_bucket_from_news_score(news_score: float | None) -> str:
    score = _safe_float(news_score, 50.0)
    if score >= 58.0:
        return "positive"
    if score <= 42.0:
        return "negative"
    return "neutral"


def impact_bucket(impact_score: float | None) -> str:
    score = _safe_float(impact_score, 0.0)
    if score >= 75.0:
        return "high"
    if score >= 50.0:
        return "medium"
    return "low"


def impact_bucket_from_news_score(news_score: float | None) -> str:
    score = abs(_safe_float(news_score, 50.0) - 50.0)
    if score >= 25.0:
        return "high"
    if score >= 10.0:
        return "medium"
    return "low"


def _confidence_from_sample_count(sample_count: int, avg_source_confidence: float) -> float:
    sample_component = _clamp((float(sample_count) / float(_MIN_SAMPLES_TO_APPLY)) * 62.0, 0.0, 72.0)
    source_component = _clamp(avg_source_confidence, 0.0, 100.0) * 0.28
    return round(_clamp(sample_component + source_component, 0.0, 100.0), 2)


def _score_adjustment_from_returns(
    *,
    sample_count: int,
    avg_abnormal_return_pct: float,
    bullish_count: int,
    bearish_count: int,
) -> float:
    if sample_count <= 0:
        return 0.0
    bullish_rate = float(bullish_count) / float(sample_count)
    bearish_rate = float(bearish_count) / float(sample_count)
    directional_edge = (bullish_rate - bearish_rate) * _MAX_ADJUSTMENT
    return_edge = _clamp(avg_abnormal_return_pct * 1.35, -_MAX_ADJUSTMENT, _MAX_ADJUSTMENT)
    raw = (0.55 * return_edge) + (0.45 * directional_edge)
    shrink = _clamp(float(sample_count) / float(_MIN_SAMPLES_TO_APPLY), 0.0, 1.0)
    return round(_clamp(raw * shrink, -_MAX_ADJUSTMENT, _MAX_ADJUSTMENT), 4)


def aggregate_news_impact_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        horizon = int(row.get("horizon_days") or 0)
        if not symbol or horizon not in {1, 3, 5}:
            continue
        sentiment = sentiment_bucket(row.get("sentiment_label"), _safe_float(row.get("sentiment_score"), 0.0))
        impact = impact_bucket(_safe_float(row.get("impact_score"), 0.0))
        category = str(row.get("category_slug") or "general").strip().lower() or "general"
        grouped[(symbol, horizon, sentiment, impact, category)].append(row)

    aggregates: list[dict[str, Any]] = []
    for (symbol, horizon, sentiment, impact, category), bucket_rows in grouped.items():
        values = [_safe_float(r.get("abnormal_return_pct"), _safe_float(r.get("return_pct"), 0.0)) for r in bucket_rows]
        volume_changes = [
            _safe_float(r.get("volume_change_pct"), 0.0)
            for r in bucket_rows
            if r.get("volume_change_pct") is not None
        ]
        confidences = [_safe_float(r.get("confidence"), 0.0) for r in bucket_rows]
        sample_count = len(values)
        bullish_count = len([v for v in values if v >= 0.5])
        bearish_count = len([v for v in values if v <= -0.5])
        neutral_count = max(0, sample_count - bullish_count - bearish_count)
        avg_return = sum(values) / sample_count if sample_count else 0.0
        avg_volume = sum(volume_changes) / len(volume_changes) if volume_changes else 0.0
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.0
        hit_rate = (float(bullish_count) / float(sample_count)) * 100.0 if sample_count else 0.0
        confidence_score = _confidence_from_sample_count(sample_count, avg_confidence)
        adjustment = _score_adjustment_from_returns(
            sample_count=sample_count,
            avg_abnormal_return_pct=avg_return,
            bullish_count=bullish_count,
            bearish_count=bearish_count,
        )
        last_event = max((r.get("event_date") for r in bucket_rows if isinstance(r.get("event_date"), date)), default=None)
        aggregates.append(
            {
                "symbol": symbol,
                "horizon_days": horizon,
                "sentiment_bucket": sentiment,
                "impact_bucket": impact,
                "category_slug": category,
                "sample_count": sample_count,
                "win_count": bullish_count,
                "loss_count": bearish_count,
                "neutral_count": neutral_count,
                "avg_abnormal_return_pct": round(avg_return, 4),
                "median_abnormal_return_pct": round(float(median(values)), 4) if values else 0.0,
                "avg_volume_change_pct": round(avg_volume, 4),
                "hit_rate_pct": round(hit_rate, 4),
                "confidence_score": confidence_score,
                "score_adjustment": adjustment,
                "last_event_at": last_event,
                "metadata": {
                    "source": "news_mail_return_research",
                    "avg_source_confidence": round(avg_confidence, 4),
                    "min_samples_to_apply": _MIN_SAMPLES_TO_APPLY,
                    "max_adjustment": _MAX_ADJUSTMENT,
                },
            }
        )
    return aggregates


def ensure_news_impact_experience_tables() -> None:
    global _schema_ready
    if _schema_ready:
        return
    return_research_ddl = """
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_symbol VARCHAR(20) NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_base_trading_date DATE NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_base_close DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_future_trading_date DATE NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_future_close DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS benchmark_return_pct DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS abnormal_return_pct DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS base_volume DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS future_volume DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS volume_change_pct DOUBLE PRECISION NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS outcome_label VARCHAR(32) NULL;
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS outcome_score DOUBLE PRECISION NULL CHECK (
        outcome_score IS NULL OR (outcome_score >= 0 AND outcome_score <= 100)
    );
    ALTER TABLE news_mail_return_research
    ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
    """

    experience_ddl = """
    CREATE TABLE IF NOT EXISTS news_impact_experience (
        id UUID PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        horizon_days INTEGER NOT NULL CHECK (horizon_days IN (1, 3, 5)),
        sentiment_bucket VARCHAR(16) NOT NULL CHECK (sentiment_bucket IN ('positive', 'negative', 'neutral', 'mixed')),
        impact_bucket VARCHAR(16) NOT NULL CHECK (impact_bucket IN ('low', 'medium', 'high')),
        category_slug VARCHAR(64) NOT NULL DEFAULT 'general',
        sample_count INTEGER NOT NULL DEFAULT 0 CHECK (sample_count >= 0),
        win_count INTEGER NOT NULL DEFAULT 0 CHECK (win_count >= 0),
        loss_count INTEGER NOT NULL DEFAULT 0 CHECK (loss_count >= 0),
        neutral_count INTEGER NOT NULL DEFAULT 0 CHECK (neutral_count >= 0),
        avg_abnormal_return_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
        median_abnormal_return_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
        avg_volume_change_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
        hit_rate_pct DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (hit_rate_pct >= 0 AND hit_rate_pct <= 100),
        confidence_score DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (confidence_score >= 0 AND confidence_score <= 100),
        score_adjustment DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (score_adjustment >= -8 AND score_adjustment <= 8),
        last_event_at DATE NULL,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (symbol, horizon_days, sentiment_bucket, impact_bucket, category_slug)
    );
    CREATE INDEX IF NOT EXISTS idx_news_impact_experience_symbol_horizon
    ON news_impact_experience(symbol, horizon_days, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_news_impact_experience_adjustment
    ON news_impact_experience(score_adjustment DESC, confidence_score DESC);
    """
    with _schema_lock:
        if _schema_ready:
            return
        with connect(settings.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(%s, %s)", (_SCHEMA_LOCK_CLASS_ID, _SCHEMA_LOCK_OBJECT_ID))
                cur.execute("SELECT to_regclass('public.news_mail_return_research')")
                row = cur.fetchone()
                if row and row[0]:
                    cur.execute(return_research_ddl)
                cur.execute(experience_ddl)
            conn.commit()
        _schema_ready = True


def refresh_news_impact_experience(*, days_back: int = 365) -> dict[str, Any]:
    ensure_news_impact_experience_tables()
    safe_days = max(1, min(int(days_back), 2000))
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    rr.symbol,
                    rr.horizon_days,
                    rr.event_date,
                    rr.return_pct,
                    rr.abnormal_return_pct,
                    rr.volume_change_pct,
                    i.sentiment_label,
                    i.sentiment_score,
                    i.impact_score,
                    i.confidence,
                    a.category_slug
                FROM news_mail_return_research rr
                JOIN news_mail_symbol_impacts i ON i.id = rr.symbol_impact_id
                JOIN news_mail_articles a ON a.id = rr.article_id
                WHERE rr.status = 'ready'
                  AND rr.event_date >= CURRENT_DATE - (%(days_back)s::INT * INTERVAL '1 day')
                """,
                {"days_back": safe_days},
            )
            source_rows = [dict(row) for row in cur.fetchall()]

    aggregates = aggregate_news_impact_rows(source_rows)
    written = 0
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            for row in aggregates:
                cur.execute(
                    """
                    INSERT INTO news_impact_experience (
                        id, symbol, horizon_days, sentiment_bucket, impact_bucket, category_slug,
                        sample_count, win_count, loss_count, neutral_count,
                        avg_abnormal_return_pct, median_abnormal_return_pct, avg_volume_change_pct,
                        hit_rate_pct, confidence_score, score_adjustment, last_event_at, metadata
                    )
                    VALUES (
                        %(id)s, %(symbol)s, %(horizon_days)s, %(sentiment_bucket)s, %(impact_bucket)s, %(category_slug)s,
                        %(sample_count)s, %(win_count)s, %(loss_count)s, %(neutral_count)s,
                        %(avg_abnormal_return_pct)s, %(median_abnormal_return_pct)s, %(avg_volume_change_pct)s,
                        %(hit_rate_pct)s, %(confidence_score)s, %(score_adjustment)s, %(last_event_at)s, %(metadata)s
                    )
                    ON CONFLICT (symbol, horizon_days, sentiment_bucket, impact_bucket, category_slug)
                    DO UPDATE SET
                        sample_count = EXCLUDED.sample_count,
                        win_count = EXCLUDED.win_count,
                        loss_count = EXCLUDED.loss_count,
                        neutral_count = EXCLUDED.neutral_count,
                        avg_abnormal_return_pct = EXCLUDED.avg_abnormal_return_pct,
                        median_abnormal_return_pct = EXCLUDED.median_abnormal_return_pct,
                        avg_volume_change_pct = EXCLUDED.avg_volume_change_pct,
                        hit_rate_pct = EXCLUDED.hit_rate_pct,
                        confidence_score = EXCLUDED.confidence_score,
                        score_adjustment = EXCLUDED.score_adjustment,
                        last_event_at = EXCLUDED.last_event_at,
                        metadata = EXCLUDED.metadata,
                        updated_at = NOW()
                    """,
                    {"id": uuid4(), **row, "metadata": _as_json(row.get("metadata") or {})},
                )
                written += 1
        conn.commit()
    return {"source_rows": len(source_rows), "aggregates_written": written, "days_back": safe_days}


def _adjustment_payload_from_rows(
    *,
    symbol: str,
    rows: list[dict[str, Any]],
    reason: str,
) -> dict[str, Any]:
    eligible = [row for row in rows if int(row.get("sample_count") or 0) >= _MIN_SAMPLES_TO_APPLY]
    if not eligible:
        return {
            "applied": False,
            "symbol": symbol,
            "reason": reason if rows else "no_news_impact_experience",
            "adjustment": 0.0,
            "min_samples": _MIN_SAMPLES_TO_APPLY,
            "rows_considered": len(rows),
        }
    horizon_weight = {1: 0.45, 3: 0.35, 5: 0.20}
    weighted = 0.0
    weight_sum = 0.0
    best = max(eligible, key=lambda r: (float(r.get("confidence_score") or 0), int(r.get("sample_count") or 0)))
    for row in eligible:
        confidence = _clamp(_safe_float(row.get("confidence_score"), 0.0), 0.0, 100.0)
        samples = max(1, int(row.get("sample_count") or 0))
        horizon = int(row.get("horizon_days") or 3)
        weight = horizon_weight.get(horizon, 0.2) * (confidence / 100.0) * min(3.0, samples / _MIN_SAMPLES_TO_APPLY)
        weighted += _safe_float(row.get("score_adjustment"), 0.0) * weight
        weight_sum += weight
    adjustment = _clamp((weighted / weight_sum) if weight_sum > 0 else 0.0, -_MAX_ADJUSTMENT, _MAX_ADJUSTMENT)
    return {
        "applied": True,
        "symbol": symbol,
        "reason": "news_impact_experience_applied",
        "adjustment": round(adjustment, 4),
        "max_adjustment": _MAX_ADJUSTMENT,
        "min_samples": _MIN_SAMPLES_TO_APPLY,
        "rows_considered": len(rows),
        "rows_applied": len(eligible),
        "sample_count": int(best.get("sample_count") or 0),
        "horizon_days": int(best.get("horizon_days") or 0),
        "sentiment_bucket": str(best.get("sentiment_bucket") or ""),
        "impact_bucket": str(best.get("impact_bucket") or ""),
        "hit_rate_pct": _safe_float(best.get("hit_rate_pct"), 0.0),
        "avg_abnormal_return_pct": _safe_float(best.get("avg_abnormal_return_pct"), 0.0),
        "confidence_score": _safe_float(best.get("confidence_score"), 0.0),
    }


def get_news_impact_adjustment_for_scanner(*, symbol: str, news_score: float | None) -> dict[str, Any]:
    ensure_news_impact_experience_tables()
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"applied": False, "reason": "missing_symbol", "adjustment": 0.0}
    sentiment = sentiment_bucket_from_news_score(news_score)
    impact = impact_bucket_from_news_score(news_score)
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT *
                FROM news_impact_experience
                WHERE symbol = %(symbol)s
                  AND sentiment_bucket = %(sentiment_bucket)s
                  AND impact_bucket = %(impact_bucket)s
                ORDER BY confidence_score DESC, sample_count DESC, updated_at DESC
                LIMIT 12
                """,
                {"symbol": sym, "sentiment_bucket": sentiment, "impact_bucket": impact},
            )
            rows = [dict(row) for row in cur.fetchall()]
            if not rows:
                cur.execute(
                    """
                    SELECT *
                    FROM news_impact_experience
                    WHERE symbol = %(symbol)s
                    ORDER BY confidence_score DESC, sample_count DESC, updated_at DESC
                    LIMIT 12
                    """,
                    {"symbol": sym},
                )
                rows = [dict(row) for row in cur.fetchall()]
                reason = "fallback_symbol_level_news_impact_experience"
            else:
                reason = "matched_news_score_bucket"
    payload = _adjustment_payload_from_rows(symbol=sym, rows=rows, reason=reason)
    payload["scanner_news_score_0_100"] = _safe_float(news_score, 50.0)
    payload["requested_sentiment_bucket"] = sentiment
    payload["requested_impact_bucket"] = impact
    return payload


def get_news_impact_adjustment_for_mail_signal(symbol: str) -> dict[str, Any]:
    ensure_news_impact_experience_tables()
    sym = str(symbol or "").strip().upper()
    if not sym:
        return {"applied": False, "reason": "missing_symbol", "adjustment": 0.0}
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT *
                FROM news_impact_experience
                WHERE symbol = %(symbol)s
                ORDER BY confidence_score DESC, sample_count DESC, updated_at DESC
                LIMIT 12
                """,
                {"symbol": sym},
            )
            rows = [dict(row) for row in cur.fetchall()]
    return _adjustment_payload_from_rows(symbol=sym, rows=rows, reason="mail_signal_symbol_level_news_impact_experience")


def apply_adjustment_to_signal_quality(metadata: dict[str, Any], adjustment_meta: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        return metadata
    metadata["news_impact_experience"] = adjustment_meta
    if not bool(adjustment_meta.get("applied")):
        return metadata
    sq = metadata.get("signal_quality")
    if not isinstance(sq, dict):
        return metadata
    original = _safe_float(sq.get("composite_score_0_100"), 0.0)
    adjustment = _clamp(_safe_float(adjustment_meta.get("adjustment"), 0.0), -_MAX_ADJUSTMENT, _MAX_ADJUSTMENT)
    adjusted = round(_clamp(original + adjustment, 0.0, 100.0), 2)
    sq["composite_score_before_news_impact_experience"] = round(original, 2)
    sq["composite_score_0_100"] = adjusted
    sq["news_impact_experience_adjustment"] = round(adjustment, 4)
    return metadata
