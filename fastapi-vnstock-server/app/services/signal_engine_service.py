from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from statistics import mean
from typing import Any, Literal
from uuid import uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings
from app.services.claude_service import ClaudeService
from app.services.redis_cache import RedisCacheService
from app.services.signal_scoring_pipeline import (
    build_signal_quality_metadata,
    confidence_from_composite_and_action,
    default_news_aggregate_callable,
    fetch_news_snapshot_for_scoring,
    fetch_symbol_daily_closes,
    score_long_term_technical,
    score_macro_fundamental_proxy,
    score_news_for_symbol,
    score_short_term_technical,
    score_technical_strategy_module,
)
from app.services.vnstock_api_service import VNStockApiService

vnstock_api_service = VNStockApiService()
_claude_service = ClaudeService()
_redis_cache = RedisCacheService()
ExchangeScope = Literal["ALL", "HOSE", "HNX", "UPCOM"]
_market_symbol_tables_ready = False
logger = logging.getLogger(__name__)
_EXCHANGE_ALIASES: dict[str, str] = {
    "HOSE": "HOSE",
    "HSX": "HOSE",
    "XHCM": "HOSE",
    "XHOS": "HOSE",
    "HNX": "HNX",
    "XHNX": "HNX",
    "XHAN": "HNX",
    "UPCOM": "UPCOM",
    "UP": "UPCOM",
    "XUPC": "UPCOM",
}


def ensure_market_symbol_tables() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS market_symbols (
        symbol VARCHAR(20) PRIMARY KEY,
        exchange VARCHAR(16) NOT NULL,
        info JSONB NOT NULL DEFAULT '{}'::jsonb,
        liquidity_gate_updated_at TIMESTAMPTZ NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_market_symbols_exchange ON market_symbols(exchange);

    CREATE TABLE IF NOT EXISTS market_symbol_daily_volume (
        symbol VARCHAR(20) NOT NULL,
        trading_date DATE NOT NULL,
        exchange VARCHAR(16) NOT NULL,
        volume DOUBLE PRECISION NOT NULL CHECK (volume >= 0),
        close_price DOUBLE PRECISION NULL,
        source VARCHAR(16) NOT NULL DEFAULT 'VCI',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (symbol, trading_date)
    );
    CREATE INDEX IF NOT EXISTS idx_market_symbol_daily_volume_exchange_date
    ON market_symbol_daily_volume(exchange, trading_date DESC);

    CREATE TABLE IF NOT EXISTS market_symbol_news (
        id BIGSERIAL PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        exchange VARCHAR(16) NOT NULL,
        news_hash VARCHAR(64) NOT NULL,
        published_at TIMESTAMPTZ NULL,
        source_id VARCHAR(64) NULL,
        category VARCHAR(64) NULL,
        title TEXT NULL,
        summary TEXT NULL,
        url TEXT NULL,
        raw_item JSONB NOT NULL DEFAULT '{}'::jsonb,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (symbol, news_hash)
    );
    CREATE INDEX IF NOT EXISTS idx_market_symbol_news_symbol_published
    ON market_symbol_news(symbol, published_at DESC);
    CREATE INDEX IF NOT EXISTS idx_market_symbol_news_exchange_published
    ON market_symbol_news(exchange, published_at DESC);
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()


def _ensure_market_symbol_tables_once() -> None:
    global _market_symbol_tables_ready
    if _market_symbol_tables_ready:
        return
    ensure_market_symbol_tables()
    _market_symbol_tables_ready = True


def _parse_trading_date_from_row(row: dict[str, Any]) -> date | None:
    raw = row.get("time") or row.get("date") or row.get("trading_date")
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    if isinstance(raw, str):
        candidate = raw.strip()
        if not candidate:
            return None
        iso = candidate[:10]
        try:
            return date.fromisoformat(iso)
        except ValueError:
            return None
    return None


def _persist_symbol_listing_rows(exchange: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    _ensure_market_symbol_tables_once()
    payload: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        instrument_type = str(row.get("type", "")).strip().lower()
        if instrument_type and instrument_type != "stock":
            continue
        row_exchange = str(row.get("exchange", "")).strip().upper()
        canonical_exchange = _EXCHANGE_ALIASES.get(row_exchange, str(exchange).upper())
        if canonical_exchange != str(exchange).upper():
            continue
        payload.append(
            {
                "symbol": symbol,
                "exchange": canonical_exchange,
                "info": Json(_sanitize_json_value(row)),
            }
        )
    if not payload:
        return
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO market_symbols (symbol, exchange, info, created_at, updated_at)
                VALUES (%(symbol)s, %(exchange)s, %(info)s, NOW(), NOW())
                ON CONFLICT (symbol)
                DO UPDATE SET
                    exchange = EXCLUDED.exchange,
                    info = EXCLUDED.info,
                    updated_at = NOW()
                """,
                payload,
            )
        conn.commit()


def _persist_symbol_daily_volume_rows(
    *,
    symbol: str,
    exchange: str,
    source: str,
    rows: list[dict[str, Any]],
    min_trading_date: date | None = None,
) -> None:
    if not rows:
        return
    _ensure_market_symbol_tables_once()
    effective_min_trading_date = min_trading_date
    if effective_min_trading_date is None:
        retention_days = max(5, int(settings.market_symbol_daily_volume_retention_days))
        effective_min_trading_date = date.today() - timedelta(days=retention_days - 1)
    payload: list[dict[str, Any]] = []
    for row in rows:
        trading_date = _parse_trading_date_from_row(row)
        if trading_date is None:
            continue
        if effective_min_trading_date is not None and trading_date < effective_min_trading_date:
            continue
        volume = _to_float(row.get("volume"))
        close_price = _to_float(row.get("close"))
        if volume < 0:
            continue
        payload.append(
            {
                "symbol": symbol,
                "trading_date": trading_date,
                "exchange": exchange,
                "volume": volume,
                "close_price": close_price if close_price > 0 else None,
                "source": source,
            }
        )
    if not payload:
        return
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO market_symbol_daily_volume (
                    symbol, trading_date, exchange, volume, close_price, source, created_at, updated_at
                )
                VALUES (
                    %(symbol)s, %(trading_date)s, %(exchange)s, %(volume)s, %(close_price)s, %(source)s, NOW(), NOW()
                )
                ON CONFLICT (symbol, trading_date)
                DO UPDATE SET
                    exchange = EXCLUDED.exchange,
                    volume = EXCLUDED.volume,
                    close_price = EXCLUDED.close_price,
                    source = EXCLUDED.source,
                    updated_at = NOW()
                """,
                payload,
            )
        conn.commit()


def _sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_value(v) for v in value]
    if isinstance(value, tuple):
        return [_sanitize_json_value(v) for v in value]
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if hasattr(value, "item"):
        try:
            casted = value.item()
            return _sanitize_json_value(casted)
        except Exception:
            return str(value)
    return value


def _parse_news_published_at(raw: Any) -> datetime | None:
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, str):
        candidate = raw.strip()
        if not candidate:
            return None
        normalized = candidate.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None


def _news_item_text_blob(item: dict[str, Any]) -> str:
    title = str(item.get("title") or "")
    summary = str(item.get("summary") or "")
    content = str(item.get("content") or "")
    return " ".join(part for part in (title, summary, content) if part).strip()


def _news_item_mentions_symbol(item: dict[str, Any], symbol: str) -> bool:
    if not symbol:
        return False
    hay = _news_item_text_blob(item).upper()
    if not hay:
        return False
    pattern = rf"(?<![A-Z0-9]){re.escape(symbol)}(?![A-Z0-9])"
    return bool(re.search(pattern, hay))


def _news_item_hash(item: dict[str, Any]) -> str:
    url = str(item.get("url") or "").strip()
    title = str(item.get("title") or "").strip()
    source_id = str(item.get("source_id") or "").strip()
    published_at = str(item.get("published_at") or "").strip()
    canonical = "|".join([url, title, source_id, published_at]).lower()
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _is_older_than_one_day(value: datetime | None) -> bool:
    if value is None:
        return True
    dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)) > timedelta(days=1)


def _fetch_symbol_news_rows_for_refresh_check(symbol: str, limit: int = 30) -> list[dict[str, Any]]:
    _ensure_market_symbol_tables_once()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT title, summary, published_at, updated_at, source_id, category, url
                FROM market_symbol_news
                WHERE symbol = %(symbol)s
                ORDER BY published_at DESC NULLS LAST, updated_at DESC
                LIMIT %(limit)s
                """,
                {"symbol": str(symbol).strip().upper(), "limit": max(1, int(limit))},
            )
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def _latest_news_timestamps(rows: list[dict[str, Any]]) -> tuple[datetime | None, datetime | None]:
    latest_published: datetime | None = None
    latest_updated_for_latest_published: datetime | None = None
    for row in rows:
        published_at = row.get("published_at")
        if not isinstance(published_at, datetime):
            continue
        dt_published = published_at if published_at.tzinfo is not None else published_at.replace(tzinfo=timezone.utc)
        updated_at = row.get("updated_at")
        dt_updated: datetime | None = None
        if isinstance(updated_at, datetime):
            dt_updated = updated_at if updated_at.tzinfo is not None else updated_at.replace(tzinfo=timezone.utc)
        if latest_published is None or dt_published > latest_published:
            latest_published = dt_published
            latest_updated_for_latest_published = dt_updated
    return latest_published, latest_updated_for_latest_published


def _fetch_company_news_for_symbol(symbol: str) -> list[dict[str, Any]]:
    for source in ("VCI", "KBS"):
        try:
            raw = vnstock_api_service.call_company("news", source=source, symbol=symbol, show_log=False)
        except Exception:
            continue
        if not isinstance(raw, list):
            continue
        out: list[dict[str, Any]] = []
        for row in raw:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title") or row.get("headline") or row.get("news_title") or "").strip()
            if not title:
                continue
            out.append(
                {
                    "title": title,
                    "summary": str(row.get("summary") or row.get("content") or row.get("news_short_content") or "").strip(),
                    "published_at": row.get("published_at")
                    or row.get("publishDate")
                    or row.get("date")
                    or row.get("created_at"),
                    "source_id": str(
                        row.get("source") or row.get("publisher") or row.get("news_source") or "vnstock_company_news"
                    ).strip(),
                    "category": "company_news",
                    "url": str(
                        row.get("news_source_link")
                        or row.get("newsSourceLink")
                        or row.get("url")
                        or row.get("link")
                        or row.get("href")
                        or row.get("news_url")
                        or row.get("article_url")
                        or ""
                    ).strip(),
                }
            )
        if out:
            return out
    return []


def get_symbol_news_for_scoring(symbol: str, exchange: str, limit: int = 30) -> list[dict[str, Any]]:
    """
    DB-first symbol news for scoring. Refresh from vnstock only when:
    - no DB rows, OR
    - latest published_at older than 1 day AND latest updated_at older than 1 day.
    """
    sym = str(symbol).strip().upper()
    ex = str(exchange).strip().upper() or "UNKNOWN"
    rows = _fetch_symbol_news_rows_for_refresh_check(sym, limit=limit)
    latest_published, latest_updated = _latest_news_timestamps(rows)
    needs_refresh = len(rows) == 0 or (_is_older_than_one_day(latest_published) and _is_older_than_one_day(latest_updated))
    if needs_refresh:
        fresh = _fetch_company_news_for_symbol(sym)
        if fresh:
            try:
                _persist_symbol_news_rows(
                    symbol=sym,
                    exchange=ex,
                    news_items=fresh,
                    max_items=max(10, int(limit)),
                )
            except Exception as exc:
                logger.warning(
                    "persist_symbol_news_refresh_failed",
                    extra={"symbol": sym, "exchange": ex, "error": str(exc)},
                )
            rows = _fetch_symbol_news_rows_for_refresh_check(sym, limit=limit)

    normalized: list[dict[str, Any]] = []
    for row in rows:
        published = row.get("published_at")
        normalized.append(
            {
                "title": str(row.get("title") or "").strip(),
                "summary": str(row.get("summary") or "").strip(),
                "published_at": published.isoformat() if isinstance(published, datetime) else None,
                "source_id": str(row.get("source_id") or "").strip(),
                "category": str(row.get("category") or "").strip(),
            }
        )
    return normalized


def _persist_symbol_news_rows(
    *,
    symbol: str,
    exchange: str,
    news_items: list[dict[str, Any]],
    max_items: int = 40,
) -> int:
    if not news_items:
        return 0
    _ensure_market_symbol_tables_once()
    sym = str(symbol).strip().upper()
    ex = str(exchange).strip().upper() or "UNKNOWN"
    if not sym:
        return 0

    payload: list[dict[str, Any]] = []
    seen_hashes: set[str] = set()
    for item in news_items:
        if not isinstance(item, dict):
            continue
        if not _news_item_mentions_symbol(item, sym):
            continue
        news_hash = _news_item_hash(item)
        if not news_hash or news_hash in seen_hashes:
            continue
        seen_hashes.add(news_hash)
        payload.append(
            {
                "symbol": sym,
                "exchange": ex,
                "news_hash": news_hash,
                "published_at": _parse_news_published_at(item.get("published_at")),
                "source_id": str(item.get("source_id") or "").strip() or None,
                "category": str(item.get("category") or "").strip() or None,
                "title": str(item.get("title") or "").strip() or None,
                "summary": str(item.get("summary") or "").strip() or None,
                "url": str(item.get("url") or "").strip() or None,
                "raw_item": Json(_sanitize_json_value(item)),
            }
        )
        if len(payload) >= max(1, int(max_items)):
            break
    if not payload:
        return 0

    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO market_symbol_news (
                    symbol,
                    exchange,
                    news_hash,
                    published_at,
                    source_id,
                    category,
                    title,
                    summary,
                    url,
                    raw_item,
                    first_seen_at,
                    last_seen_at,
                    created_at,
                    updated_at
                )
                VALUES (
                    %(symbol)s,
                    %(exchange)s,
                    %(news_hash)s,
                    %(published_at)s,
                    %(source_id)s,
                    %(category)s,
                    %(title)s,
                    %(summary)s,
                    %(url)s,
                    %(raw_item)s,
                    NOW(),
                    NOW(),
                    NOW(),
                    NOW()
                )
                ON CONFLICT (symbol, news_hash)
                DO UPDATE SET
                    exchange = EXCLUDED.exchange,
                    published_at = COALESCE(EXCLUDED.published_at, market_symbol_news.published_at),
                    source_id = COALESCE(EXCLUDED.source_id, market_symbol_news.source_id),
                    category = COALESCE(EXCLUDED.category, market_symbol_news.category),
                    title = COALESCE(EXCLUDED.title, market_symbol_news.title),
                    summary = COALESCE(EXCLUDED.summary, market_symbol_news.summary),
                    url = COALESCE(EXCLUDED.url, market_symbol_news.url),
                    raw_item = EXCLUDED.raw_item,
                    last_seen_at = NOW(),
                    updated_at = NOW()
                """,
                payload,
            )
        conn.commit()
    return len(payload)


def persist_symbol_news_rows(
    *,
    symbol: str,
    exchange: str,
    news_items: list[dict[str, Any]],
    max_items: int = 40,
) -> int:
    """
    Public wrapper to persist symbol-scoped news items into market_symbol_news.
    """
    return _persist_symbol_news_rows(
        symbol=symbol,
        exchange=exchange,
        news_items=news_items,
        max_items=max_items,
    )


def ensure_signals_table() -> None:
    query = """
    CREATE TABLE IF NOT EXISTS signals (
        id UUID PRIMARY KEY,
        strategy_type VARCHAR(20) NOT NULL CHECK (strategy_type IN ('SHORT_TERM', 'LONG_TERM', 'TECHNICAL')),
        symbol VARCHAR(20) NOT NULL,
        action VARCHAR(10) NOT NULL CHECK (action IN ('BUY', 'SELL', 'HOLD')),
        entry_price DOUBLE PRECISION,
        take_profit_price DOUBLE PRECISION,
        stoploss_price DOUBLE PRECISION,
        confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0 AND confidence <= 100),
        reason TEXT NOT NULL,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _normalize_exchange_scope(exchange_scope: str) -> ExchangeScope:
    normalized = str(exchange_scope or "ALL").strip().upper()
    return normalized if normalized in {"ALL", "HOSE", "HNX", "UPCOM"} else "ALL"


def _resolve_exchange_list(exchange_scope: str) -> tuple[str, ...]:
    scope = _normalize_exchange_scope(exchange_scope)
    if scope == "ALL":
        return ("HOSE", "HNX", "UPCOM")
    return (scope,)


def _get_sample_symbols(limit_per_exchange: int = 20, exchange_scope: str = "ALL") -> list[str]:
    exchanges = _resolve_exchange_list(exchange_scope)
    symbols: list[str] = []
    for exchange in exchanges:
        rows = vnstock_api_service.call_listing(
            "symbols_by_exchange",
            method_kwargs={"exchange": exchange},
        )
        if isinstance(rows, list):
            for row in rows[:limit_per_exchange]:
                if isinstance(row, dict):
                    sym = str(row.get("symbol", "")).strip().upper()
                    if sym:
                        symbols.append(sym)
    seen = set()
    output = []
    for sym in symbols:
        if sym in seen:
            continue
        seen.add(sym)
        output.append(sym)
    return output


def _get_scan_symbols_round_robin_boards(limit_total: int, exchange_scope: str = "ALL") -> list[str]:
    """
    Build scan universe from HOSE, HNX, UPCOM with round-robin order so a capped run
    (e.g. limit_symbols=30) still includes all three boards instead of front-loading HOSE only.
    """
    exchanges = _resolve_exchange_list(exchange_scope)
    raw_by_ex: dict[str, list[str]] = {}
    for exchange in exchanges:
        try:
            rows = vnstock_api_service.call_listing(
                "symbols_by_exchange",
                method_kwargs={"exchange": exchange},
            )
        except Exception:
            rows = []
        if isinstance(rows, list):
            dict_rows = [row for row in rows if isinstance(row, dict)]
            try:
                _persist_symbol_listing_rows(exchange, dict_rows)
            except Exception as exc:
                logger.warning("persist_symbol_listing_rows_failed", extra={"exchange": exchange, "error": str(exc)})
        syms: list[str] = []
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    s = str(row.get("symbol", "")).strip().upper()
                    instrument_type = str(row.get("type", "")).strip().lower()
                    row_exchange = str(row.get("exchange", "")).strip().upper()
                    mapped_exchange = _EXCHANGE_ALIASES.get(row_exchange, exchange)
                    if instrument_type and instrument_type != "stock":
                        continue
                    if mapped_exchange != exchange:
                        continue
                    if s:
                        syms.append(s)
        raw_by_ex[exchange] = syms

    idx: dict[str, int] = {ex: 0 for ex in exchanges}
    used: set[str] = set()
    picked: list[str] = []
    requested_total = int(limit_total)
    unlimited = requested_total <= 0

    while unlimited or len(picked) < requested_total:
        progressed = False
        for ex in exchanges:
            syms = raw_by_ex[ex]
            while idx[ex] < len(syms):
                sym = syms[idx[ex]]
                idx[ex] += 1
                if sym in used:
                    continue
                used.add(sym)
                picked.append(sym)
                progressed = True
                break
            if (not unlimited) and len(picked) >= requested_total:
                break
        if not progressed:
            break
    return picked


def _get_scan_symbol_exchange_pairs_round_robin(limit_total: int, exchange_scope: str = "ALL") -> list[tuple[str, str]]:
    exchanges = _resolve_exchange_list(exchange_scope)
    raw_by_ex: dict[str, list[str]] = {}
    for exchange in exchanges:
        try:
            rows = vnstock_api_service.call_listing(
                "symbols_by_exchange",
                method_kwargs={"exchange": exchange},
            )
        except Exception:
            rows = []
        if isinstance(rows, list):
            dict_rows = [row for row in rows if isinstance(row, dict)]
            try:
                _persist_symbol_listing_rows(exchange, dict_rows)
            except Exception as exc:
                logger.warning("persist_symbol_listing_rows_failed", extra={"exchange": exchange, "error": str(exc)})
        syms: list[str] = []
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    s = str(row.get("symbol", "")).strip().upper()
                    instrument_type = str(row.get("type", "")).strip().lower()
                    row_exchange = str(row.get("exchange", "")).strip().upper()
                    mapped_exchange = _EXCHANGE_ALIASES.get(row_exchange, exchange)
                    if instrument_type and instrument_type != "stock":
                        continue
                    if mapped_exchange != exchange:
                        continue
                    if s:
                        syms.append(s)
        raw_by_ex[exchange] = syms

    idx: dict[str, int] = {ex: 0 for ex in exchanges}
    used: set[str] = set()
    picked: list[tuple[str, str]] = []
    requested_total = int(limit_total)
    unlimited = requested_total <= 0

    while unlimited or len(picked) < requested_total:
        progressed = False
        for ex in exchanges:
            syms = raw_by_ex[ex]
            while idx[ex] < len(syms):
                sym = syms[idx[ex]]
                idx[ex] += 1
                if sym in used:
                    continue
                used.add(sym)
                picked.append((sym, ex))
                progressed = True
                break
            if (not unlimited) and len(picked) >= requested_total:
                break
        if not progressed:
            break
    return picked


def _get_scan_symbol_exchange_pairs_from_liquidity_cache(
    limit_total: int,
    exchange_scope: str = "ALL",
) -> list[tuple[str, str]]:
    """
    Build scanner universe from Redis liquidity cache keys only.
    Key format: scan:liquidity:{exchange}:{symbol}
    """
    exchanges = _resolve_exchange_list(exchange_scope)
    keys = _redis_cache.scan_keys("scan:liquidity:*", limit=500_000)
    preferred_by_exchange: dict[str, list[str]] = {ex: [] for ex in exchanges}
    fallback_by_exchange: dict[str, list[str]] = {ex: [] for ex in exchanges}
    preferred_seen: dict[str, set[str]] = {ex: set() for ex in exchanges}
    fallback_seen: dict[str, set[str]] = {ex: set() for ex in exchanges}

    for key in keys:
        parts = str(key).split(":", 3)
        if len(parts) != 4:
            continue
        _, domain, exchange, symbol = parts
        if domain != "liquidity":
            continue
        ex = str(exchange).strip().upper()
        sym = str(symbol).strip().upper()
        if ex not in preferred_by_exchange or not sym:
            continue
        payload = _redis_cache.get_json(str(key))
        if not isinstance(payload, dict):
            continue
        if not bool(payload.get("eligible_liquidity", False)):
            continue
        # Preferred universe: symbols that were already liquid + spiking from warm cache.
        if bool(payload.get("eligible_spike", False)):
            if sym in preferred_seen[ex]:
                continue
            preferred_seen[ex].add(sym)
            preferred_by_exchange[ex].append(sym)
            continue
        # Fallback universe: keep liquid symbols even when warm cache has no spike yet.
        # Scanner will re-evaluate fresh spike ratios during run.
        if sym in fallback_seen[ex]:
            continue
        fallback_seen[ex].add(sym)
        fallback_by_exchange[ex].append(sym)

    by_exchange = preferred_by_exchange
    if all(len(by_exchange[ex]) == 0 for ex in exchanges):
        logger.warning(
            "short_term_scan_universe_cache_fallback_liquidity_only",
            extra={"exchange_scope": _normalize_exchange_scope(exchange_scope)},
        )
        by_exchange = fallback_by_exchange

    idx: dict[str, int] = {ex: 0 for ex in exchanges}
    used: set[str] = set()
    picked: list[tuple[str, str]] = []
    requested_total = int(limit_total)
    unlimited = requested_total <= 0

    while unlimited or len(picked) < requested_total:
        progressed = False
        for ex in exchanges:
            syms = by_exchange[ex]
            while idx[ex] < len(syms):
                sym = syms[idx[ex]]
                idx[ex] += 1
                if sym in used:
                    continue
                used.add(sym)
                picked.append((sym, ex))
                progressed = True
                break
            if (not unlimited) and len(picked) >= requested_total:
                break
        if not progressed:
            break
    return picked


def _get_close_and_volume(symbol: str, bars: int = 120, exchange: str = "") -> tuple[list[float], list[float]]:
    try:
        rows = vnstock_api_service.call_quote(
            "history",
            source="VCI",
            symbol=symbol,
            method_kwargs={"interval": "1D", "count_back": bars},
        )
    except Exception:
        # Skip symbol-level data-source failures; do not fail entire scan batch.
        return [], []
    closes: list[float] = []
    volumes: list[float] = []
    raw_rows: list[dict[str, Any]] = []
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            raw_rows.append(row)
            close = _to_float(row.get("close"))
            volume = _to_float(row.get("volume"))
            if close > 0 and volume >= 0:
                closes.append(close)
                volumes.append(volume)
    if exchange and raw_rows:
        try:
            _persist_symbol_daily_volume_rows(
                symbol=symbol,
                exchange=str(exchange).upper(),
                source="VCI",
                rows=raw_rows,
            )
        except Exception as exc:
            logger.warning(
                "persist_symbol_daily_volume_rows_failed",
                extra={"symbol": symbol, "exchange": str(exchange).upper(), "error": str(exc)},
            )
    return closes, volumes


def _is_short_term_liquid_enough(baseline_vol: float, latest_vol: float) -> bool:
    return bool(
        baseline_vol >= float(settings.short_term_scan_min_avg_daily_volume)
        and latest_vol >= float(settings.short_term_scan_min_latest_volume)
    )


def _is_short_term_volume_spike(spike_ratio: float) -> bool:
    return bool(spike_ratio >= float(settings.short_term_scan_min_volume_spike_ratio))


def _short_term_entry_gate(
    *,
    closes: list[float],
    volumes: list[float],
    spike: float,
    last_close: float,
    ema20_proxy: float,
    min_spike_ratio: float | None = None,
    min_momentum_5d_pct: float = 1.0,
    max_distance_from_ema20_pct: float = 8.0,
    rsi_min: float = 52.0,
    rsi_max: float = 75.0,
    macd_min_line: float = 0.0,
) -> tuple[bool, dict[str, Any]]:
    """
    Production-safe BUY gate to reduce false positives:
    - trend + liquidity/spike already filtered upstream
    - add breakout confirmation, momentum confirmation, and avoid overly stretched entries.
    """
    if len(closes) < 55 or len(volumes) < 55 or ema20_proxy <= 0 or last_close <= 0:
        return False, {"reason": "insufficient_bars_or_invalid_price_for_full_technical_confirm"}

    min_spike = float(min_spike_ratio) if min_spike_ratio is not None else float(settings.short_term_scan_min_volume_spike_ratio)
    breakout_lookback = 20
    prev_window = closes[-(breakout_lookback + 1) : -1]
    prev_high = max(prev_window) if prev_window else last_close
    breakout_ok = bool(last_close >= prev_high * 0.995)

    ema50_proxy = mean(closes[-50:])
    ema_alignment_ok = bool(last_close > ema20_proxy > ema50_proxy)

    momentum_5d = 0.0
    if len(closes) >= 6 and closes[-6] > 0:
        momentum_5d = ((last_close / closes[-6]) - 1.0) * 100.0
    momentum_ok = bool(momentum_5d >= float(min_momentum_5d_pct))

    # RSI(14) using simple average gain/loss for deterministic gating.
    deltas = [float(closes[i] - closes[i - 1]) for i in range(1, len(closes))]
    recent_deltas = deltas[-14:]
    gains = [d for d in recent_deltas if d > 0]
    losses = [abs(d) for d in recent_deltas if d < 0]
    avg_gain = (sum(gains) / 14.0) if recent_deltas else 0.0
    avg_loss = (sum(losses) / 14.0) if recent_deltas else 0.0
    if avg_loss <= 0:
        rsi14 = 100.0 if avg_gain > 0 else 50.0
    else:
        rs = avg_gain / avg_loss
        rsi14 = 100.0 - (100.0 / (1.0 + rs))
    rsi_ok = bool(float(rsi_min) <= rsi14 <= float(rsi_max))

    def _ema_series(values: list[float], period: int) -> list[float]:
        out: list[float] = []
        if not values:
            return out
        alpha = 2.0 / (float(period) + 1.0)
        ema = float(values[0])
        out.append(ema)
        for v in values[1:]:
            ema = alpha * float(v) + (1.0 - alpha) * ema
            out.append(ema)
        return out

    ema12 = _ema_series([float(v) for v in closes], 12)
    ema26 = _ema_series([float(v) for v in closes], 26)
    macd_line_series = [a - b for a, b in zip(ema12, ema26)]
    macd_signal_series = _ema_series(macd_line_series, 9)
    macd_line = float(macd_line_series[-1]) if macd_line_series else 0.0
    macd_signal = float(macd_signal_series[-1]) if macd_signal_series else 0.0
    macd_ok = bool(macd_line >= macd_signal and macd_line >= float(macd_min_line))

    distance_pct = ((last_close / ema20_proxy) - 1.0) * 100.0 if ema20_proxy > 0 else 0.0
    not_overstretched = bool(distance_pct <= float(max_distance_from_ema20_pct))

    pass_gate = bool(
        spike >= min_spike
        and ema_alignment_ok
        and breakout_ok
        and momentum_ok
        and rsi_ok
        and macd_ok
        and not_overstretched
    )
    return pass_gate, {
        "min_spike_ratio": round(min_spike, 4),
        "spike_ratio": round(float(spike), 4),
        "ema20_proxy": round(float(ema20_proxy), 4),
        "ema50_proxy": round(float(ema50_proxy), 4),
        "ema_alignment_ok": ema_alignment_ok,
        "prev_20d_high": round(float(prev_high), 4),
        "breakout_ok": breakout_ok,
        "momentum_5d_pct": round(float(momentum_5d), 4),
        "min_momentum_5d_pct": round(float(min_momentum_5d_pct), 4),
        "momentum_ok": momentum_ok,
        "rsi14": round(float(rsi14), 4),
        "rsi_min": round(float(rsi_min), 4),
        "rsi_max": round(float(rsi_max), 4),
        "rsi_ok": rsi_ok,
        "macd_line": round(float(macd_line), 6),
        "macd_signal": round(float(macd_signal), 6),
        "macd_min_line": round(float(macd_min_line), 6),
        "macd_ok": macd_ok,
        "distance_from_ema20_pct": round(float(distance_pct), 4),
        "max_distance_from_ema20_pct": round(float(max_distance_from_ema20_pct), 4),
        "not_overstretched": not_overstretched,
    }


def _entry_gate_thresholds_from_experience(exp_meta: dict[str, Any] | None, *, market_regime: str = "neutral") -> dict[str, Any]:
    """
    Tighten entry gate when recent experience indicates frequent losses/stoploss.
    """
    base_spike = float(settings.short_term_scan_min_volume_spike_ratio)
    thresholds = {
        "experience_tightening_level": "normal",
        "min_spike_ratio": base_spike,
        "min_momentum_5d_pct": 1.0,
        "max_distance_from_ema20_pct": 8.0,
    }
    if not isinstance(exp_meta, dict) or not bool(exp_meta.get("applied")):
        return thresholds
    claude_adaptation = exp_meta.get("claude_market_adaptation")
    if isinstance(claude_adaptation, dict):
        regime_row = claude_adaptation.get(str(market_regime).lower())
        if isinstance(regime_row, dict):
            try:
                thresholds["min_spike_ratio"] = max(base_spike, float(regime_row.get("min_spike_ratio")))
                thresholds["min_momentum_5d_pct"] = max(0.0, float(regime_row.get("min_momentum_5d_pct")))
                thresholds["max_distance_from_ema20_pct"] = max(2.0, float(regime_row.get("max_distance_from_ema20_pct")))
                thresholds["experience_tightening_level"] = f"claude_{str(market_regime).lower()}"
                return thresholds
            except (TypeError, ValueError):
                pass
    stoploss_ratio = float(exp_meta.get("stoploss_ratio") or 0.0)
    win_ratio = float(exp_meta.get("win_ratio") or 0.0)
    avg_loss_pct = float(exp_meta.get("avg_loss_percent_abs") or 0.0)
    if stoploss_ratio >= 0.7 or avg_loss_pct >= 2.5:
        thresholds.update(
            {
                "experience_tightening_level": "strict",
                "min_spike_ratio": max(base_spike, 2.2),
                "min_momentum_5d_pct": 1.8,
                "max_distance_from_ema20_pct": 5.0,
            }
        )
    elif stoploss_ratio >= 0.5 or (win_ratio < 0.35 and stoploss_ratio >= 0.35):
        thresholds.update(
            {
                "experience_tightening_level": "tight",
                "min_spike_ratio": max(base_spike, 1.9),
                "min_momentum_5d_pct": 1.3,
                "max_distance_from_ema20_pct": 6.5,
            }
        )
    return thresholds


def _experience_buy_cooldown(symbol: str, action: str) -> tuple[bool, dict[str, Any]]:
    """
    Block re-entry for symbols that recently hit stoploss in experience records.
    """
    normalized_action = str(action or "").upper()
    if normalized_action != "BUY":
        return False, {"applied": False, "reason": "non_buy_action"}
    sym = str(symbol or "").strip().upper()
    if not sym:
        return False, {"applied": False, "reason": "missing_symbol"}
    cooldown_days = 3
    try:
        with connect(settings.database_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT created_at, pnl_percent, mistake_tags
                    FROM experience
                    WHERE symbol = %(symbol)s
                      AND strategy_type = 'SHORT_TERM'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    {"symbol": sym},
                )
                row = cur.fetchone()
    except Exception:
        return False, {"applied": False, "reason": "experience_query_failed", "cooldown_days": cooldown_days}
    if not row:
        return False, {"applied": False, "reason": "no_samples", "cooldown_days": cooldown_days}

    created_at = row.get("created_at")
    pnl_percent = float(row.get("pnl_percent") or 0.0)
    tags = row.get("mistake_tags") or []
    tags_norm = {str(t).strip().lower() for t in tags} if isinstance(tags, list) else set()
    stoploss_like = bool("stoploss_hit" in tags_norm or pnl_percent <= -1.0)
    if not stoploss_like:
        return False, {"applied": True, "reason": "latest_trade_not_stoploss", "cooldown_days": cooldown_days}
    if not isinstance(created_at, datetime):
        return False, {"applied": True, "reason": "missing_created_at", "cooldown_days": cooldown_days}

    age = datetime.now(tz=timezone.utc) - created_at
    active = age < timedelta(days=cooldown_days)
    return active, {
        "applied": True,
        "cooldown_days": cooldown_days,
        "latest_experience_at": created_at.isoformat(),
        "latest_pnl_percent": round(pnl_percent, 4),
        "latest_stoploss_like": stoploss_like,
        "cooldown_active": active,
        "age_hours": round(max(0.0, age.total_seconds() / 3600.0), 4),
    }


def _dynamic_short_term_buy_composite_floor(*, benchmark_closes: list[float] | None) -> float:
    """
    Dynamic BUY gate for short-term composite score.
    - Risk-on regime (VNINDEX above EMA20): lower floor.
    - Risk-off regime (VNINDEX below EMA20): raise floor.
    """
    floor = 58.0
    closes = [float(v) for v in (benchmark_closes or []) if float(v) > 0]
    if len(closes) < 20:
        return floor
    last_close = float(closes[-1])
    ema20_proxy = mean(closes[-20:])
    if ema20_proxy <= 0:
        return floor
    trend_ratio = (last_close / ema20_proxy) - 1.0
    if trend_ratio >= 0.01:
        return 55.0
    if trend_ratio <= -0.01:
        return 62.0
    return floor


def _technical_thresholds_from_market_regime(benchmark_closes: list[float] | None) -> dict[str, float | str]:
    closes = [float(v) for v in (benchmark_closes or []) if float(v) > 0]
    if len(closes) < 20:
        return {"regime": "neutral", "rsi_min": 52.0, "rsi_max": 75.0, "macd_min_line": 0.0}
    last = closes[-1]
    ema20 = mean(closes[-20:])
    if ema20 <= 0:
        return {"regime": "neutral", "rsi_min": 52.0, "rsi_max": 75.0, "macd_min_line": 0.0}
    trend_ratio = (last / ema20) - 1.0
    if trend_ratio >= 0.01:
        return {"regime": "risk_on", "rsi_min": 50.0, "rsi_max": 80.0, "macd_min_line": -0.02}
    if trend_ratio <= -0.01:
        return {"regime": "risk_off", "rsi_min": 55.0, "rsi_max": 70.0, "macd_min_line": 0.05}
    return {"regime": "neutral", "rsi_min": 52.0, "rsi_max": 75.0, "macd_min_line": 0.0}


def _liquidity_cache_key(symbol: str, exchange: str) -> str:
    return f"scan:liquidity:{exchange}:{symbol}"


def _read_liquidity_gate_cache(symbol: str, exchange: str) -> dict[str, Any] | None:
    return _redis_cache.get_json(_liquidity_cache_key(symbol, exchange))


def _write_liquidity_gate_cache(
    *,
    symbol: str,
    exchange: str,
    baseline_vol: float,
    latest_vol: float,
    spike_ratio: float,
    eligible_liquidity: bool,
    eligible_spike: bool,
) -> None:
    _redis_cache.set_json(
        _liquidity_cache_key(symbol, exchange),
        {
            "symbol": symbol,
            "exchange": exchange,
            "baseline_vol": baseline_vol,
            "latest_vol": latest_vol,
            "spike_ratio": spike_ratio,
            "eligible_liquidity": bool(eligible_liquidity),
            "eligible_spike": bool(eligible_spike),
        },
        ttl_seconds=int(settings.short_term_symbol_liquidity_cache_ttl_seconds),
    )


def _insert_signal(
    strategy_type: str,
    symbol: str,
    action: str,
    entry: float | None,
    tp: float | None,
    sl: float | None,
    confidence: float,
    reason: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    ensure_signals_table()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO signals (
                    id, strategy_type, symbol, action, entry_price, take_profit_price, stoploss_price,
                    confidence, reason, metadata, status, created_at
                ) VALUES (
                    %(id)s, %(strategy_type)s, %(symbol)s, %(action)s, %(entry)s, %(tp)s, %(sl)s,
                    %(confidence)s, %(reason)s, %(metadata)s, 'ACTIVE', NOW()
                )
                RETURNING *
                """,
                {
                    "id": uuid4(),
                    "strategy_type": strategy_type,
                    "symbol": symbol,
                    "action": action,
                    "entry": entry,
                    "tp": tp,
                    "sl": sl,
                    "confidence": max(0.0, min(100.0, confidence)),
                    "reason": reason,
                    "metadata": Json(metadata or {}),
                },
            )
            row = cur.fetchone()
        conn.commit()
    return dict(row or {})


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


def _claude_scoring_analysis(
    *,
    strategy_type: str,
    symbol: str,
    action: str,
    reason: str,
    metadata: dict[str, Any],
) -> dict[str, Any] | None:
    if not settings.ai_claude_signal_scoring_enabled:
        return None
    prompt = (
        "Ban la risk analyst cho scoring trading bot. "
        "Danh gia nhanh ket qua scoring va tra ve JSON hop le, khong markdown.\n"
        "Schema:\n"
        "{\n"
        '  "score_commentary": "string",\n'
        '  "risk_notes": ["string"],\n'
        '  "confidence_adjustment": number\n'
        "}\n"
        f"Input:\nstrategy_type={strategy_type}\nsymbol={symbol}\naction={action}\nreason={reason}\nmetadata={metadata}"
    )
    raw = _claude_service.generate_text_with_resilience(
        prompt=prompt,
        system_prompt="Tra ve JSON dung schema, ngan gon, uu tien canh bao rui ro.",
        max_tokens=settings.ai_claude_signal_scoring_max_tokens,
        temperature=0.1,
        cache_namespace=f"signal-scoring:{strategy_type}",
        cache_ttl_seconds=settings.ai_claude_signal_scoring_cache_ttl_seconds,
    )
    parsed = _extract_json_object(raw)
    if not parsed:
        return None
    notes_raw = parsed.get("risk_notes")
    risk_notes = [str(item).strip() for item in notes_raw if str(item).strip()] if isinstance(notes_raw, list) else []
    adjustment = float(parsed.get("confidence_adjustment") or 0.0)
    adjustment = max(-15.0, min(15.0, adjustment))
    return {
        "score_commentary": str(parsed.get("score_commentary") or "").strip(),
        "risk_notes": risk_notes,
        "confidence_adjustment": adjustment,
    }


def _experience_confidence_adjustment(symbol: str, action: str) -> tuple[float, dict[str, Any]]:
    normalized_action = str(action or "").upper()
    if normalized_action != "BUY":
        return 0.0, {"applied": False, "reason": "non_buy_action"}
    sym = str(symbol or "").strip().upper()
    if not sym:
        return 0.0, {"applied": False, "reason": "missing_symbol"}
    try:
        with connect(settings.database_url, row_factory=dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pnl_percent, mistake_tags, market_context
                    FROM experience
                    WHERE symbol = %(symbol)s
                      AND strategy_type = 'SHORT_TERM'
                    ORDER BY created_at DESC
                    LIMIT 20
                    """,
                    {"symbol": sym},
                )
                rows = cur.fetchall()
    except Exception:
        return 0.0, {"applied": False, "reason": "experience_query_failed"}
    samples = [dict(r) for r in rows or []]
    if not samples:
        return 0.0, {"applied": False, "reason": "no_samples"}
    stoploss_hits = 0
    wins = 0
    losses = 0
    loss_abs_sum = 0.0
    for row in samples:
        pnl = float(row.get("pnl_percent") or 0.0)
        tags = row.get("mistake_tags") or []
        tags_norm = {str(t).strip().lower() for t in tags} if isinstance(tags, list) else set()
        if pnl < 0:
            losses += 1
            loss_abs_sum += abs(pnl)
            if "stoploss_hit" in tags_norm or pnl <= -1.0:
                stoploss_hits += 1
        elif pnl > 0:
            wins += 1
    n = len(samples)
    stoploss_ratio = float(stoploss_hits) / float(n)
    win_ratio = float(wins) / float(n)
    avg_loss_percent_abs = (loss_abs_sum / float(losses)) if losses > 0 else 0.0
    adjustment = 0.0
    if stoploss_ratio >= 0.5:
        adjustment -= min(12.0, 4.0 + stoploss_ratio * 12.0)
    if win_ratio >= 0.6:
        adjustment += min(6.0, 2.0 + win_ratio * 6.0)
    adjustment = max(-15.0, min(8.0, adjustment))
    latest_ctx = samples[0].get("market_context") if samples else None
    claude_adaptation = (
        latest_ctx.get("claude_market_adaptation")
        if isinstance(latest_ctx, dict) and isinstance(latest_ctx.get("claude_market_adaptation"), dict)
        else None
    )
    return adjustment, {
        "applied": True,
        "samples": n,
        "stoploss_hits": stoploss_hits,
        "wins": wins,
        "losses": losses,
        "stoploss_ratio": round(stoploss_ratio, 4),
        "win_ratio": round(win_ratio, 4),
        "avg_loss_percent_abs": round(avg_loss_percent_abs, 4),
        "confidence_adjustment": adjustment,
        "claude_market_adaptation": claude_adaptation,
    }


def run_short_term_scan_batch(limit_symbols: int = 0, exchange_scope: str = "ALL") -> dict[str, Any]:
    """
    Run the short-term scanner once, returning inserted rows plus coverage counters
    for automation and observability.
    """
    signals: list[dict[str, Any]] = []
    scanned = 0
    skipped_insufficient_data = 0
    skipped_low_liquidity = 0
    skipped_no_volume_spike = 0
    skipped_entry_gate = 0
    skipped_experience_cooldown = 0
    skipped_dynamic_buy_floor = 0
    experience_threshold_source_claude = 0
    experience_threshold_source_heuristic = 0
    scan_started_at = time.perf_counter()
    step_stats: dict[str, dict[str, float | int]] = {}

    def _track_step(name: str, started: float) -> None:
        elapsed = time.perf_counter() - started
        bucket = step_stats.setdefault(name, {"seconds": 0.0, "calls": 0})
        bucket["seconds"] = float(bucket["seconds"]) + float(elapsed)
        bucket["calls"] = int(bucket["calls"]) + 1

    bench_t0 = time.perf_counter()
    bench_closes = fetch_symbol_daily_closes(
        vnstock_api_service.call_quote,
        "VNINDEX",
        90,
    )
    _track_step("fetch_symbol_daily_closes_benchmark", bench_t0)
    dynamic_buy_floor = _dynamic_short_term_buy_composite_floor(benchmark_closes=bench_closes)
    technical_thresholds = _technical_thresholds_from_market_regime(bench_closes)
    market_regime = str(technical_thresholds.get("regime") or "neutral")

    normalized_scope = _normalize_exchange_scope(exchange_scope)
    universe_t0 = time.perf_counter()
    symbol_pairs = _get_scan_symbol_exchange_pairs_from_liquidity_cache(limit_symbols, normalized_scope)
    _track_step("_get_scan_symbol_exchange_pairs_from_liquidity_cache", universe_t0)
    if not symbol_pairs:
        raise RuntimeError(
            f"scanner_universe_empty: no symbols found in redis liquidity cache for scope={normalized_scope}"
        )
    for symbol, symbol_exchange in symbol_pairs:
        scanned += 1
        cache_t0 = time.perf_counter()
        cached = _read_liquidity_gate_cache(symbol, symbol_exchange)
        _track_step("_read_liquidity_gate_cache", cache_t0)
        if isinstance(cached, dict):
            if not bool(cached.get("eligible_liquidity", True)):
                skipped_low_liquidity += 1
                continue
            if not bool(cached.get("eligible_spike", True)):
                skipped_no_volume_spike += 1
                continue
        cv_t0 = time.perf_counter()
        closes, volumes = _get_close_and_volume(symbol, bars=50, exchange=symbol_exchange)
        _track_step("_get_close_and_volume", cv_t0)
        if len(closes) < 25 or len(volumes) < 25:
            skipped_insufficient_data += 1
            continue
        last_close = closes[-1]
        ema20_proxy = mean(closes[-20:])
        baseline_vol = mean(volumes[-31:-1]) if len(volumes) >= 31 else 0.0
        latest_vol = volumes[-1]
        spike = (latest_vol / baseline_vol) if baseline_vol > 0 else 0.0

        eligible_liquidity = _is_short_term_liquid_enough(baseline_vol, latest_vol)
        eligible_spike = _is_short_term_volume_spike(spike)
        write_cache_t0 = time.perf_counter()
        _write_liquidity_gate_cache(
            symbol=symbol,
            exchange=symbol_exchange,
            baseline_vol=baseline_vol,
            latest_vol=latest_vol,
            spike_ratio=spike,
            eligible_liquidity=eligible_liquidity,
            eligible_spike=eligible_spike,
        )
        _track_step("_write_liquidity_gate_cache", write_cache_t0)
        if not eligible_liquidity:
            skipped_low_liquidity += 1
            continue
        if not eligible_spike:
            skipped_no_volume_spike += 1
            continue

        exp_adj = 0.0
        exp_meta: dict[str, Any] = {"applied": False, "reason": "not_computed"}
        try:
            exp_adj, exp_meta = _experience_confidence_adjustment(symbol=symbol, action="BUY")
        except Exception:
            pass
        gate_thresholds = _entry_gate_thresholds_from_experience(exp_meta, market_regime=market_regime)
        if str(gate_thresholds.get("experience_tightening_level") or "").startswith("claude_"):
            experience_threshold_source_claude += 1
        else:
            experience_threshold_source_heuristic += 1
        action = "HOLD"
        confidence = 45.0
        reason = "Short-term conditions not strong enough."
        gate_pass, gate_meta = _short_term_entry_gate(
            closes=closes,
            volumes=volumes,
            spike=spike,
            last_close=last_close,
            ema20_proxy=ema20_proxy,
            min_spike_ratio=float(gate_thresholds["min_spike_ratio"]),
            min_momentum_5d_pct=float(gate_thresholds["min_momentum_5d_pct"]),
            max_distance_from_ema20_pct=float(gate_thresholds["max_distance_from_ema20_pct"]),
            rsi_min=float(technical_thresholds["rsi_min"]),
            rsi_max=float(technical_thresholds["rsi_max"]),
            macd_min_line=float(technical_thresholds["macd_min_line"]),
        )
        if gate_pass:
            action = "BUY"
            confidence = min(85.0, 55.0 + spike * 10.0)
            reason = (
                f"Entry gate passed: spike {spike:.2f}x, breakout and momentum confirmed "
                "with price above EMA20 proxy."
            )
        else:
            skipped_entry_gate += 1

        technical = score_short_term_technical(
            spike=spike,
            last_close=last_close,
            ema20_proxy=ema20_proxy,
            closes=closes,
            volumes=volumes,
        )
        news_fetch_t0 = time.perf_counter()
        news_items = get_symbol_news_for_scoring(symbol, symbol_exchange, limit=30)
        _track_step("get_symbol_news_for_scoring", news_fetch_t0)
        news_score_t0 = time.perf_counter()
        news = score_news_for_symbol(symbol, news_items)
        _track_step("score_news_for_symbol", news_score_t0)
        macro_t0 = time.perf_counter()
        macro = score_macro_fundamental_proxy(
            symbol,
            vnstock_api_service.call_quote,
            vnstock_api_service.call_company,
            None,
            benchmark_closes=bench_closes or None,
            api_call_trading=vnstock_api_service.call_trading,
        )
        _track_step("score_macro_fundamental_proxy", macro_t0)
        meta_t0 = time.perf_counter()
        metadata = build_signal_quality_metadata(
            strategy_kind="SHORT_TERM",
            technical=technical,
            news=news,
            macro=macro,
            weights=None,
            legacy_flat={"volume_spike_ratio": round(spike, 4)},
        )
        metadata["entry_gate"] = gate_meta
        metadata["experience_entry_tightening"] = gate_thresholds
        metadata["market_regime_technical_thresholds"] = technical_thresholds
        cooldown_active, cooldown_meta = _experience_buy_cooldown(symbol=symbol, action=action)
        metadata["experience_buy_cooldown"] = cooldown_meta
        if action == "BUY" and cooldown_active:
            action = "HOLD"
            reason = "Recent stoploss cooldown active for symbol; downgraded to HOLD."
            skipped_experience_cooldown += 1
        _track_step("build_signal_quality_metadata", meta_t0)
        composite = float(metadata["signal_quality"]["composite_score_0_100"])
        if action == "BUY" and composite < dynamic_buy_floor:
            action = "HOLD"
            reason = (
                f"Composite {composite:.1f} below dynamic BUY floor {dynamic_buy_floor:.1f}; "
                "downgraded to HOLD."
            )
            skipped_dynamic_buy_floor += 1
        confidence = confidence_from_composite_and_action(
            action=action,
            base_confidence_from_rules=confidence,
            composite=composite,
            blend=0.5,
        )
        try:
            ai_scoring = _claude_scoring_analysis(
                strategy_type="SHORT_TERM",
                symbol=symbol,
                action=action,
                reason=reason,
                metadata=metadata,
            )
            if ai_scoring:
                metadata["claude_scoring_analysis"] = ai_scoring
                confidence = max(0.0, min(95.0, confidence + float(ai_scoring.get("confidence_adjustment") or 0.0)))
        except Exception:
            pass
        metadata["experience_scoring_adjustment"] = exp_meta
        confidence = max(0.0, min(95.0, confidence + float(exp_adj)))

        insert_t0 = time.perf_counter()
        if action in {"BUY", "SELL"}:
            signal = _insert_signal(
                "SHORT_TERM",
                symbol,
                action,
                last_close if action == "BUY" else None,
                (last_close * 1.04) if action == "BUY" else None,
                (last_close * 0.97) if action == "BUY" else None,
                confidence,
                reason,
                metadata,
            )
            _track_step("_insert_signal", insert_t0)
            signals.append(signal)
    total_scan_seconds = time.perf_counter() - scan_started_at
    step_rows = sorted(
        (
            {
                "name": name,
                "seconds": round(float(stats["seconds"]), 4),
                "calls": int(stats["calls"]),
                "avg_ms": round((float(stats["seconds"]) * 1000.0) / max(1, int(stats["calls"])), 2),
            }
            for name, stats in step_stats.items()
        ),
        key=lambda row: float(row["seconds"]),
        reverse=True,
    )
    logger.warning(
        "short_term_scan_timing_breakdown",
        extra={
            "scan_total_seconds": round(total_scan_seconds, 4),
            "exchange_scope": normalized_scope,
            "scanned": scanned,
            "signals_written": len(signals),
            "skipped_insufficient_data": skipped_insufficient_data,
            "skipped_low_liquidity": skipped_low_liquidity,
            "skipped_no_volume_spike": skipped_no_volume_spike,
            "skipped_entry_gate": skipped_entry_gate,
            "skipped_experience_cooldown": skipped_experience_cooldown,
            "skipped_dynamic_buy_floor": skipped_dynamic_buy_floor,
            "dynamic_buy_composite_floor": dynamic_buy_floor,
            "experience_threshold_source_claude": experience_threshold_source_claude,
            "experience_threshold_source_heuristic": experience_threshold_source_heuristic,
            "steps": step_rows,
        },
    )
    return {
        "signals": signals,
        "scanned": scanned,
        "skipped_insufficient_data": skipped_insufficient_data,
        "skipped_low_liquidity": skipped_low_liquidity,
        "skipped_no_volume_spike": skipped_no_volume_spike,
        "skipped_entry_gate": skipped_entry_gate,
        "skipped_experience_cooldown": skipped_experience_cooldown,
        "skipped_dynamic_buy_floor": skipped_dynamic_buy_floor,
        "dynamic_buy_composite_floor": dynamic_buy_floor,
        "experience_threshold_source_claude": experience_threshold_source_claude,
        "experience_threshold_source_heuristic": experience_threshold_source_heuristic,
        "exchange_scope": normalized_scope,
    }


def run_short_term_scan_batch_light(limit_symbols: int = 10, exchange_scope: str = "ALL") -> dict[str, Any]:
    """
    Lightweight fallback scanner:
    - fewer symbols (default up to 12: ~4 per exchange)
    - no heavy macro/fundamental fetches
    - no Claude scoring (latency budget)
    Used when full scanner exceeds hard timeout.
    """
    signals: list[dict[str, Any]] = []
    scanned = 0
    skipped_insufficient_data = 0
    skipped_low_liquidity = 0
    skipped_no_volume_spike = 0
    skipped_entry_gate = 0
    skipped_experience_cooldown = 0
    skipped_dynamic_buy_floor = 0
    experience_threshold_source_claude = 0
    experience_threshold_source_heuristic = 0
    scan_started_at = time.perf_counter()
    step_stats: dict[str, dict[str, float | int]] = {}

    def _track_step(name: str, started: float) -> None:
        elapsed = time.perf_counter() - started
        bucket = step_stats.setdefault(name, {"seconds": 0.0, "calls": 0})
        bucket["seconds"] = float(bucket["seconds"]) + float(elapsed)
        bucket["calls"] = int(bucket["calls"]) + 1

    requested = int(limit_symbols)
    normalized_scope = _normalize_exchange_scope(exchange_scope)
    if requested <= 0:
        # Unlimited mode: still use round-robin board order, but do not cap symbol count.
        symbol_batch = _get_scan_symbol_exchange_pairs_from_liquidity_cache(0, normalized_scope)
        listing_per_exchange_cap: int | None = None
        listing_target_symbols: int | None = None
    else:
        cap = int(settings.automation_short_term_scan_fallback_light_max_symbols)
        max_symbols = max(1, min(requested, cap))
        # Keep board balancing and preserve exchange metadata for per-symbol cache key.
        symbol_batch = _get_scan_symbol_exchange_pairs_from_liquidity_cache(max_symbols, normalized_scope)
        per_exchange = None
        listing_per_exchange_cap = per_exchange
        listing_target_symbols = max_symbols

    bench_t0 = time.perf_counter()
    bench_closes = fetch_symbol_daily_closes(
        vnstock_api_service.call_quote,
        "VNINDEX",
        90,
    )
    _track_step("fetch_symbol_daily_closes_benchmark", bench_t0)
    dynamic_buy_floor = _dynamic_short_term_buy_composite_floor(benchmark_closes=bench_closes)
    technical_thresholds = _technical_thresholds_from_market_regime(bench_closes)
    market_regime = str(technical_thresholds.get("regime") or "neutral")

    if not symbol_batch:
        raise RuntimeError(
            f"scanner_universe_empty: no symbols found in redis liquidity cache for scope={normalized_scope}"
        )

    for symbol, symbol_exchange in symbol_batch:
        scanned += 1
        cache_t0 = time.perf_counter()
        cached = _read_liquidity_gate_cache(symbol, symbol_exchange)
        _track_step("_read_liquidity_gate_cache", cache_t0)
        if isinstance(cached, dict):
            if not bool(cached.get("eligible_liquidity", True)):
                skipped_low_liquidity += 1
                continue
            if not bool(cached.get("eligible_spike", True)):
                skipped_no_volume_spike += 1
                continue
        cv_t0 = time.perf_counter()
        closes, volumes = _get_close_and_volume(symbol, bars=35, exchange=symbol_exchange)
        _track_step("_get_close_and_volume", cv_t0)
        if len(closes) < 22 or len(volumes) < 22:
            skipped_insufficient_data += 1
            continue
        last_close = closes[-1]
        ema20_proxy = mean(closes[-20:])
        baseline_vol = mean(volumes[-31:-1]) if len(volumes) >= 31 else 0.0
        latest_vol = volumes[-1]
        spike = (latest_vol / baseline_vol) if baseline_vol > 0 else 0.0

        eligible_liquidity = _is_short_term_liquid_enough(baseline_vol, latest_vol)
        eligible_spike = _is_short_term_volume_spike(spike)
        write_cache_t0 = time.perf_counter()
        _write_liquidity_gate_cache(
            symbol=symbol,
            exchange=symbol_exchange,
            baseline_vol=baseline_vol,
            latest_vol=latest_vol,
            spike_ratio=spike,
            eligible_liquidity=eligible_liquidity,
            eligible_spike=eligible_spike,
        )
        _track_step("_write_liquidity_gate_cache", write_cache_t0)
        if not eligible_liquidity:
            skipped_low_liquidity += 1
            continue
        if not eligible_spike:
            skipped_no_volume_spike += 1
            continue

        exp_adj = 0.0
        exp_meta: dict[str, Any] = {"applied": False, "reason": "not_computed"}
        try:
            exp_adj, exp_meta = _experience_confidence_adjustment(symbol=symbol, action="BUY")
        except Exception:
            pass
        gate_thresholds = _entry_gate_thresholds_from_experience(exp_meta, market_regime=market_regime)
        if str(gate_thresholds.get("experience_tightening_level") or "").startswith("claude_"):
            experience_threshold_source_claude += 1
        else:
            experience_threshold_source_heuristic += 1
        action = "HOLD"
        confidence = 45.0
        reason = "Short-term conditions not strong enough."
        gate_pass, gate_meta = _short_term_entry_gate(
            closes=closes,
            volumes=volumes,
            spike=spike,
            last_close=last_close,
            ema20_proxy=ema20_proxy,
            min_spike_ratio=float(gate_thresholds["min_spike_ratio"]),
            min_momentum_5d_pct=float(gate_thresholds["min_momentum_5d_pct"]),
            max_distance_from_ema20_pct=float(gate_thresholds["max_distance_from_ema20_pct"]),
            rsi_min=float(technical_thresholds["rsi_min"]),
            rsi_max=float(technical_thresholds["rsi_max"]),
            macd_min_line=float(technical_thresholds["macd_min_line"]),
        )
        if gate_pass:
            action = "BUY"
            confidence = min(82.0, 55.0 + spike * 9.0)
            reason = (
                f"Entry gate passed: spike {spike:.2f}x, breakout and momentum confirmed "
                "with price above EMA20 proxy."
            )
        else:
            skipped_entry_gate += 1

        technical = score_short_term_technical(
            spike=spike,
            last_close=last_close,
            ema20_proxy=ema20_proxy,
            closes=closes,
            volumes=volumes,
        )
        news_fetch_t0 = time.perf_counter()
        news_items = get_symbol_news_for_scoring(symbol, symbol_exchange, limit=30)
        _track_step("get_symbol_news_for_scoring", news_fetch_t0)
        news_score_t0 = time.perf_counter()
        news = score_news_for_symbol(symbol, news_items)
        _track_step("score_news_for_symbol", news_score_t0)
        macro = {
            "score_0_100": 50.0,
            "detail": {"mode": "fallback_light", "reason": "full_macro_disabled_due_timeout"},
        }
        metadata = build_signal_quality_metadata(
            strategy_kind="SHORT_TERM",
            technical=technical,
            news=news,
            macro=macro,
            weights=None,
            legacy_flat={"volume_spike_ratio": round(spike, 4), "scan_mode": "fallback_light"},
        )
        metadata["entry_gate"] = gate_meta
        metadata["experience_entry_tightening"] = gate_thresholds
        metadata["market_regime_technical_thresholds"] = technical_thresholds
        cooldown_active, cooldown_meta = _experience_buy_cooldown(symbol=symbol, action=action)
        metadata["experience_buy_cooldown"] = cooldown_meta
        if action == "BUY" and cooldown_active:
            action = "HOLD"
            reason = "Recent stoploss cooldown active for symbol; downgraded to HOLD."
            skipped_experience_cooldown += 1
        composite = float(metadata["signal_quality"]["composite_score_0_100"])
        if action == "BUY" and composite < dynamic_buy_floor:
            action = "HOLD"
            reason = (
                f"Composite {composite:.1f} below dynamic BUY floor {dynamic_buy_floor:.1f}; "
                "downgraded to HOLD."
            )
            skipped_dynamic_buy_floor += 1
        confidence = confidence_from_composite_and_action(
            action=action,
            base_confidence_from_rules=confidence,
            composite=composite,
            blend=0.45,
        )
        metadata["experience_scoring_adjustment"] = exp_meta
        confidence = max(0.0, min(95.0, confidence + float(exp_adj)))

        insert_t0 = time.perf_counter()
        if action in {"BUY", "SELL"}:
            signal = _insert_signal(
                "SHORT_TERM",
                symbol,
                action,
                last_close if action == "BUY" else None,
                (last_close * 1.04) if action == "BUY" else None,
                (last_close * 0.97) if action == "BUY" else None,
                confidence,
                reason,
                metadata,
            )
            _track_step("_insert_signal", insert_t0)
            signals.append(signal)
    total_scan_seconds = time.perf_counter() - scan_started_at
    step_rows = sorted(
        (
            {
                "name": name,
                "seconds": round(float(stats["seconds"]), 4),
                "calls": int(stats["calls"]),
                "avg_ms": round((float(stats["seconds"]) * 1000.0) / max(1, int(stats["calls"])), 2),
            }
            for name, stats in step_stats.items()
        ),
        key=lambda row: float(row["seconds"]),
        reverse=True,
    )
    logger.warning(
        "short_term_scan_light_timing_breakdown",
        extra={
            "scan_total_seconds": round(total_scan_seconds, 4),
            "exchange_scope": normalized_scope,
            "scanned": scanned,
            "signals_written": len(signals),
            "skipped_insufficient_data": skipped_insufficient_data,
            "skipped_low_liquidity": skipped_low_liquidity,
            "skipped_no_volume_spike": skipped_no_volume_spike,
            "skipped_entry_gate": skipped_entry_gate,
            "skipped_experience_cooldown": skipped_experience_cooldown,
            "skipped_dynamic_buy_floor": skipped_dynamic_buy_floor,
            "dynamic_buy_composite_floor": dynamic_buy_floor,
            "experience_threshold_source_claude": experience_threshold_source_claude,
            "experience_threshold_source_heuristic": experience_threshold_source_heuristic,
            "steps": step_rows,
        },
    )
    return {
        "signals": signals,
        "scanned": scanned,
        "skipped_insufficient_data": skipped_insufficient_data,
        "skipped_low_liquidity": skipped_low_liquidity,
        "skipped_no_volume_spike": skipped_no_volume_spike,
        "skipped_entry_gate": skipped_entry_gate,
        "skipped_experience_cooldown": skipped_experience_cooldown,
        "skipped_dynamic_buy_floor": skipped_dynamic_buy_floor,
        "dynamic_buy_composite_floor": dynamic_buy_floor,
        "experience_threshold_source_claude": experience_threshold_source_claude,
        "experience_threshold_source_heuristic": experience_threshold_source_heuristic,
        "scan_mode": "fallback_light",
        "exchange_scope": normalized_scope,
        "listing_target_symbols": listing_target_symbols if listing_target_symbols is not None else scanned,
        "listing_candidates_total": len(symbol_batch),
        "listing_per_exchange_cap": listing_per_exchange_cap,
    }


def run_short_term(limit_symbols: int = 0, exchange_scope: str = "ALL") -> list[dict[str, Any]]:
    return run_short_term_scan_batch(limit_symbols, exchange_scope)["signals"]


def run_long_term(limit_symbols: int = 0, exchange_scope: str = "ALL") -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    news_snapshot = fetch_news_snapshot_for_scoring(default_news_aggregate_callable())
    news_items: list[dict[str, Any]] = list(news_snapshot.get("items") or [])
    bench_closes = fetch_symbol_daily_closes(
        vnstock_api_service.call_quote,
        "VNINDEX",
        90,
    )

    for symbol, symbol_exchange in _get_scan_symbol_exchange_pairs_from_liquidity_cache(
        limit_symbols, _normalize_exchange_scope(exchange_scope)
    ):
        try:
            _persist_symbol_news_rows(symbol=symbol, exchange=symbol_exchange, news_items=news_items)
        except Exception as exc:
            logger.warning(
                "persist_symbol_news_rows_failed",
                extra={"symbol": symbol, "exchange": symbol_exchange, "error": str(exc)},
            )
        closes, volumes = _get_close_and_volume(symbol, bars=180, exchange=symbol_exchange)
        if len(closes) < 120:
            continue
        if len(volumes) < 31:
            continue
        baseline_vol = mean(volumes[-31:-1]) if len(volumes) >= 31 else 0.0
        latest_vol = volumes[-1] if volumes else 0.0
        if not _is_short_term_liquid_enough(baseline_vol, latest_vol):
            continue
        last_close = closes[-1]
        ma60 = mean(closes[-60:])
        ma120 = mean(closes[-120:])
        momentum = ((last_close - ma120) / ma120 * 100.0) if ma120 > 0 else 0.0

        action = "HOLD"
        confidence = 50.0
        reason = "Long-term trend not confirmed."
        if last_close > ma60 > ma120 and momentum >= 8:
            action = "BUY"
            confidence = min(90.0, 60.0 + momentum * 1.5)
            reason = f"Long-term uptrend confirmed (MA60>MA120, momentum {momentum:.2f}%)."

        technical = score_long_term_technical(
            last_close=last_close,
            ma60=ma60,
            ma120=ma120,
            momentum_pct=momentum,
        )
        news = score_news_for_symbol(symbol, news_items)
        macro = score_macro_fundamental_proxy(
            symbol,
            vnstock_api_service.call_quote,
            vnstock_api_service.call_company,
            None,
            benchmark_closes=bench_closes or None,
            api_call_trading=vnstock_api_service.call_trading,
        )
        metadata = build_signal_quality_metadata(
            strategy_kind="LONG_TERM",
            technical=technical,
            news=news,
            macro=macro,
            weights=None,
            legacy_flat={"momentum_percent": round(momentum, 4)},
        )
        feed_errors = news_snapshot.get("feed_errors") or {}
        if feed_errors:
            metadata["news_feed_errors_count"] = len(feed_errors)

        composite = float(metadata["signal_quality"]["composite_score_0_100"])
        confidence = confidence_from_composite_and_action(
            action=action,
            base_confidence_from_rules=confidence,
            composite=composite,
            blend=0.45,
        )
        try:
            ai_scoring = _claude_scoring_analysis(
                strategy_type="LONG_TERM",
                symbol=symbol,
                action=action,
                reason=reason,
                metadata=metadata,
            )
            if ai_scoring:
                metadata["claude_scoring_analysis"] = ai_scoring
                confidence = max(0.0, min(95.0, confidence + float(ai_scoring.get("confidence_adjustment") or 0.0)))
        except Exception:
            pass

        signal = _insert_signal(
            "LONG_TERM",
            symbol,
            action,
            last_close if action == "BUY" else None,
            (last_close * 1.12) if action == "BUY" else None,
            (last_close * 0.9) if action == "BUY" else None,
            confidence,
            reason,
            metadata,
        )
        signals.append(signal)
    return signals


def run_technical(limit_symbols: int = 0, exchange_scope: str = "ALL") -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    news_snapshot = fetch_news_snapshot_for_scoring(default_news_aggregate_callable())
    news_items: list[dict[str, Any]] = list(news_snapshot.get("items") or [])
    bench_closes = fetch_symbol_daily_closes(
        vnstock_api_service.call_quote,
        "VNINDEX",
        90,
    )

    for symbol, symbol_exchange in _get_scan_symbol_exchange_pairs_from_liquidity_cache(
        limit_symbols, _normalize_exchange_scope(exchange_scope)
    ):
        try:
            _persist_symbol_news_rows(symbol=symbol, exchange=symbol_exchange, news_items=news_items)
        except Exception as exc:
            logger.warning(
                "persist_symbol_news_rows_failed",
                extra={"symbol": symbol, "exchange": symbol_exchange, "error": str(exc)},
            )
        closes, volumes = _get_close_and_volume(symbol, bars=220, exchange=symbol_exchange)
        if len(closes) < 200:
            continue
        if len(volumes) < 31:
            continue
        baseline_vol = mean(volumes[-31:-1]) if len(volumes) >= 31 else 0.0
        latest_vol = volumes[-1] if volumes else 0.0
        if not _is_short_term_liquid_enough(baseline_vol, latest_vol):
            continue
        last_close = closes[-1]
        ema20 = mean(closes[-20:])
        ema50 = mean(closes[-50:])
        ema200 = mean(closes[-200:])

        action = "HOLD"
        confidence = 48.0
        reason = "Technical trend alignment not met."
        if ema20 > ema50 > ema200 and last_close >= ema20:
            action = "BUY"
            confidence = 72.0
            reason = "EMA20 > EMA50 > EMA200 with price near trend support."

        technical = score_technical_strategy_module(
            last_close=last_close,
            ema20=ema20,
            ema50=ema50,
            ema200=ema200,
        )
        news = score_news_for_symbol(symbol, news_items)
        macro = score_macro_fundamental_proxy(
            symbol,
            vnstock_api_service.call_quote,
            vnstock_api_service.call_company,
            None,
            benchmark_closes=bench_closes or None,
            api_call_trading=vnstock_api_service.call_trading,
        )
        metadata = build_signal_quality_metadata(
            strategy_kind="TECHNICAL",
            technical=technical,
            news=news,
            macro=macro,
            weights=None,
            legacy_flat={
                "ema20": round(ema20, 4),
                "ema50": round(ema50, 4),
                "ema200": round(ema200, 4),
            },
        )
        feed_errors = news_snapshot.get("feed_errors") or {}
        if feed_errors:
            metadata["news_feed_errors_count"] = len(feed_errors)

        composite = float(metadata["signal_quality"]["composite_score_0_100"])
        confidence = confidence_from_composite_and_action(
            action=action,
            base_confidence_from_rules=confidence,
            composite=composite,
            blend=0.5,
        )
        try:
            ai_scoring = _claude_scoring_analysis(
                strategy_type="TECHNICAL",
                symbol=symbol,
                action=action,
                reason=reason,
                metadata=metadata,
            )
            if ai_scoring:
                metadata["claude_scoring_analysis"] = ai_scoring
                confidence = max(0.0, min(95.0, confidence + float(ai_scoring.get("confidence_adjustment") or 0.0)))
        except Exception:
            pass

        signal = _insert_signal(
            "TECHNICAL",
            symbol,
            action,
            last_close if action == "BUY" else None,
            (last_close * 1.06) if action == "BUY" else None,
            (last_close * 0.96) if action == "BUY" else None,
            confidence,
            reason,
            metadata,
        )
        signals.append(signal)
    return signals


def warm_short_term_liquidity_cache(limit_symbols: int = 0, exchange_scope: str = "ALL") -> dict[str, Any]:
    """
    Pre-compute liquidity/spike gate cache in Redis for symbol universe.
    This does not insert signals; it only refreshes cache for scan pre-filtering.
    """
    normalized_scope = _normalize_exchange_scope(exchange_scope)
    scanned = 0
    cached_written = 0
    skipped_insufficient_data = 0
    low_liquidity = 0
    no_volume_spike = 0

    for symbol, symbol_exchange in _get_scan_symbol_exchange_pairs_round_robin(limit_symbols, normalized_scope):
        scanned += 1
        closes, volumes = _get_close_and_volume(symbol, bars=50, exchange=symbol_exchange)
        if len(closes) < 25 or len(volumes) < 31:
            skipped_insufficient_data += 1
            continue
        baseline_vol = mean(volumes[-31:-1]) if len(volumes) >= 31 else 0.0
        latest_vol = volumes[-1]
        spike = (latest_vol / baseline_vol) if baseline_vol > 0 else 0.0
        eligible_liquidity = _is_short_term_liquid_enough(baseline_vol, latest_vol)
        eligible_spike = _is_short_term_volume_spike(spike)
        _write_liquidity_gate_cache(
            symbol=symbol,
            exchange=symbol_exchange,
            baseline_vol=baseline_vol,
            latest_vol=latest_vol,
            spike_ratio=spike,
            eligible_liquidity=eligible_liquidity,
            eligible_spike=eligible_spike,
        )
        cached_written += 1
        if not eligible_liquidity:
            low_liquidity += 1
        elif not eligible_spike:
            no_volume_spike += 1

    return {
        "exchange_scope": normalized_scope,
        "scanned": scanned,
        "cached_written": cached_written,
        "skipped_insufficient_data": skipped_insufficient_data,
        "low_liquidity": low_liquidity,
        "no_volume_spike": no_volume_spike,
    }


def warm_daily_volume_for_saved_symbols(days: int = 30) -> dict[str, Any]:
    """
    Warm daily volume history in DB for all saved stock symbols.
    Data source: vnstock quote history (1D), count_back=`days`.
    """
    retention_days = max(5, int(settings.market_symbol_daily_volume_retention_days))
    safe_days = max(5, min(int(days), retention_days))
    cutoff_date = date.today() - timedelta(days=safe_days - 1)
    _ensure_market_symbol_tables_once()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT symbol, exchange
                FROM market_symbols
                ORDER BY symbol
                """
            )
            symbols = cur.fetchall()
            cur.execute(
                """
                SELECT DISTINCT symbol
                FROM market_symbol_daily_volume
                """
            )
            existing_symbol_rows = cur.fetchall()
        conn.commit()

    existing_symbols = {
        str(row.get("symbol", "")).strip().upper()
        for row in existing_symbol_rows
        if str(row.get("symbol", "")).strip()
    }

    scanned = 0
    warmed = 0
    errors = 0
    skipped_existing = 0
    for row in symbols:
        symbol = str(row.get("symbol", "")).strip().upper()
        exchange = str(row.get("exchange", "")).strip().upper()
        if not symbol:
            continue
        if symbol in existing_symbols:
            skipped_existing += 1
            continue
        scanned += 1
        try:
            rows = vnstock_api_service.call_quote(
                "history",
                source="VCI",
                symbol=symbol,
                method_kwargs={"interval": "1D", "count_back": safe_days},
            )
            if not isinstance(rows, list):
                continue
            dict_rows = [item for item in rows if isinstance(item, dict)]
            _persist_symbol_daily_volume_rows(
                symbol=symbol,
                exchange=exchange or "UNKNOWN",
                source="VCI",
                rows=dict_rows,
                min_trading_date=cutoff_date,
            )
            warmed += 1
        except Exception as exc:
            errors += 1
            logger.warning(
                "warm_daily_volume_for_saved_symbols_failed",
                extra={"symbol": symbol, "exchange": exchange, "error": str(exc)},
            )
            continue

    return {
        "days": safe_days,
        "cutoff_date": str(cutoff_date),
        "total_symbols": len(symbols),
        "symbols_skipped_existing": skipped_existing,
        "symbols_scanned": scanned,
        "symbols_warmed": warmed,
        "errors": errors,
    }


def refresh_short_term_liquidity_cache_from_db(days: int = 30, exchange_scope: str = "ALL") -> dict[str, Any]:
    """
    Rebuild short-term liquidity/spike Redis cache using persisted DB volume history only.
    Uses at most `days` latest sessions per symbol (default: 30), suitable for post-close refresh.
    """
    _ensure_market_symbol_tables_once()
    normalized_scope = _normalize_exchange_scope(exchange_scope)
    exchanges = _resolve_exchange_list(normalized_scope)
    safe_days = max(5, min(int(days), int(settings.market_symbol_daily_volume_retention_days)))
    cutoff_date = date.today() - timedelta(days=safe_days - 1)

    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT symbol, exchange, trading_date, volume
                FROM market_symbol_daily_volume
                WHERE trading_date >= %(cutoff_date)s
                  AND (%(all_scope)s OR exchange = ANY(%(exchanges)s))
                ORDER BY symbol ASC, trading_date DESC
                """,
                {
                    "cutoff_date": cutoff_date,
                    "all_scope": normalized_scope == "ALL",
                    "exchanges": list(exchanges),
                },
            )
            rows = cur.fetchall()
        conn.commit()

    deleted_old_keys = 0
    if normalized_scope == "ALL":
        deleted_old_keys += _redis_cache.delete_keys_by_pattern("scan:liquidity:*")
    else:
        for exchange in exchanges:
            deleted_old_keys += _redis_cache.delete_keys_by_pattern(f"scan:liquidity:{exchange}:*")

    volume_by_symbol: dict[str, list[float]] = defaultdict(list)
    exchange_by_symbol: dict[str, str] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        exchange = str(row.get("exchange", "")).strip().upper() or "UNKNOWN"
        volume = _to_float(row.get("volume"))
        if not symbol:
            continue
        volume_by_symbol[symbol].append(volume)
        exchange_by_symbol[symbol] = exchange

    scanned = 0
    cached_written = 0
    skipped_insufficient_data = 0
    low_liquidity = 0
    no_volume_spike = 0
    skipped_not_spike = 0

    for symbol, volumes in volume_by_symbol.items():
        # Need at least latest + 1 baseline point.
        if len(volumes) < 2:
            skipped_insufficient_data += 1
            continue
        latest_vol = float(volumes[0])
        baseline_slice = [float(v) for v in volumes[1:] if float(v) >= 0]
        if not baseline_slice:
            skipped_insufficient_data += 1
            continue

        scanned += 1
        baseline_vol = float(mean(baseline_slice))
        spike = (latest_vol / baseline_vol) if baseline_vol > 0 else 0.0
        eligible_liquidity = _is_short_term_liquid_enough(baseline_vol, latest_vol)
        eligible_spike = _is_short_term_volume_spike(spike)
        if not eligible_spike:
            skipped_not_spike += 1
            no_volume_spike += 1
            if not eligible_liquidity:
                low_liquidity += 1
            continue
        _write_liquidity_gate_cache(
            symbol=symbol,
            exchange=exchange_by_symbol.get(symbol, "UNKNOWN"),
            baseline_vol=baseline_vol,
            latest_vol=latest_vol,
            spike_ratio=spike,
            eligible_liquidity=eligible_liquidity,
            eligible_spike=eligible_spike,
        )
        cached_written += 1
        if not eligible_liquidity:
            low_liquidity += 1

    return {
        "source": "market_symbol_daily_volume",
        "exchange_scope": normalized_scope,
        "days": safe_days,
        "cutoff_date": str(cutoff_date),
        "db_rows": len(rows),
        "deleted_old_redis_keys": deleted_old_keys,
        "symbols_scanned": scanned,
        "cached_written": cached_written,
        "skipped_insufficient_data": skipped_insufficient_data,
        "skipped_not_spike": skipped_not_spike,
        "low_liquidity": low_liquidity,
        "no_volume_spike": no_volume_spike,
    }


def scan_and_persist_symbol_news_from_liquidity_cache(
    *,
    exchange_scope: str = "ALL",
    max_symbols: int = 0,
    per_symbol_news_limit: int = 20,
) -> dict[str, Any]:
    """
    Read symbols from Redis liquidity keys and refresh company news into DB per symbol.
    Intended for post-close scheduler after liquidity keys are rebuilt.
    """
    normalized_scope = _normalize_exchange_scope(exchange_scope)
    symbol_pairs = _get_scan_symbol_exchange_pairs_from_liquidity_cache(int(max_symbols), normalized_scope)
    scanned = 0
    persisted_symbols = 0
    persisted_rows = 0
    errors = 0

    for symbol, exchange in symbol_pairs:
        scanned += 1
        try:
            normalized_news = _fetch_company_news_for_symbol(symbol)[: max(1, int(per_symbol_news_limit))]
            if not normalized_news:
                continue

            written = persist_symbol_news_rows(
                symbol=symbol,
                exchange=exchange or "UNKNOWN",
                news_items=normalized_news,
                max_items=max(1, int(per_symbol_news_limit)),
            )
            if written > 0:
                persisted_symbols += 1
                persisted_rows += int(written)
        except Exception as exc:
            errors += 1
            logger.warning(
                "scan_and_persist_symbol_news_failed",
                extra={"symbol": symbol, "exchange": exchange, "error": str(exc)},
            )

    return {
        "exchange_scope": normalized_scope,
        "symbols_from_redis": len(symbol_pairs),
        "symbols_scanned": scanned,
        "symbols_persisted": persisted_symbols,
        "news_rows_persisted": persisted_rows,
        "errors": errors,
    }


def generate_all_signals() -> dict[str, Any]:
    short_term = run_short_term()
    long_term = run_long_term()
    technical = run_technical()
    return {
        "short_term_count": len(short_term),
        "long_term_count": len(long_term),
        "technical_count": len(technical),
        "total_count": len(short_term) + len(long_term) + len(technical),
    }


def get_signal_scoring_claude_runtime_metrics() -> dict[str, Any]:
    return _claude_service.get_runtime_metrics()


def list_signals(
    *,
    strategy_type: str | None = None,
    symbol: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    ensure_signals_table()
    filters = []
    params: dict[str, Any] = {"limit": max(1, min(limit, 500))}
    if strategy_type:
        filters.append("strategy_type = %(strategy_type)s")
        params["strategy_type"] = strategy_type
    if symbol:
        filters.append("symbol = %(symbol)s")
        params["symbol"] = symbol.upper()
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = f"""
    SELECT *
    FROM signals
    {where_clause}
    ORDER BY created_at DESC
    LIMIT %(limit)s
    """
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return [dict(r) for r in rows]
