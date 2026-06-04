from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.types.json import Json

from app.core.config import settings
from app.services.fundamental_scoring_service import DISCLAIMER, score_long_term_stock
from app.services.gpt_service import GptService
from app.services.macro_service import get_macro_regime
from app.services.news_mail_service import ensure_news_mail_tables
from app.services.vnstock_api_service import VNStockApiService

vnstock_api_service = VNStockApiService()
gpt_service = GptService()

_TABLES_READY = False


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


def _normalize_symbol(value: Any) -> str:
    return "".join(ch for ch in str(value or "").strip().upper() if ch.isalnum())[:20]


def _rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        return [payload]
    return []


_FINANCIAL_ROW_META_KEYS = {"item", "item_en", "item_id"}
_GPT_FINANCIAL_METRIC_IDS: dict[str, set[str]] = {
    "ratio_rows": {
        "pe",
        "pe_ratio",
        "pb",
        "pb_ratio",
        "roe",
        "roa",
        "de",
        "debtPerEquity",
        "debt_to_equity",
    },
    "income_rows": {
        "revenue",
        "net_sales",
        "operating_sales",
        "sales",
        "doanh_thu_thuan",
        "profit_after_tax",
        "net_profit",
        "net_profit_loss_after_tax",
        "post_tax_profit",
        "lnst",
        "attributable_to_parent_company",
    },
    "cash_flow_rows": {
        "net_cash_flow_from_operating_activities",
        "net_cash_inflows_outflows_from_operating_activities",
        "cashflow_from_operating",
        "cfo",
    },
}


def _is_financial_period_key(value: Any) -> bool:
    return bool(re.fullmatch(r"\d{4}(?:-Q[1-4])?", str(value or "").strip()))


def _period_sort_key(value: Any) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})(?:-Q([1-4]))?", str(value or "").strip())
    if not match:
        return (0, 0)
    return (int(match.group(1)), int(match.group(2) or 0))


def _normalize_financial_statement_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    VCI financial endpoints may return metric rows with period columns:
    {item_id: "net_sales", "2026-Q1": 1, "2025-Q4": 2}.
    The scorer expects period rows:
    {period: "2026-Q1", net_sales: 1}.
    """
    if not rows:
        return []
    if not any("item_id" in row and any(_is_financial_period_key(key) for key in row) for row in rows):
        return rows

    period_rows: dict[str, dict[str, Any]] = {}
    for row in rows:
        item_id = str(row.get("item_id") or "").strip()
        if not item_id:
            continue
        for key, value in row.items():
            if key in _FINANCIAL_ROW_META_KEYS or not _is_financial_period_key(key):
                continue
            period_rows.setdefault(str(key), {"period": str(key)})[item_id] = value

    return [period_rows[period] for period in sorted(period_rows, key=_period_sort_key, reverse=True)]


def _compact_financial_rows_for_gpt(
    rows: list[dict[str, Any]],
    *,
    wanted_item_ids: set[str],
    period_limit: int = 6,
) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    wanted_lower = {item.lower() for item in wanted_item_ids}
    for row in rows:
        item_id = str(row.get("item_id") or "").strip()
        if item_id and item_id.lower() not in wanted_lower:
            continue
        period_keys = [key for key in row if _is_financial_period_key(key)]
        if period_keys:
            values: dict[str, Any] = {}
            for period in sorted(period_keys, key=_period_sort_key, reverse=True)[:period_limit]:
                value = row.get(period)
                if _to_float(value) is not None or isinstance(value, str):
                    values[str(period)] = value
            if values:
                compact.append(
                    {
                        "item_id": item_id,
                        "item": str(row.get("item") or "")[:120],
                        "item_en": str(row.get("item_en") or "")[:120],
                        "values": values,
                    }
                )
            continue

        keys = {str(key).lower() for key in row}
        if wanted_lower.intersection(keys):
            compact.append({str(key): value for key, value in row.items() if str(key).lower() in wanted_lower})
    return compact[:80]


def _first_number(mapping: dict[str, Any] | None, *keys: str) -> float | None:
    if not isinstance(mapping, dict):
        return None
    lowered = {str(k).lower(): v for k, v in mapping.items()}
    for key in keys:
        number = _to_float(lowered.get(key.lower()))
        if number is not None:
            return number
    return None


def _first_text(mapping: dict[str, Any] | None, *keys: str) -> str:
    if not isinstance(mapping, dict):
        return ""
    lowered = {str(k).lower(): v for k, v in mapping.items()}
    for key in keys:
        value = lowered.get(key.lower())
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def ensure_long_term_research_tables() -> None:
    global _TABLES_READY
    if _TABLES_READY:
        return
    ddl = """
    CREATE TABLE IF NOT EXISTS long_term_research_runs (
        id UUID PRIMARY KEY,
        mode VARCHAR(16) NOT NULL CHECK (mode IN ('AUTO', 'MANUAL')),
        run_status VARCHAR(32) NOT NULL,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMPTZ NULL,
        universe_exchange VARCHAR(16) NOT NULL DEFAULT 'HOSE',
        universe_size INTEGER NOT NULL DEFAULT 0 CHECK (universe_size >= 0),
        scored_count INTEGER NOT NULL DEFAULT 0 CHECK (scored_count >= 0),
        error TEXT NULL,
        params JSONB NOT NULL DEFAULT '{}'::jsonb,
        macro_context JSONB NOT NULL DEFAULT '{}'::jsonb
    );
    CREATE INDEX IF NOT EXISTS idx_long_term_research_runs_started
    ON long_term_research_runs(started_at DESC);
    CREATE TABLE IF NOT EXISTS stock_universe_snapshots (
        id UUID PRIMARY KEY,
        run_id UUID NOT NULL REFERENCES long_term_research_runs(id) ON DELETE CASCADE,
        exchange VARCHAR(16) NOT NULL DEFAULT 'HOSE',
        symbol VARCHAR(20) NOT NULL,
        sector TEXT NULL,
        market_cap DOUBLE PRECISION NULL,
        market_cap_source VARCHAR(32) NOT NULL DEFAULT 'unknown',
        latest_close DOUBLE PRECISION NULL,
        issue_share DOUBLE PRECISION NULL,
        data_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
        raw_overview JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (run_id, symbol)
    );
    CREATE INDEX IF NOT EXISTS idx_stock_universe_snapshots_run_cap
    ON stock_universe_snapshots(run_id, market_cap DESC NULLS LAST);
    CREATE TABLE IF NOT EXISTS long_term_stock_scores (
        id UUID PRIMARY KEY,
        run_id UUID NOT NULL REFERENCES long_term_research_runs(id) ON DELETE CASCADE,
        symbol VARCHAR(20) NOT NULL,
        exchange VARCHAR(16) NULL,
        sector TEXT NULL,
        market_cap DOUBLE PRECISION NULL,
        rank INTEGER NULL CHECK (rank IS NULL OR rank > 0),
        final_score DOUBLE PRECISION NOT NULL CHECK (final_score >= 0 AND final_score <= 100),
        rating TEXT NOT NULL,
        score_components JSONB NOT NULL DEFAULT '{}'::jsonb,
        risk_penalty DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (risk_penalty >= 0 AND risk_penalty <= 20),
        macro_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        sector_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        news_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        data_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
        disclaimer TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (run_id, symbol)
    );
    CREATE INDEX IF NOT EXISTS idx_long_term_stock_scores_run_rank
    ON long_term_stock_scores(run_id, rank ASC NULLS LAST);
    CREATE INDEX IF NOT EXISTS idx_long_term_stock_scores_symbol_created
    ON long_term_stock_scores(symbol, created_at DESC);
    CREATE TABLE IF NOT EXISTS long_term_symbol_analyses (
        id UUID PRIMARY KEY,
        symbol VARCHAR(20) NOT NULL,
        mode VARCHAR(16) NOT NULL CHECK (mode IN ('AUTO', 'MANUAL')),
        run_id UUID NULL REFERENCES long_term_research_runs(id) ON DELETE SET NULL,
        final_score DOUBLE PRECISION NOT NULL CHECK (final_score >= 0 AND final_score <= 100),
        rating TEXT NOT NULL,
        score_components JSONB NOT NULL DEFAULT '{}'::jsonb,
        risk_penalty DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (risk_penalty >= 0 AND risk_penalty <= 20),
        macro_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        sector_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        news_context JSONB NOT NULL DEFAULT '{}'::jsonb,
        ai_thesis TEXT NOT NULL DEFAULT '',
        catalysts JSONB NOT NULL DEFAULT '[]'::jsonb,
        risks JSONB NOT NULL DEFAULT '[]'::jsonb,
        data_gaps JSONB NOT NULL DEFAULT '[]'::jsonb,
        disclaimer TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_long_term_symbol_analyses_symbol_created
    ON long_term_symbol_analyses(symbol, created_at DESC);
    """
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    _TABLES_READY = True


def _extract_listing_symbols(payload: Any, exchange: str = "HOSE") -> list[dict[str, str]]:
    rows = _rows(payload)
    out: list[dict[str, str]] = []
    for row in rows:
        symbol = _normalize_symbol(row.get("symbol") or row.get("code") or row.get("ticker"))
        if not symbol:
            continue
        row_exchange = _first_text(row, "exchange", "board", "com_group_code", "comGroupCode")
        if row_exchange and row_exchange.upper() != exchange.upper():
            continue
        out.append({"symbol": symbol, "exchange": row_exchange.upper() if row_exchange else exchange.upper()})
    seen: set[str] = set()
    deduped: list[dict[str, str]] = []
    for row in out:
        if row["symbol"] in seen:
            continue
        seen.add(row["symbol"])
        deduped.append(row)
    return deduped


def _fetch_company_overview(symbol: str) -> dict[str, Any]:
    raw = vnstock_api_service.call_company("overview", source="VCI", symbol=symbol, show_log=False)
    rows = _rows(raw)
    if rows:
        return rows[0]
    return raw if isinstance(raw, dict) else {}


def _fetch_latest_close(symbol: str) -> float | None:
    end = date.today()
    start = end - timedelta(days=21)
    raw = vnstock_api_service.call_quote(
        "history",
        source="VCI",
        symbol=symbol,
        show_log=False,
        method_kwargs={"interval": "1D", "start": start.isoformat(), "end": end.isoformat()},
    )
    closes = [_to_float(row.get("close")) for row in _rows(raw)]
    closes = [close for close in closes if close is not None and close > 0]
    return closes[-1] if closes else None


def _market_cap_from_overview(
    overview: dict[str, Any],
    latest_close: float | None,
) -> tuple[float | None, str, list[str]]:
    data_gaps: list[str] = []
    market_cap = _first_number(overview, "market_cap", "marketcap", "market_capitalization")
    if market_cap is not None and market_cap > 0:
        return market_cap, "overview", data_gaps
    issue_share = _first_number(overview, "issue_share", "financial_ratio_issue_share")
    if latest_close is not None and issue_share is not None and latest_close > 0 and issue_share > 0:
        return latest_close * issue_share, "latest_close_x_issue_share", ["market_cap_missing_used_fallback"]
    return None, "missing", ["market_cap_missing"]


def build_hose_universe(*, universe_size: int = 100, candidate_limit: int = 400) -> list[dict[str, Any]]:
    target_size = max(1, min(int(universe_size), 100))
    cap = max(target_size, min(int(candidate_limit), 600))
    raw_listing: Any
    try:
        raw_listing = vnstock_api_service.call_listing(
            "symbols_by_exchange",
            source="KBS",
            method_kwargs={"exchange": "HOSE"},
        )
    except Exception:
        raw_listing = vnstock_api_service.call_listing(
            "symbols_by_group",
            source="KBS",
            method_kwargs={"group": "HOSE"},
        )
    symbols = _extract_listing_symbols(raw_listing, "HOSE")[:cap]
    enriched: list[dict[str, Any]] = []
    for item in symbols:
        symbol = item["symbol"]
        overview: dict[str, Any] = {}
        latest_close: float | None = None
        data_gaps: list[str] = []
        try:
            overview = _fetch_company_overview(symbol)
        except Exception as exc:
            data_gaps.append(f"overview_fetch_failed:{type(exc).__name__}")
        market_cap = _first_number(overview, "market_cap", "marketcap", "market_capitalization")
        if market_cap is None:
            try:
                latest_close = _fetch_latest_close(symbol)
            except Exception as exc:
                data_gaps.append(f"latest_close_fetch_failed:{type(exc).__name__}")
        cap_value, cap_source, cap_gaps = _market_cap_from_overview(overview, latest_close)
        data_gaps.extend(cap_gaps)
        enriched.append(
            {
                "symbol": symbol,
                "exchange": item.get("exchange") or "HOSE",
                "sector": _first_text(overview, "industry", "sector", "icb_name3", "icb_name2", "icb_name4") or None,
                "market_cap": cap_value or 0.0,
                "market_cap_source": cap_source,
                "latest_close": latest_close,
                "issue_share": _first_number(overview, "issue_share", "financial_ratio_issue_share"),
                "data_gaps": sorted(set(data_gaps)),
                "raw_overview": overview,
            }
        )
    enriched.sort(key=lambda row: float(row.get("market_cap") or 0.0), reverse=True)
    selected = enriched[:target_size]
    for idx, row in enumerate(selected, start=1):
        row["universe_rank"] = idx
    return selected


def _read_macro_context() -> dict[str, Any]:
    try:
        macro = get_macro_regime(persist_snapshot=False)
        return {
            "regime": macro.get("regime"),
            "regime_score": macro.get("regime_score"),
            "components": macro.get("components") or {},
            "warnings": macro.get("warnings") or [],
            "data_gaps": macro.get("data_gaps") or [],
            "as_of": macro.get("as_of"),
        }
    except Exception as exc:
        return {
            "regime": "Recovery",
            "regime_score": 50.0,
            "components": {},
            "warnings": [f"macro_context_unavailable:{type(exc).__name__}"],
            "data_gaps": ["macro_context_unavailable"],
        }


def _read_news_context(symbol: str, limit: int = 20) -> list[dict[str, Any]]:
    try:
        ensure_news_mail_tables()
        with connect(settings.database_url, row_factory=dict_row) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT i.sentiment_label, i.sentiment_score, i.impact_score, i.confidence,
                           i.rationale, a.title, a.url, a.source_host, a.updated_at
                    FROM news_mail_symbol_impacts i
                    JOIN news_mail_articles a ON a.id = i.article_id
                    WHERE i.symbol = %(symbol)s
                    ORDER BY i.created_at DESC
                    LIMIT %(limit)s
                    """,
                    {"symbol": symbol, "limit": max(1, min(int(limit), 100))},
                )
                return [_json_safe(dict(row)) for row in cur.fetchall()]
    except Exception:
        return []


_GPT_FINANCIAL_PARSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "latest_ratio": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "pe": {"type": ["number", "null"]},
                "pb": {"type": ["number", "null"]},
                "roe": {"type": ["number", "null"]},
                "roa": {"type": ["number", "null"]},
                "debt_to_equity": {"type": ["number", "null"]},
            },
            "required": ["pe", "pb", "roe", "roa", "debt_to_equity"],
        },
        "income_rows": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "period": {"type": "string"},
                    "revenue": {"type": ["number", "null"]},
                    "profit_after_tax": {"type": ["number", "null"]},
                },
                "required": ["period", "revenue", "profit_after_tax"],
            },
        },
        "cash_flow_rows": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "period": {"type": "string"},
                    "net_cash_flow_from_operating_activities": {"type": ["number", "null"]},
                },
                "required": ["period", "net_cash_flow_from_operating_activities"],
            },
        },
        "data_gaps": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["latest_ratio", "income_rows", "cash_flow_rows", "data_gaps"],
}


def _nonnull_number_dict(row: dict[str, Any], keys: tuple[str, ...]) -> dict[str, float]:
    out: dict[str, float] = {}
    for key in keys:
        value = _to_float(row.get(key))
        if value is not None:
            out[key] = value
    return out


def _coerce_gpt_financial_parse(parsed: dict[str, Any]) -> dict[str, Any]:
    ratio = parsed.get("latest_ratio") if isinstance(parsed.get("latest_ratio"), dict) else {}
    ratio_row = _nonnull_number_dict(
        ratio,
        ("pe", "pb", "roe", "roa", "debt_to_equity"),
    )
    income_rows: list[dict[str, Any]] = []
    raw_income_rows = parsed.get("income_rows")
    income_source = raw_income_rows if isinstance(raw_income_rows, list) else []
    for row in income_source:
        if not isinstance(row, dict):
            continue
        period = str(row.get("period") or "").strip()
        values = _nonnull_number_dict(row, ("revenue", "profit_after_tax"))
        if period and values:
            income_rows.append({"period": period, **values})
    cash_flow_rows: list[dict[str, Any]] = []
    raw_cash_flow_rows = parsed.get("cash_flow_rows")
    cash_flow_source = raw_cash_flow_rows if isinstance(raw_cash_flow_rows, list) else []
    for row in cash_flow_source:
        if not isinstance(row, dict):
            continue
        period = str(row.get("period") or "").strip()
        values = _nonnull_number_dict(row, ("net_cash_flow_from_operating_activities",))
        if period and values:
            cash_flow_rows.append({"period": period, **values})
    data_gaps = [str(item) for item in parsed.get("data_gaps") or [] if str(item).strip()]
    return {
        "ratio_rows": [ratio_row] if ratio_row else [],
        "income_rows": income_rows[:8],
        "cash_flow_rows": cash_flow_rows[:8],
        "data_gaps": data_gaps[:12],
    }


def _parse_financials_with_gpt(
    *,
    symbol: str,
    raw_financial_rows: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    if not settings.use_gpt:
        return None
    compact_payload = {
        "symbol": symbol,
        "ratio_rows": _compact_financial_rows_for_gpt(
            raw_financial_rows.get("ratio_rows") or [],
            wanted_item_ids=_GPT_FINANCIAL_METRIC_IDS["ratio_rows"],
        ),
        "income_rows": _compact_financial_rows_for_gpt(
            raw_financial_rows.get("income_rows") or [],
            wanted_item_ids=_GPT_FINANCIAL_METRIC_IDS["income_rows"],
        ),
        "cash_flow_rows": _compact_financial_rows_for_gpt(
            raw_financial_rows.get("cash_flow_rows") or [],
            wanted_item_ids=_GPT_FINANCIAL_METRIC_IDS["cash_flow_rows"],
        ),
    }
    if not any(compact_payload[key] for key in ("ratio_rows", "income_rows", "cash_flow_rows")):
        return None
    prompt = (
        "Parse compact VNStock VCI financial rows into scorer-ready metrics. "
        "Use only the supplied VNStock data; do not infer values from outside knowledge. "
        "Select the latest available period for ratios. For income and cash flow, return newest periods first. "
        "Map P/E to pe, P/B to pb, ROE to roe, ROA to roa, debt/equity to debt_to_equity, "
        "net sales or operating sales to revenue, net profit after tax to profit_after_tax, "
        "and net operating cash flow to net_cash_flow_from_operating_activities. "
        "If a metric is absent, use null and add a short data_gaps item.\n"
        f"Payload: {json.dumps(_json_safe(compact_payload), ensure_ascii=True)[:14000]}"
    )
    try:
        text = gpt_service.generate_text_with_resilience(
            prompt=prompt,
            system_prompt=(
                "You are a financial data parser. Return strict JSON only. "
                "Do not provide investment advice or narrative."
            ),
            model=settings.gpt_model,
            max_tokens=min(900, settings.gpt_max_tokens),
            temperature=0.0,
            cache_namespace="long_term_financial_parse",
            cache_ttl_seconds=86400,
            output_schema=_GPT_FINANCIAL_PARSE_SCHEMA,
        )
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return _coerce_gpt_financial_parse(parsed)
    except Exception:
        return None
    return None


def fetch_symbol_research_inputs(
    symbol: str,
    *,
    parse_financials_with_gpt: bool = False,
    overview_override: dict[str, Any] | None = None,
    latest_close_override: float | None = None,
    fetch_latest_close: bool = True,
) -> dict[str, Any]:
    sym = _normalize_symbol(symbol)
    data_gaps: list[str] = []
    overview: dict[str, Any] = overview_override or {}
    latest_close: float | None = latest_close_override
    ratio_rows: list[dict[str, Any]] = []
    income_rows: list[dict[str, Any]] = []
    balance_rows: list[dict[str, Any]] = []
    cash_flow_rows: list[dict[str, Any]] = []
    raw_financial_rows: dict[str, list[dict[str, Any]]] = {}
    if not overview:
        try:
            overview = _fetch_company_overview(sym)
        except Exception as exc:
            data_gaps.append(f"overview_fetch_failed:{type(exc).__name__}")
    if latest_close is None and fetch_latest_close:
        try:
            latest_close = _fetch_latest_close(sym)
        except Exception as exc:
            data_gaps.append(f"latest_close_fetch_failed:{type(exc).__name__}")
    for method, target in (
        ("ratio", "ratio_rows"),
        ("income_statement", "income_rows"),
        ("balance_sheet", "balance_rows"),
        ("cash_flow", "cash_flow_rows"),
    ):
        try:
            raw = vnstock_api_service.call_financial(method, source="VCI", symbol=sym, period="quarter", get_all=True, show_log=False)
            rows = _rows(raw)
        except Exception as exc:
            data_gaps.append(f"{method}_fetch_failed:{type(exc).__name__}")
            rows = []
        raw_financial_rows[target] = rows
        normalized_rows = _normalize_financial_statement_rows(rows)
        if target == "ratio_rows":
            ratio_rows = normalized_rows
        elif target == "income_rows":
            income_rows = normalized_rows
        elif target == "balance_rows":
            balance_rows = normalized_rows
        else:
            cash_flow_rows = normalized_rows
    if parse_financials_with_gpt:
        parsed = _parse_financials_with_gpt(symbol=sym, raw_financial_rows=raw_financial_rows)
        if parsed is not None:
            ratio_rows = parsed["ratio_rows"] or ratio_rows
            income_rows = parsed["income_rows"] or income_rows
            cash_flow_rows = parsed["cash_flow_rows"] or cash_flow_rows
            data_gaps.extend(parsed.get("data_gaps") or [])
    return {
        "overview": overview,
        "latest_close": latest_close,
        "ratio_rows": ratio_rows,
        "income_rows": income_rows,
        "balance_rows": balance_rows,
        "cash_flow_rows": cash_flow_rows,
        "data_gaps": data_gaps,
    }


_AI_THESIS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "ai_thesis": {"type": "string"},
        "catalysts": {"type": "array", "items": {"type": "string"}},
        "risks": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["ai_thesis", "catalysts", "risks"],
}


def _fallback_thesis(result: dict[str, Any]) -> str:
    return (
        f"{result['symbol']} đạt {result['final_score']}/100 ({result['rating']}). "
        "Mô hình định lượng là nguồn xếp hạng chính; phần diễn giải AI chỉ dùng làm bối cảnh nghiên cứu."
    )


def _build_ai_thesis(result: dict[str, Any]) -> dict[str, Any]:
    if not settings.use_gpt:
        return {"ai_thesis": _fallback_thesis(result), "catalysts": result["catalysts"], "risks": result["risks"]}
    prompt = (
        "Translate and rewrite the long-term equity research detail for a Vietnam-listed stock in Vietnamese. "
        "Return ai_thesis, catalysts, and risks in natural Vietnamese. "
        "Do not give buy/sell instructions. Use the deterministic score as the ranking truth. "
        "Preserve all numbers, scores, sector names, and risk facts from the payload. "
        "Do not add outside information.\n"
        f"Payload: {json.dumps(_json_safe(result), ensure_ascii=True)[:12000]}"
    )
    try:
        text = gpt_service.generate_text_with_resilience(
            prompt=prompt,
            system_prompt=(
                "You are a cautious Vietnam equity research translator. "
                "Return strict JSON only, with all user-facing text in Vietnamese."
            ),
            model=settings.gpt_model,
            max_tokens=min(900, settings.gpt_max_tokens),
            temperature=0.2,
            cache_namespace="long_term_ai_thesis",
            cache_ttl_seconds=3600,
            output_schema=_AI_THESIS_SCHEMA,
        )
        parsed = json.loads(text)
        if isinstance(parsed, dict) and isinstance(parsed.get("ai_thesis"), str):
            return {
                "ai_thesis": parsed.get("ai_thesis", "").strip() or _fallback_thesis(result),
                "catalysts": [str(x) for x in parsed.get("catalysts") or result["catalysts"]][:8],
                "risks": [str(x) for x in parsed.get("risks") or result["risks"]][:8],
            }
    except Exception:
        pass
    return {"ai_thesis": _fallback_thesis(result), "catalysts": result["catalysts"], "risks": result["risks"]}


def analyze_symbol_for_research(
    symbol: str,
    *,
    mode: str = "MANUAL",
    run_id: UUID | None = None,
    include_ai: bool = True,
    persist: bool = True,
) -> dict[str, Any]:
    sym = _normalize_symbol(symbol)
    if not sym:
        raise ValueError("symbol is required")
    inputs = fetch_symbol_research_inputs(sym)
    macro_context = _read_macro_context()
    news_items = _read_news_context(sym)
    result = score_long_term_stock(
        symbol=sym,
        overview=inputs.get("overview") or {},
        ratio_rows=inputs.get("ratio_rows") or [],
        income_rows=inputs.get("income_rows") or [],
        balance_rows=inputs.get("balance_rows") or [],
        cash_flow_rows=inputs.get("cash_flow_rows") or [],
        latest_close=inputs.get("latest_close"),
        macro_context=macro_context,
        news_items=news_items,
        seed_data_gaps=inputs.get("data_gaps") or [],
    )
    thesis = _build_ai_thesis(result) if include_ai else {"ai_thesis": "", "catalysts": result["catalysts"], "risks": result["risks"]}
    payload = {
        **result,
        "mode": mode.upper(),
        "run_id": str(run_id) if run_id else None,
        "ai_thesis": thesis["ai_thesis"],
        "catalysts": thesis["catalysts"],
        "risks": thesis["risks"],
    }
    if persist:
        persist_symbol_analysis(payload, run_id=run_id)
    return _json_safe(payload)


def _insert_run(run_id: UUID, params: dict[str, Any], macro_context: dict[str, Any]) -> None:
    ensure_long_term_research_tables()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO long_term_research_runs (
                    id, mode, run_status, started_at, universe_exchange, universe_size,
                    scored_count, params, macro_context
                ) VALUES (
                    %(id)s, 'AUTO', 'RUNNING', NOW(), 'HOSE', %(universe_size)s, 0,
                    %(params)s, %(macro_context)s
                )
                """,
                {
                    "id": run_id,
                    "universe_size": int(params.get("universe_size") or 100),
                    "params": Json(_json_safe(params)),
                    "macro_context": Json(_json_safe(macro_context)),
                },
            )
        conn.commit()


def _finish_run(run_id: UUID, *, status: str, scored_count: int, error: str | None = None) -> None:
    ensure_long_term_research_tables()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE long_term_research_runs
                SET run_status = %(status)s,
                    finished_at = NOW(),
                    scored_count = %(scored_count)s,
                    error = %(error)s
                WHERE id = %(id)s
                """,
                {"id": run_id, "status": status, "scored_count": scored_count, "error": error},
            )
        conn.commit()


def _persist_universe_row(run_id: UUID, row: dict[str, Any]) -> None:
    ensure_long_term_research_tables()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stock_universe_snapshots (
                    id, run_id, exchange, symbol, sector, market_cap, market_cap_source,
                    latest_close, issue_share, data_gaps, raw_overview, created_at
                ) VALUES (
                    %(id)s, %(run_id)s, %(exchange)s, %(symbol)s, %(sector)s, %(market_cap)s,
                    %(market_cap_source)s, %(latest_close)s, %(issue_share)s, %(data_gaps)s,
                    %(raw_overview)s, NOW()
                )
                ON CONFLICT (run_id, symbol) DO NOTHING
                """,
                {
                    "id": uuid4(),
                    "run_id": run_id,
                    "exchange": row.get("exchange") or "HOSE",
                    "symbol": row["symbol"],
                    "sector": row.get("sector"),
                    "market_cap": row.get("market_cap"),
                    "market_cap_source": row.get("market_cap_source") or "unknown",
                    "latest_close": row.get("latest_close"),
                    "issue_share": row.get("issue_share"),
                    "data_gaps": Json(_json_safe(row.get("data_gaps") or [])),
                    "raw_overview": Json(_json_safe(row.get("raw_overview") or {})),
                },
            )
        conn.commit()


def persist_stock_score(payload: dict[str, Any], *, run_id: UUID, rank: int | None = None) -> None:
    ensure_long_term_research_tables()
    sector_context = payload.get("sector_context") or {}
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO long_term_stock_scores (
                    id, run_id, symbol, exchange, sector, market_cap, rank, final_score, rating,
                    score_components, risk_penalty, macro_context, sector_context, news_context,
                    data_gaps, disclaimer, created_at
                ) VALUES (
                    %(id)s, %(run_id)s, %(symbol)s, %(exchange)s, %(sector)s, %(market_cap)s,
                    %(rank)s, %(final_score)s, %(rating)s, %(score_components)s,
                    %(risk_penalty)s, %(macro_context)s, %(sector_context)s, %(news_context)s,
                    %(data_gaps)s, %(disclaimer)s, NOW()
                )
                ON CONFLICT (run_id, symbol) DO UPDATE SET
                    rank = EXCLUDED.rank,
                    final_score = EXCLUDED.final_score,
                    rating = EXCLUDED.rating,
                    score_components = EXCLUDED.score_components,
                    risk_penalty = EXCLUDED.risk_penalty,
                    macro_context = EXCLUDED.macro_context,
                    sector_context = EXCLUDED.sector_context,
                    news_context = EXCLUDED.news_context,
                    data_gaps = EXCLUDED.data_gaps
                """,
                {
                    "id": uuid4(),
                    "run_id": run_id,
                    "symbol": payload["symbol"],
                    "exchange": sector_context.get("exchange"),
                    "sector": sector_context.get("sector"),
                    "market_cap": sector_context.get("market_cap"),
                    "rank": rank,
                    "final_score": payload["final_score"],
                    "rating": payload["rating"],
                    "score_components": Json(_json_safe(payload.get("score_components") or {})),
                    "risk_penalty": payload["risk_penalty"],
                    "macro_context": Json(_json_safe(payload.get("macro_context") or {})),
                    "sector_context": Json(_json_safe(sector_context)),
                    "news_context": Json(_json_safe(payload.get("news_context") or {})),
                    "data_gaps": Json(_json_safe(payload.get("data_gaps") or [])),
                    "disclaimer": payload.get("disclaimer") or DISCLAIMER,
                },
            )
        conn.commit()


def persist_symbol_analysis(payload: dict[str, Any], *, run_id: UUID | None = None) -> None:
    ensure_long_term_research_tables()
    with connect(settings.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO long_term_symbol_analyses (
                    id, symbol, mode, run_id, final_score, rating, score_components, risk_penalty,
                    macro_context, sector_context, news_context, ai_thesis, catalysts, risks,
                    data_gaps, disclaimer, created_at
                ) VALUES (
                    %(id)s, %(symbol)s, %(mode)s, %(run_id)s, %(final_score)s, %(rating)s,
                    %(score_components)s, %(risk_penalty)s, %(macro_context)s, %(sector_context)s,
                    %(news_context)s, %(ai_thesis)s, %(catalysts)s, %(risks)s, %(data_gaps)s,
                    %(disclaimer)s, NOW()
                )
                """,
                {
                    "id": uuid4(),
                    "symbol": payload["symbol"],
                    "mode": str(payload.get("mode") or "MANUAL").upper(),
                    "run_id": run_id,
                    "final_score": payload["final_score"],
                    "rating": payload["rating"],
                    "score_components": Json(_json_safe(payload.get("score_components") or {})),
                    "risk_penalty": payload["risk_penalty"],
                    "macro_context": Json(_json_safe(payload.get("macro_context") or {})),
                    "sector_context": Json(_json_safe(payload.get("sector_context") or {})),
                    "news_context": Json(_json_safe(payload.get("news_context") or {})),
                    "ai_thesis": str(payload.get("ai_thesis") or ""),
                    "catalysts": Json(_json_safe(payload.get("catalysts") or [])),
                    "risks": Json(_json_safe(payload.get("risks") or [])),
                    "data_gaps": Json(_json_safe(payload.get("data_gaps") or [])),
                    "disclaimer": payload.get("disclaimer") or DISCLAIMER,
                },
            )
        conn.commit()


def run_long_term_scan(*, universe_size: int = 100, candidate_limit: int = 400) -> dict[str, Any]:
    run_id = uuid4()
    macro_context = _read_macro_context()
    params = {"universe_size": max(1, min(int(universe_size), 100)), "candidate_limit": int(candidate_limit)}
    _insert_run(run_id, params, macro_context)
    scores: list[dict[str, Any]] = []
    try:
        universe = build_hose_universe(universe_size=params["universe_size"], candidate_limit=candidate_limit)
        for row in universe:
            _persist_universe_row(run_id, row)
            inputs = fetch_symbol_research_inputs(
                row["symbol"],
                parse_financials_with_gpt=True,
                overview_override=row.get("raw_overview") or None,
                latest_close_override=row.get("latest_close"),
                fetch_latest_close=False,
            )
            result = score_long_term_stock(
                symbol=row["symbol"],
                overview=inputs.get("overview") or row.get("raw_overview") or {},
                ratio_rows=inputs.get("ratio_rows") or [],
                income_rows=inputs.get("income_rows") or [],
                balance_rows=inputs.get("balance_rows") or [],
                cash_flow_rows=inputs.get("cash_flow_rows") or [],
                latest_close=inputs.get("latest_close") if inputs.get("latest_close") is not None else row.get("latest_close"),
                macro_context=macro_context,
                news_items=_read_news_context(row["symbol"], limit=10),
                seed_data_gaps=sorted(set((row.get("data_gaps") or []) + (inputs.get("data_gaps") or []))),
            )
            scores.append(result)
        scores.sort(key=lambda item: float(item.get("final_score") or 0.0), reverse=True)
        for idx, score in enumerate(scores, start=1):
            score["rank"] = idx
            score["mode"] = "AUTO"
            score["run_id"] = str(run_id)
            persist_stock_score(score, run_id=run_id, rank=idx)
        _finish_run(run_id, status="COMPLETED", scored_count=len(scores))
        return {
            "success": True,
            "run_id": str(run_id),
            "run_status": "COMPLETED",
            "universe_size": len(universe),
            "scored_count": len(scores),
            "rankings": _json_safe(scores),
            "macro_context": macro_context,
        }
    except Exception as exc:
        _finish_run(run_id, status="FAILED", scored_count=len(scores), error=str(exc))
        raise


def list_long_term_rankings(
    *,
    run_id: str | None = None,
    limit: int = 100,
    rating: str | None = None,
    sector: str | None = None,
) -> dict[str, Any]:
    ensure_long_term_research_tables()
    safe_limit = max(1, min(int(limit), 100))
    filters: list[str] = []
    params: dict[str, Any] = {"limit": safe_limit}
    if run_id:
        filters.append("s.run_id = %(run_id)s::uuid")
        params["run_id"] = run_id
    else:
        filters.append("s.run_id = (SELECT id FROM long_term_research_runs WHERE mode = 'AUTO' ORDER BY started_at DESC LIMIT 1)")
    if rating:
        filters.append("s.rating = %(rating)s")
        params["rating"] = rating
    if sector:
        filters.append("LOWER(COALESCE(s.sector, '')) LIKE %(sector)s")
        params["sector"] = f"%{sector.strip().lower()}%"
    where = "WHERE " + " AND ".join(filters)
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT s.id::text AS id, s.run_id::text AS run_id, s.symbol, s.exchange,
                       s.sector, s.market_cap, s.rank, s.final_score, s.rating,
                       s.score_components, s.risk_penalty, s.macro_context, s.sector_context,
                       s.news_context, s.data_gaps, s.disclaimer, s.created_at
                FROM long_term_stock_scores s
                {where}
                ORDER BY s.rank ASC NULLS LAST, s.final_score DESC
                LIMIT %(limit)s
                """,
                params,
            )
            rows = [_json_safe(dict(row)) for row in cur.fetchall()]
    return {"items": rows, "count": len(rows), "limit": safe_limit, "run_id": run_id}


def get_long_term_scan(run_id: str) -> dict[str, Any]:
    ensure_long_term_research_tables()
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id::text AS id, mode, run_status, started_at, finished_at,
                       universe_exchange, universe_size, scored_count, error, params, macro_context
                FROM long_term_research_runs
                WHERE id = %(run_id)s::uuid
                """,
                {"run_id": run_id},
            )
            run = cur.fetchone()
    if not run:
        raise ValueError("scan run not found")
    rankings = list_long_term_rankings(run_id=run_id, limit=100)
    return {"run": _json_safe(dict(run)), "rankings": rankings["items"], "count": rankings["count"]}


def get_latest_symbol_score(symbol: str) -> dict[str, Any]:
    ensure_long_term_research_tables()
    sym = _normalize_symbol(symbol)
    with connect(settings.database_url, row_factory=dict_row) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id::text AS id, run_id::text AS run_id, symbol, exchange, sector,
                       market_cap, rank, final_score, rating, score_components, risk_penalty,
                       macro_context, sector_context, news_context, data_gaps, disclaimer, created_at
                FROM long_term_stock_scores
                WHERE symbol = %(symbol)s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                {"symbol": sym},
            )
            row = cur.fetchone()
    if not row:
        raise ValueError("long-term score not found")
    return _json_safe(dict(row))
