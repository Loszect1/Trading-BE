from __future__ import annotations

from typing import Any

DISCLAIMER = "Research context only. This is not financial advice or an execution signal."


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


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


def _first_number(mapping: dict[str, Any] | None, *keys: str) -> float | None:
    if not isinstance(mapping, dict):
        return None
    lowered = {str(k).lower(): v for k, v in mapping.items()}
    for key in keys:
        value = lowered.get(key.lower())
        number = _to_float(value)
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


def _rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
    return []


def _latest_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    return rows[0] if rows else None


def _growth_from_rows(rows: list[dict[str, Any]], *keys: str) -> float | None:
    if len(rows) < 2:
        return None
    latest = _first_number(rows[0], *keys)
    previous = _first_number(rows[1], *keys)
    if latest is None or previous is None or previous == 0:
        return None
    return ((latest - previous) / abs(previous)) * 100.0


def _score_growth_pct(value: float | None) -> float | None:
    if value is None:
        return None
    return _clamp(5.0 + value * 0.75, 0.0, 20.0)


def _score_pe(pe: float | None) -> float | None:
    if pe is None or pe <= 0:
        return None
    if 8 <= pe <= 18:
        return 12.0
    if 18 < pe <= 26 or 5 <= pe < 8:
        return 8.0
    if 26 < pe <= 35:
        return 5.0
    return 2.0


def _score_pb(pb: float | None) -> float | None:
    if pb is None or pb <= 0:
        return None
    if 0.8 <= pb <= 2.2:
        return 8.0
    if 2.2 < pb <= 3.5 or 0.4 <= pb < 0.8:
        return 5.0
    return 2.0


def _ratio_percent(value: float | None) -> float | None:
    if value is None:
        return None
    if -1.0 <= value <= 1.0:
        return value * 100.0
    return value


def _rating(score: float) -> str:
    if score >= 80:
        return "Long-term compounder candidate"
    if score >= 65:
        return "Watchlist candidate"
    if score >= 50:
        return "Neutral / wait for better valuation"
    if score >= 35:
        return "High risk / avoid unless special situation"
    return "Avoid"


def score_long_term_stock(
    *,
    symbol: str,
    overview: dict[str, Any] | None,
    ratio_rows: list[dict[str, Any]] | None = None,
    income_rows: list[dict[str, Any]] | None = None,
    balance_rows: list[dict[str, Any]] | None = None,
    cash_flow_rows: list[dict[str, Any]] | None = None,
    latest_close: float | None = None,
    macro_context: dict[str, Any] | None = None,
    news_items: list[dict[str, Any]] | None = None,
    seed_data_gaps: list[str] | None = None,
) -> dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    ov = overview if isinstance(overview, dict) else {}
    ratios = ratio_rows or []
    incomes = income_rows or []
    balances = balance_rows or []
    cash_flows = cash_flow_rows or []
    macro = macro_context if isinstance(macro_context, dict) else {}
    news = news_items or []
    data_gaps = list(seed_data_gaps or [])
    catalysts: list[str] = []
    risks: list[str] = []

    industry = _first_text(ov, "industry", "sector", "icb_name3", "icb_name2", "icb_name4")
    exchange = _first_text(ov, "exchange", "com_group_code", "board")
    market_cap = _first_number(ov, "market_cap", "marketcap", "market_capitalization")
    issue_share = _first_number(ov, "issue_share", "financial_ratio_issue_share")
    if market_cap is None and latest_close and issue_share:
        market_cap = latest_close * issue_share
        data_gaps.append("market_cap_fallback_latest_close_x_issue_share")
    elif market_cap is None:
        data_gaps.append("market_cap_missing")

    profile = _first_text(ov, "company_profile", "profile", "history")
    business_quality = 6.0
    if industry:
        business_quality += 4.0
    else:
        data_gaps.append("industry_missing")
    if profile:
        business_quality += 4.0 if len(profile) >= 240 else 2.0
    else:
        data_gaps.append("company_profile_missing")
    if market_cap and market_cap >= 10_000_000_000_000:
        business_quality += 4.0
    elif market_cap and market_cap >= 1_000_000_000_000:
        business_quality += 2.0
    business_quality += 2.0 if exchange == "HOSE" else 0.5
    business_quality = _clamp(business_quality, 0.0, 20.0)

    revenue_growth = _growth_from_rows(incomes, "revenue", "net_sales", "operating_sales", "sales", "doanh_thu_thuan")
    profit_growth = _growth_from_rows(
        incomes,
        "profit_after_tax",
        "net_profit_loss_after_tax",
        "net_profit",
        "post_tax_profit",
        "lnst",
        "attributable_to_parent_company",
    )
    growth_parts = [_score_growth_pct(revenue_growth), _score_growth_pct(profit_growth)]
    growth_parts = [part for part in growth_parts if part is not None]
    if growth_parts:
        growth = _clamp(sum(growth_parts) / len(growth_parts), 0.0, 20.0)
    else:
        growth = 9.0
        data_gaps.append("growth_statement_history_missing")

    latest_ratio = _latest_row(ratios) or {}
    pe = _first_number(latest_ratio, "pe", "pe_ratio", "p/e", "price_to_earnings")
    pb = _first_number(latest_ratio, "pb", "pb_ratio", "p/b", "price_to_book")
    valuation_parts = [_score_pe(pe), _score_pb(pb)]
    valuation_parts = [part for part in valuation_parts if part is not None]
    if valuation_parts:
        valuation = _clamp(sum(valuation_parts), 0.0, 20.0)
    else:
        valuation = 10.0
        data_gaps.append("valuation_ratio_missing")

    roe = _ratio_percent(_first_number(latest_ratio, "roe", "return_on_equity"))
    roa = _ratio_percent(_first_number(latest_ratio, "roa", "return_on_assets"))
    debt_to_equity = _first_number(latest_ratio, "debt_to_equity", "debtPerEquity", "de", "debt/equity")
    cfo = _first_number(
        _latest_row(cash_flows),
        "net_cash_flow_from_operating_activities",
        "net_cash_inflows_outflows_from_operating_activities",
        "cashflow_from_operating",
        "cfo",
    )
    financial_strength = 5.0
    if roe is not None:
        financial_strength += _clamp(roe / 20.0 * 5.0, 0.0, 5.0)
    else:
        data_gaps.append("roe_missing")
    if roa is not None:
        financial_strength += _clamp(roa / 10.0 * 2.0, 0.0, 2.0)
    if debt_to_equity is not None:
        financial_strength += 3.0 if debt_to_equity <= 1.0 else (1.5 if debt_to_equity <= 2.0 else 0.0)
    else:
        data_gaps.append("leverage_ratio_missing")
    if cfo is not None:
        financial_strength += 2.0 if cfo > 0 else -1.0
    financial_strength = _clamp(financial_strength, 0.0, 15.0)

    catalyst = 6.0
    regime = str(macro.get("regime") or "").strip()
    if regime == "Expansion":
        catalyst += 3.0
        catalysts.append("Macro regime is Expansion.")
    elif regime == "Recovery":
        catalyst += 1.5
        catalysts.append("Macro regime is Recovery.")
    elif regime == "Stress":
        catalyst -= 2.0
        risks.append("Macro regime is Stress.")
    elif regime == "Overheated":
        catalyst -= 1.0
        risks.append("Macro regime is Overheated.")
    if industry:
        catalyst += 1.0
        catalysts.append(f"Sector context available: {industry}.")

    positive_news = 0.0
    negative_news = 0.0
    for item in news:
        label = str(item.get("sentiment_label") or item.get("sentiment") or "neutral").lower()
        impact = _to_float(item.get("impact_score")) or 0.0
        confidence = (_to_float(item.get("confidence")) or 60.0) / 100.0
        weighted = impact * max(0.2, min(1.0, confidence))
        if label == "positive":
            positive_news += weighted
        elif label == "negative":
            negative_news += weighted
    if positive_news:
        boost = min(4.0, positive_news / 35.0)
        catalyst += boost
        catalysts.append(f"Recent positive news support adds {boost:.1f} catalyst points.")
    catalyst = _clamp(catalyst, 0.0, 15.0)

    risk_penalty = 0.0
    if negative_news:
        penalty = min(7.0, negative_news / 25.0)
        risk_penalty += penalty
        risks.append(f"Recent negative news adds {penalty:.1f} risk penalty points.")
    if regime == "Stress":
        risk_penalty += 5.0
    elif regime == "Overheated":
        risk_penalty += 3.0
    if debt_to_equity is not None and debt_to_equity > 2.0:
        risk_penalty += 3.0
        risks.append("Leverage ratio is elevated.")
    if market_cap is not None and market_cap < 1_000_000_000_000:
        risk_penalty += 2.0
        risks.append("Market cap is small for long-term ranking quality.")
    if len(data_gaps) >= 5:
        risk_penalty += 3.0
        risks.append("Data coverage is weak.")
    risk_penalty = round(_clamp(risk_penalty, 0.0, 20.0), 2)

    components = {
        "business_quality": round(business_quality, 2),
        "growth": round(growth, 2),
        "valuation": round(valuation, 2),
        "financial_strength": round(financial_strength, 2),
        "catalyst": round(catalyst, 2),
        "positive_component_sum": round(business_quality + growth + valuation + financial_strength + catalyst, 2),
        "positive_component_max": 90,
        "model_version": "long_term_100_v1",
    }
    positive_sum = components["positive_component_sum"]
    final_score = round(_clamp((positive_sum / 90.0) * 100.0 - risk_penalty, 0.0, 100.0), 2)
    news_context = {
        "sample_count": len(news),
        "positive_weighted_impact": round(positive_news, 2),
        "negative_weighted_impact": round(negative_news, 2),
        "scoring_rule": "News affects catalyst and risk penalty only; it does not create buy/sell advice.",
    }
    sector_context = {
        "exchange": exchange or None,
        "sector": industry or None,
        "market_cap": market_cap,
        "issue_share": issue_share,
        "latest_close": latest_close,
    }
    if not catalysts:
        catalysts.append("No strong catalyst found from available deterministic inputs.")
    if not risks:
        risks.append("No major deterministic risk flag found in available inputs.")

    return {
        "symbol": sym,
        "final_score": final_score,
        "rating": _rating(final_score),
        "score_components": components,
        "risk_penalty": risk_penalty,
        "macro_context": macro,
        "sector_context": sector_context,
        "news_context": news_context,
        "catalysts": catalysts[:8],
        "risks": risks[:8],
        "data_gaps": sorted(set(data_gaps)),
        "disclaimer": DISCLAIMER,
    }
