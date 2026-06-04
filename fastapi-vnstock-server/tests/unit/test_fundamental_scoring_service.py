from __future__ import annotations

from app.services.fundamental_scoring_service import score_long_term_stock


def _base_kwargs() -> dict:
    return {
        "symbol": "AAA",
        "overview": {
            "exchange": "HOSE",
            "industry": "Banking",
            "market_cap": 25_000_000_000_000,
            "company_profile": "A large listed company with diversified operations. " * 8,
        },
        "ratio_rows": [{"pe": 14, "pb": 1.4, "roe": 18, "roa": 2.0, "debt_to_equity": 0.8}],
        "income_rows": [
            {"revenue": 125, "profit_after_tax": 22},
            {"revenue": 100, "profit_after_tax": 16},
        ],
        "cash_flow_rows": [{"net_cash_flow_from_operating_activities": 10}],
        "macro_context": {"regime": "Expansion", "regime_score": 72},
        "news_items": [],
    }


def test_rating_thresholds_and_component_math() -> None:
    out = score_long_term_stock(**_base_kwargs())

    assert 0 <= out["final_score"] <= 100
    assert out["rating"] in {
        "Long-term compounder candidate",
        "Watchlist candidate",
        "Neutral / wait for better valuation",
        "High risk / avoid unless special situation",
        "Avoid",
    }
    assert out["score_components"]["positive_component_max"] == 90


def test_risk_penalty_is_capped() -> None:
    kwargs = _base_kwargs()
    kwargs["macro_context"] = {"regime": "Stress", "regime_score": 20}
    kwargs["ratio_rows"] = [{"pe": 60, "pb": 8, "roe": 2, "debt_to_equity": 6}]
    kwargs["news_items"] = [
        {"sentiment_label": "negative", "impact_score": 100, "confidence": 100}
        for _ in range(8)
    ]
    kwargs["overview"] = {"exchange": "UPCOM", "market_cap": 100}
    kwargs["seed_data_gaps"] = ["a", "b", "c", "d", "e"]

    out = score_long_term_stock(**kwargs)

    assert out["risk_penalty"] == 20
    assert out["final_score"] >= 0


def test_news_only_changes_catalyst_and_risk_context() -> None:
    base = score_long_term_stock(**_base_kwargs())
    kwargs = _base_kwargs()
    kwargs["news_items"] = [{"sentiment_label": "positive", "impact_score": 90, "confidence": 90}]
    with_news = score_long_term_stock(**kwargs)

    for key in ("business_quality", "growth", "valuation", "financial_strength"):
        assert with_news["score_components"][key] == base["score_components"][key]
    assert with_news["score_components"]["catalyst"] >= base["score_components"]["catalyst"]
    assert "buy/sell advice" in with_news["news_context"]["scoring_rule"].lower()
    assert with_news["rating"] in {
        "Long-term compounder candidate",
        "Watchlist candidate",
        "Neutral / wait for better valuation",
        "High risk / avoid unless special situation",
        "Avoid",
    }


def test_vci_alias_fields_are_scored_without_false_gaps() -> None:
    kwargs = _base_kwargs()
    kwargs["overview"] = {
        "exchange": "HOSE",
        "sector": "Securities",
        "market_cap": 25_000_000_000_000,
        "company_profile": "A large listed company with diversified operations. " * 8,
    }
    kwargs["ratio_rows"] = [
        {"pe_ratio": 18, "pb_ratio": 2.0, "roe": 0.19, "roa": 0.09, "debt_to_equity": 0.7}
    ]
    kwargs["income_rows"] = [
        {"net_sales": 125, "net_profit_loss_after_tax": 22},
        {"net_sales": 100, "net_profit_loss_after_tax": 16},
    ]
    kwargs["cash_flow_rows"] = [{"net_cash_inflows_outflows_from_operating_activities": 10}]

    out = score_long_term_stock(**kwargs)

    assert "industry_missing" not in out["data_gaps"]
    assert "growth_statement_history_missing" not in out["data_gaps"]
    assert "valuation_ratio_missing" not in out["data_gaps"]
    assert "roe_missing" not in out["data_gaps"]
    assert "leverage_ratio_missing" not in out["data_gaps"]
