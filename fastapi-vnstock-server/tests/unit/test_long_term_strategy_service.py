from __future__ import annotations

import json
from uuid import UUID

from app.services import long_term_strategy_service as service


class _FakeVNStock:
    def call_listing(self, method_name: str, **_kwargs):  # noqa: ANN003
        assert method_name == "symbols_by_exchange"
        return [
            {"symbol": "AAA", "exchange": "HOSE"},
            {"symbol": "BBB", "exchange": "HOSE"},
            {"symbol": "CCC", "exchange": "HOSE"},
        ]

    def call_company(self, method_name: str, *, symbol: str, **_kwargs):  # noqa: ANN003
        assert method_name == "overview"
        return {
            "AAA": {"symbol": "AAA", "exchange": "HOSE", "market_cap": 30_000, "industry": "Banking"},
            "BBB": {"symbol": "BBB", "exchange": "HOSE", "issue_share": 2_000, "industry": "Steel"},
            "CCC": {"symbol": "CCC", "exchange": "HOSE", "market_cap": 10_000, "industry": "Retail"},
        }[symbol]

    def call_quote(self, method_name: str, *, symbol: str, **_kwargs):  # noqa: ANN003
        assert method_name == "history"
        return [{"close": {"BBB": 12.0}.get(symbol, 1.0)}]


def test_build_hose_universe_uses_market_cap_fallback(monkeypatch) -> None:
    monkeypatch.setattr(service, "vnstock_api_service", _FakeVNStock())

    rows = service.build_hose_universe(universe_size=2, candidate_limit=3)

    assert [row["symbol"] for row in rows] == ["AAA", "BBB"]
    assert rows[1]["market_cap"] == 24_000
    assert rows[1]["market_cap_source"] == "latest_close_x_issue_share"
    assert "market_cap_missing_used_fallback" in rows[1]["data_gaps"]


def test_normalizes_vci_financial_statement_rows() -> None:
    rows = service._normalize_financial_statement_rows(
        [
            {"item_id": "net_sales", "2026-Q1": 125, "2025-Q4": 100, "item": "Net sales"},
            {"item_id": "net_profit_loss_after_tax", "2026-Q1": 22, "2025-Q4": 16, "item": "Profit"},
        ]
    )

    assert rows == [
        {"period": "2026-Q1", "net_sales": 125, "net_profit_loss_after_tax": 22},
        {"period": "2025-Q4", "net_sales": 100, "net_profit_loss_after_tax": 16},
    ]


def test_read_macro_context_attaches_global_strategy_memory(monkeypatch) -> None:
    memory = [{"memory": {"title": "Balanced strategy"}, "scope": ["long_term_research"]}]
    monkeypatch.setattr(
        service,
        "get_macro_regime",
        lambda persist_snapshot=False: {
            "regime": "Recovery",
            "regime_score": 55,
            "components": {"growth": 60},
            "warnings": [],
            "data_gaps": [],
            "as_of": "2026-06-05",
        },
    )
    monkeypatch.setattr(service, "_read_global_strategy_memory", lambda: (memory, {"MACRO_STRATEGY_MEMORY": memory}))

    out = service._read_macro_context()

    assert out["regime"] == "Recovery"
    assert out["strategy_memory"] == memory
    assert out["strategy_memory_by_category"] == {"MACRO_STRATEGY_MEMORY": memory}


def test_gpt_financial_parser_returns_scorer_rows(monkeypatch) -> None:
    class FakeGpt:
        def generate_text_with_resilience(self, **kwargs):  # noqa: ANN003
            assert kwargs["cache_namespace"] == "long_term_financial_parse"
            return json.dumps(
                {
                    "latest_ratio": {"pe": 12.0, "pb": 1.4, "roe": 18.0, "roa": 2.0, "debt_to_equity": 0.8},
                    "income_rows": [
                        {"period": "2026-Q1", "revenue": 125.0, "profit_after_tax": 22.0},
                        {"period": "2025-Q4", "revenue": 100.0, "profit_after_tax": 16.0},
                    ],
                    "cash_flow_rows": [
                        {"period": "2026-Q1", "net_cash_flow_from_operating_activities": 10.0}
                    ],
                    "data_gaps": [],
                }
            )

    monkeypatch.setattr(service.settings, "use_gpt", True)
    monkeypatch.setattr(service, "gpt_service", FakeGpt())

    out = service._parse_financials_with_gpt(
        symbol="AAA",
        raw_financial_rows={
            "ratio_rows": [{"item_id": "pe_ratio", "item_en": "P/E", "2026-Q1": 12.0}],
            "income_rows": [{"item_id": "net_sales", "item_en": "Net sales", "2026-Q1": 125.0}],
            "cash_flow_rows": [
                {
                    "item_id": "net_cash_inflows_outflows_from_operating_activities",
                    "item_en": "Operating cash flow",
                    "2026-Q1": 10.0,
                }
            ],
        },
    )

    assert out == {
        "ratio_rows": [{"pe": 12.0, "pb": 1.4, "roe": 18.0, "roa": 2.0, "debt_to_equity": 0.8}],
        "income_rows": [
            {"period": "2026-Q1", "revenue": 125.0, "profit_after_tax": 22.0},
            {"period": "2025-Q4", "revenue": 100.0, "profit_after_tax": 16.0},
        ],
        "cash_flow_rows": [{"period": "2026-Q1", "net_cash_flow_from_operating_activities": 10.0}],
        "data_gaps": [],
    }


def test_ai_thesis_prompt_requests_vietnamese(monkeypatch) -> None:
    captured: dict[str, str] = {}

    class FakeGpt:
        def generate_text_with_resilience(self, **kwargs):  # noqa: ANN003
            captured["prompt"] = kwargs["prompt"]
            captured["system_prompt"] = kwargs["system_prompt"]
            return json.dumps(
                {
                    "ai_thesis": "Luận điểm tiếng Việt.",
                    "catalysts": ["Động lực tiếng Việt."],
                    "risks": ["Rủi ro tiếng Việt."],
                }
            )

    monkeypatch.setattr(service.settings, "use_gpt", True)
    monkeypatch.setattr(service, "gpt_service", FakeGpt())

    out = service._build_ai_thesis(
        {
            "symbol": "AAA",
            "final_score": 70,
            "rating": "Watchlist candidate",
            "catalysts": ["English catalyst."],
            "risks": ["English risk."],
        }
    )

    assert "Vietnamese" in captured["prompt"]
    assert "Vietnamese" in captured["system_prompt"]
    assert out["ai_thesis"] == "Luận điểm tiếng Việt."


def test_run_long_term_scan_uses_full_gpt_parsed_financial_inputs(monkeypatch) -> None:
    calls: dict[str, object] = {}

    monkeypatch.setattr(service, "_read_macro_context", lambda: {"regime": "Recovery"})
    monkeypatch.setattr(service, "_insert_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "_persist_universe_row", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "persist_stock_score", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "_finish_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(service, "_read_news_context", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        service,
        "build_hose_universe",
        lambda **_kwargs: [
            {
                "symbol": "AAA",
                "exchange": "HOSE",
                "latest_close": None,
                "data_gaps": [],
                "raw_overview": {"exchange": "HOSE", "sector": "Banking", "market_cap": 1_000_000_000_000},
            }
        ],
    )

    def fake_fetch(symbol: str, **kwargs):  # noqa: ANN003
        calls["fetch_kwargs"] = kwargs
        return {
            "overview": kwargs["overview_override"],
            "latest_close": None,
            "ratio_rows": [{"pe": 12, "pb": 1.2, "roe": 16, "debt_to_equity": 0.5}],
            "income_rows": [{"revenue": 120, "profit_after_tax": 20}, {"revenue": 100, "profit_after_tax": 12}],
            "balance_rows": [],
            "cash_flow_rows": [{"net_cash_flow_from_operating_activities": 5}],
            "data_gaps": [],
        }

    def fake_score(**kwargs):  # noqa: ANN003
        calls["score_kwargs"] = kwargs
        return {
            "symbol": kwargs["symbol"],
            "final_score": 70.0,
            "rating": "Watchlist candidate",
            "score_components": {},
            "risk_penalty": 0.0,
            "macro_context": kwargs["macro_context"],
            "sector_context": {"exchange": "HOSE", "sector": "Banking", "market_cap": 1_000_000_000_000},
            "news_context": {},
            "data_gaps": [],
            "disclaimer": service.DISCLAIMER,
        }

    monkeypatch.setattr(service, "fetch_symbol_research_inputs", fake_fetch)
    monkeypatch.setattr(service, "score_long_term_stock", fake_score)

    out = service.run_long_term_scan(universe_size=1, candidate_limit=1)

    fetch_kwargs = calls["fetch_kwargs"]
    assert fetch_kwargs["parse_financials_with_gpt"] is True
    assert fetch_kwargs["fetch_latest_close"] is False
    score_kwargs = calls["score_kwargs"]
    assert score_kwargs["ratio_rows"] == [{"pe": 12, "pb": 1.2, "roe": 16, "debt_to_equity": 0.5}]
    assert out["scored_count"] == 1
    assert UUID(out["run_id"])


def test_manual_analysis_returns_when_gpt_disabled(monkeypatch) -> None:
    monkeypatch.setattr(service.settings, "use_gpt", False)
    monkeypatch.setattr(
        service,
        "fetch_symbol_research_inputs",
        lambda _symbol: {
            "overview": {"exchange": "HOSE", "industry": "Banking", "market_cap": 5_000_000_000_000},
            "latest_close": 10.0,
            "ratio_rows": [{"pe": 12, "pb": 1.2, "roe": 15, "debt_to_equity": 0.5}],
            "income_rows": [{"revenue": 120, "profit_after_tax": 15}, {"revenue": 100, "profit_after_tax": 10}],
            "balance_rows": [],
            "cash_flow_rows": [{"net_cash_flow_from_operating_activities": 5}],
            "data_gaps": [],
        },
    )
    monkeypatch.setattr(service, "_read_macro_context", lambda: {"regime": "Recovery", "regime_score": 55})
    monkeypatch.setattr(service, "_read_news_context", lambda _symbol: [])
    monkeypatch.setattr(service, "persist_symbol_analysis", lambda *_args, **_kwargs: None)

    out = service.analyze_symbol_for_research("aaa", include_ai=True, persist=True)

    assert out["symbol"] == "AAA"
    assert out["mode"] == "MANUAL"
    assert out["ai_thesis"]
    assert out["disclaimer"]
