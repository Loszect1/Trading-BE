from __future__ import annotations

import json
from datetime import datetime, timezone


def _overview() -> dict:
    return {
        "session_id": "demo-1",
        "cash_balance": 25_000_000.0,
        "stock_value": 11_000_000.0,
        "total_assets": 36_000_000.0,
        "realized_pnl": 0.0,
        "tp_slot_pct": 0.3,
        "holdings": [
            {
                "symbol": "FPT",
                "quantity": 1000,
                "average_buy_price": 10_000.0,
                "position_value": 11_000_000.0,
                "take_profit_price": 12_000.0,
                "stoploss_price": 9_500.0,
                "settled_quantity": 1000,
                "pending_settlement_quantity": 0,
                "is_t2_sell_allowed": True,
                "opened_at": datetime(2026, 5, 1, tzinfo=timezone.utc),
                "strategy_code": "SHORT_TERM",
            }
        ],
    }


def _codex_payload(*, stoploss: float = 10_200.0, take_profit: float = 12_800.0) -> str:
    return json.dumps(
        {
            "portfolio_view": "Tactical hold with trailed risk.",
            "risk_notes": ["Raise stop if price holds above support."],
            "adjustments": [
                {
                    "symbol": "FPT",
                    "decision": "ADJUST",
                    "take_profit_price": take_profit,
                    "stoploss_price": stoploss,
                    "reason": "Trail stop under recent support while preserving upside.",
                    "confidence": 72,
                }
            ],
        }
    )


def test_demo_portfolio_review_applies_valid_adjustment(monkeypatch):
    import app.services.demo_portfolio_review_service as svc

    applied: list[dict] = []
    persisted: list[dict] = []

    monkeypatch.setattr(svc, "get_active_scheduler_demo_session_id_from_db", lambda: "demo-1")
    monkeypatch.setattr(svc, "get_demo_session_overview", lambda session_id: _overview())
    monkeypatch.setattr(svc._gpt_service, "generate_text", lambda **kwargs: _codex_payload())
    monkeypatch.setattr(
        svc,
        "upsert_demo_symbol_exit_levels",
        lambda **kwargs: applied.append(kwargs),
    )
    monkeypatch.setattr(svc, "_persist_run_row", lambda **kwargs: persisted.append(kwargs))

    result = svc.run_demo_portfolio_review_once(trigger_source="manual")

    assert result["success"] is True
    assert result["run_status"] == "COMPLETED"
    assert result["applied_count"] == 1
    assert result["skipped_count"] == 0
    assert applied == [
        {
            "session_id": "demo-1",
            "symbol": "FPT",
            "take_profit_price": 12800.0,
            "stoploss_price": 10200.0,
        }
    ]
    assert persisted[0]["run_status"] == "COMPLETED"
    assert persisted[0]["applied_count"] == 1


def test_demo_portfolio_review_rejects_looser_stoploss(monkeypatch):
    import app.services.demo_portfolio_review_service as svc

    applied: list[dict] = []

    monkeypatch.setattr(svc, "get_active_scheduler_demo_session_id_from_db", lambda: "demo-1")
    monkeypatch.setattr(svc, "get_demo_session_overview", lambda session_id: _overview())
    monkeypatch.setattr(svc._gpt_service, "generate_text", lambda **kwargs: _codex_payload(stoploss=9_000.0))
    monkeypatch.setattr(
        svc,
        "upsert_demo_symbol_exit_levels",
        lambda **kwargs: applied.append(kwargs),
    )
    monkeypatch.setattr(svc, "_persist_run_row", lambda **kwargs: None)

    result = svc.run_demo_portfolio_review_once(trigger_source="manual")

    assert result["success"] is True
    assert result["run_status"] == "COMPLETED"
    assert result["applied_count"] == 0
    assert applied == []
    assert result["detail"]["skipped"][0]["reason"] == "stoploss_would_loosen"


def test_demo_portfolio_review_skips_without_active_session(monkeypatch):
    import app.services.demo_portfolio_review_service as svc

    monkeypatch.setattr(svc, "get_active_scheduler_demo_session_id_from_db", lambda: None)
    monkeypatch.setattr(svc, "_persist_run_row", lambda **kwargs: None)

    result = svc.run_demo_portfolio_review_once(trigger_source="manual")

    assert result["success"] is True
    assert result["run_status"] == "SKIPPED"
    assert result["error"] == "missing_active_demo_session"
