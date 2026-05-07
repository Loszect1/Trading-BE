from __future__ import annotations

import app.services.short_term_automation_service as auto_svc


def test_refine_buy_levels_with_claude_parses_valid_json(monkeypatch) -> None:
    monkeypatch.setattr(auto_svc.settings, "use_claude", True)
    monkeypatch.setattr(
        auto_svc._automation_claude_service,
        "generate_text_with_resilience",
        lambda **kwargs: (
            '{"entry_price": 10.5, "take_profit_price": 11.2, "stoploss_price": 9.9, "rationale": "ok"}'
        ),
    )
    out = auto_svc._refine_buy_levels_with_claude(
        {"symbol": "AAA", "entry_price": 10.0, "take_profit_price": 11.0, "stoploss_price": 9.5}
    )
    assert isinstance(out, dict)
    assert out["entry_price"] == 10.5
    assert out["take_profit_price"] == 11.2
    assert out["stoploss_price"] == 9.9


def test_refine_buy_levels_with_claude_rejects_invalid_constraints(monkeypatch) -> None:
    monkeypatch.setattr(auto_svc.settings, "use_claude", True)
    monkeypatch.setattr(
        auto_svc._automation_claude_service,
        "generate_text_with_resilience",
        lambda **kwargs: '{"entry_price": 10.0, "take_profit_price": 9.0, "stoploss_price": 10.1}',
    )
    out = auto_svc._refine_buy_levels_with_claude(
        {"symbol": "AAA", "entry_price": 10.0, "take_profit_price": 11.0, "stoploss_price": 9.5}
    )
    assert out is None
