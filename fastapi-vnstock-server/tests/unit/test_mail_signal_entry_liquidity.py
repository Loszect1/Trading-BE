from __future__ import annotations

from app.services import mail_signal_scheduler_service as service


def test_mail_signal_entry_skips_low_liquidity_before_price_or_order(monkeypatch) -> None:
    mail_payload = {
        "items": [
            {
                "symbol": "BNA",
                "entry": 6.2,
                "take_profit": 7.5,
                "stop_loss": 5.8,
                "confidence": 0.8,
                "reason": "fixture",
            }
        ]
    }

    def fake_get_json(key: str):  # noqa: ANN202
        if str(key).startswith("signals:mail:daily:"):
            return mail_payload
        return None

    monkeypatch.setattr(service._redis, "get_json", fake_get_json)
    monkeypatch.setattr(service._redis, "set_json", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        service,
        "get_cached_symbol_liquidity_status",
        lambda symbol: {
            "symbol": str(symbol).upper(),
            "eligible_liquidity": False,
            "reason": "low_or_irregular_liquidity",
        },
    )
    monkeypatch.setattr(
        service,
        "_extract_latest_price",
        lambda symbol: (_ for _ in ()).throw(AssertionError("price should not be fetched")),
    )
    monkeypatch.setattr(
        service,
        "place_order",
        lambda payload: (_ for _ in ()).throw(AssertionError("order should not be placed")),
    )

    result = service.run_prev_day_entry_auto_trading_once(
        account_mode_override="DEMO",
        demo_session_id_override="demo-fixture",
        nav_override=100_000_000.0,
    )

    assert result["executed"] == []
    assert result["skipped"][0]["symbol"] == "BNA"
    assert result["skipped"][0]["reason"] == "low_or_irregular_liquidity"
    assert result["skipped"][0]["liquidity"]["eligible_liquidity"] is False
