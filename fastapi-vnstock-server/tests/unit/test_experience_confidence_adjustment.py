from app.services.signal_engine_service import _experience_confidence_adjustment


def test_experience_confidence_adjustment_non_buy_noop() -> None:
    adj, meta = _experience_confidence_adjustment(symbol="AAA", action="SELL")
    assert adj == 0.0
    assert meta.get("applied") is False
