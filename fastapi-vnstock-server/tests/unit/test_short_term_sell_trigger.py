from app.services.short_term_automation_service import _resolve_sell_trigger


def test_resolve_sell_trigger_take_profit() -> None:
    assert _resolve_sell_trigger(last_price=10.5, take_profit=10.0, stoploss=9.5) == "take_profit_hit"


def test_resolve_sell_trigger_stoploss() -> None:
    assert _resolve_sell_trigger(last_price=9.4, take_profit=10.0, stoploss=9.5) == "stoploss_hit"


def test_resolve_sell_trigger_none_when_inside_band() -> None:
    assert _resolve_sell_trigger(last_price=9.8, take_profit=10.0, stoploss=9.5) is None
