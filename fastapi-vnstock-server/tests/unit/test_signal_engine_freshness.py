from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

from app.services import signal_engine_service as service


def test_latest_completed_trading_date_honors_post_close_grace(monkeypatch) -> None:
    monkeypatch.setattr(service, "get_vn_market_holiday_dates", lambda: set())
    monkeypatch.setattr(service.settings, "short_term_daily_bar_completion_grace_minutes", 75)

    tz = ZoneInfo("Asia/Ho_Chi_Minh")

    assert service._latest_completed_vn_trading_date(datetime(2026, 5, 18, 14, 45, tzinfo=tz)) == date(
        2026, 5, 15
    )
    assert service._latest_completed_vn_trading_date(datetime(2026, 5, 18, 15, 59, tzinfo=tz)) == date(
        2026, 5, 15
    )
    assert service._latest_completed_vn_trading_date(datetime(2026, 5, 18, 16, 0, tzinfo=tz)) == date(2026, 5, 18)


def test_get_close_and_volume_refreshes_stale_full_db_cache(monkeypatch) -> None:
    cached_closes = [10.0 + i for i in range(60)]
    cached_volumes = [1000.0 + i for i in range(60)]
    fresh_rows = [{"time": f"2026-05-{i:02d}", "close": 20.0 + i, "volume": 2000.0 + i} for i in range(1, 61)]
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(service, "_get_close_and_volume_from_db", lambda **_kwargs: (cached_closes, cached_volumes))
    monkeypatch.setattr(service, "_latest_cached_trading_date", lambda *_args, **_kwargs: date(2026, 5, 15))
    monkeypatch.setattr(service, "_latest_completed_vn_trading_date", lambda *_args, **_kwargs: date(2026, 5, 18))

    def fake_call_quote(*args, **kwargs):  # noqa: ANN002, ANN003
        calls.append({"args": args, "kwargs": kwargs})
        return fresh_rows

    monkeypatch.setattr(service.vnstock_api_service, "call_quote", fake_call_quote)
    monkeypatch.setattr(service, "_persist_symbol_daily_volume_rows", lambda **_kwargs: None)

    closes, volumes, source = service._get_close_and_volume_with_source(
        "AAA",
        bars=60,
        exchange="HOSE",
        min_cached_bars=55,
    )

    assert calls
    assert source == "vnstock"
    assert closes[-1] == 80.0
    assert volumes[-1] == 2060.0
