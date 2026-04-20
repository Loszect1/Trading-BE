"""Unit tests for monitoring bot status classification (no DB, no network)."""

from __future__ import annotations

import pytest

from app.core import config
from app.services.monitoring_service import _classify_bot_status


@pytest.fixture(autouse=True)
def _monitoring_thresholds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config.settings, "monitoring_error_count_threshold", 5)
    monkeypatch.setattr(config.settings, "monitoring_signal_stale_minutes", 60)
    monkeypatch.setattr(config.settings, "monitoring_drawdown_alert_pct", 20.0)


def test_classify_halted_when_kill_switch_active() -> None:
    m = {"rejected_orders_window": 0, "signal_missing": False, "signal_age_seconds": 0.0, "drawdown_proxy_pct": 0.0}
    assert _classify_bot_status(m, kill_active=True) == "HALTED"


def test_classify_error_when_rejections_at_threshold() -> None:
    m = {"rejected_orders_window": 5, "signal_missing": False, "signal_age_seconds": 0.0, "drawdown_proxy_pct": 0.0}
    assert _classify_bot_status(m, kill_active=False) == "ERROR"


def test_classify_degraded_when_signal_missing() -> None:
    m = {"rejected_orders_window": 0, "signal_missing": True, "signal_age_seconds": None, "drawdown_proxy_pct": 0.0}
    assert _classify_bot_status(m, kill_active=False) == "DEGRADED"


def test_classify_degraded_when_signal_stale() -> None:
    stale_sec = 60 * 60 + 1
    m = {"rejected_orders_window": 0, "signal_missing": False, "signal_age_seconds": stale_sec, "drawdown_proxy_pct": 0.0}
    assert _classify_bot_status(m, kill_active=False) == "DEGRADED"


def test_classify_degraded_when_drawdown_near_alert_threshold() -> None:
    m = {"rejected_orders_window": 0, "signal_missing": False, "signal_age_seconds": 10.0, "drawdown_proxy_pct": 15.0}
    assert _classify_bot_status(m, kill_active=False) == "DEGRADED"


def test_classify_ok_when_metrics_healthy() -> None:
    m = {"rejected_orders_window": 0, "signal_missing": False, "signal_age_seconds": 120.0, "drawdown_proxy_pct": 5.0}
    assert _classify_bot_status(m, kill_active=False) == "OK"
