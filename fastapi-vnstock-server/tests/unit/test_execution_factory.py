"""Unit tests for execution adapter factory (no broker network)."""

from __future__ import annotations

import pytest

from app.core import config
from app.services.execution.demo_adapter import DemoExecutionAdapter
from app.services.execution.dnse_adapter import DnseLiveExecutionAdapter
from app.services.execution.factory import get_execution_adapter


def test_get_execution_adapter_demo_mode_always_demo() -> None:
    a = get_execution_adapter("demo")
    assert isinstance(a, DemoExecutionAdapter)
    assert a.name == "demo"


def test_get_execution_adapter_real_defaults_to_demo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config.settings, "real_execution_adapter", "demo")
    a = get_execution_adapter("REAL")
    assert isinstance(a, DemoExecutionAdapter)


def test_get_execution_adapter_real_selects_dnse_live_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config.settings, "real_execution_adapter", "dnse_live")
    a = get_execution_adapter("REAL")
    assert isinstance(a, DnseLiveExecutionAdapter)
