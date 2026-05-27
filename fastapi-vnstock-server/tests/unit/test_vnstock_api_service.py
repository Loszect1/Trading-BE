from __future__ import annotations

import pytest

from app.services import vnstock_api_service as svc_mod
from app.services.vnstock_api_service import VNStockApiService


def _service_without_network(monkeypatch: pytest.MonkeyPatch) -> VNStockApiService:
    svc = VNStockApiService()
    monkeypatch.setattr(svc, "_ensure_api_key", lambda: None)
    monkeypatch.setattr(svc, "_throttle_before_call", lambda: None)
    return svc


def test_call_company_converts_provider_system_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeCompany:
        def __init__(self, **_kwargs):
            pass

        def overview(self):
            raise SystemExit("Rate limit exceeded. Process terminated.")

    monkeypatch.setattr(svc_mod, "Company", FakeCompany)
    svc = _service_without_network(monkeypatch)

    with pytest.raises(RuntimeError, match="vnstock provider aborted: Rate limit exceeded"):
        svc.call_company("overview", symbol="FPT")


def test_call_financial_converts_provider_system_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeFinance:
        def __init__(self, **_kwargs):
            pass

        def income_statement(self):
            raise SystemExit("Rate limit exceeded. Process terminated.")

    monkeypatch.setattr(svc_mod, "Finance", FakeFinance)
    monkeypatch.setattr(svc_mod.RedisCacheService, "get_json", lambda *_args, **_kwargs: None)
    svc = _service_without_network(monkeypatch)

    with pytest.raises(RuntimeError) as exc_info:
        svc.call_financial("income_statement", symbol="FPT")

    message = str(exc_info.value)
    assert "All financial sources failed" in message
    assert "vnstock provider aborted: Rate limit exceeded" in message
