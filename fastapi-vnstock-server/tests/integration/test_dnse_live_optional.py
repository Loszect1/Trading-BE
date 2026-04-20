"""
Optional live DNSE broker calls (orders API path, read-only probe).

Excluded from default CI via marker ``dnse_live``. Also requires
``RUN_DNSE_LIVE_TESTS=1`` and valid ``DNSE_ACCESS_TOKEN`` + ``DNSE_TRADING_TOKEN``
(see ``AppSettings`` / README).
"""

from __future__ import annotations

import os

import pytest

from app.core.config import AppSettings
from app.services.execution.dnse_adapter import DnseLiveExecutionAdapter
from app.services.execution.dnse_token import validate_dnse_tokens_for_live

pytestmark = pytest.mark.dnse_live

_LIVE_GATE = os.getenv("RUN_DNSE_LIVE_TESTS", "").strip() == "1"


@pytest.mark.skipif(
    not _LIVE_GATE,
    reason="Set RUN_DNSE_LIVE_TESTS=1 to run live DNSE integration tests (never default CI).",
)
def test_live_dnse_adapter_readonly_sub_accounts_contract():
    from tests.helpers.dnse_live_contract import assert_sub_account_table_shape

    cfg = AppSettings()
    ok, reason = validate_dnse_tokens_for_live(cfg.dnse_access_token or "", cfg.dnse_trading_token or "")
    if not ok:
        pytest.skip(f"DNSE tokens not configured for live probe: {reason}")

    adapter = DnseLiveExecutionAdapter(settings=cfg)
    rows = adapter.readonly_list_sub_accounts()
    assert_sub_account_table_shape(rows)
