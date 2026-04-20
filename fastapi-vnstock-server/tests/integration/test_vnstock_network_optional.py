"""
Optional live vnstock / market-data calls.

Disabled unless ``RUN_VNSTOCK_NETWORK=1``. Excluded from default CI via marker
``network_vnstock`` (see ``pytest.ini`` / workflow).
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.network_vnstock


@pytest.mark.skipif(
    os.getenv("RUN_VNSTOCK_NETWORK", "").strip() != "1",
    reason="Set RUN_VNSTOCK_NETWORK=1 to run live vnstock smoke tests (off by default / CI).",
)
def test_live_vnstock_quote_history_smoke():
    from app.services.vnstock_api_service import VNStockApiService

    svc = VNStockApiService()
    rows = svc.call_quote(
        "history",
        source="VCI",
        symbol="VCI",
        method_kwargs={"start": "2024-01-02", "end": "2024-01-10", "interval": "1D"},
    )
    assert isinstance(rows, list)
    assert len(rows) >= 1
