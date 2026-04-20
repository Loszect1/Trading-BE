from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.core.config import settings
from app.services.execution.demo_adapter import DemoExecutionAdapter
from app.services.execution.dnse_adapter import DnseLiveExecutionAdapter
from app.services.execution.types import ExecutionOutcome, OrderExecutionContext


@runtime_checkable
class ExecutionAdapter(Protocol):
    name: str

    def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome: ...
    # Optional: ``DnseLiveExecutionAdapter.cancel_at_broker`` — discovered via getattr in trading_core_service.


def get_execution_adapter(account_mode: str) -> ExecutionAdapter:
    mode = str(account_mode).upper()
    if mode == "DEMO":
        return DemoExecutionAdapter()
    adapter = (settings.real_execution_adapter or "demo").strip().lower()
    if adapter in {"dnse", "dnse_live", "live_dnse"}:
        return DnseLiveExecutionAdapter(settings=settings)
    return DemoExecutionAdapter()
