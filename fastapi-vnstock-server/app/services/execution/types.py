from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class OrderExecutionContext:
    internal_order_id: str
    account_mode: str
    symbol: str
    side: str
    quantity: int
    price: float
    idempotency_key: str | None
    broker_order_id: str | None
    order_metadata: dict[str, Any]
    current_status: str


@dataclass
class ExecutionOutcome:
    """Result of a single execution attempt (place + optional poll)."""

    internal_status: str
    reason: str | None
    broker_order_id: str | None
    broker_raw: dict[str, Any] = field(default_factory=dict)
    log_messages: list[str] = field(default_factory=list)
