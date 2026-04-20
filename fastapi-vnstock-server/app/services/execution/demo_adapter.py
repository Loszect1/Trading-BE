from __future__ import annotations

from app.services.execution.types import ExecutionOutcome, OrderExecutionContext


class DemoExecutionAdapter:
    """Local simulated broker: always reaches FILLED with a synthetic broker id."""

    name = "demo"

    def execute(self, ctx: OrderExecutionContext) -> ExecutionOutcome:
        broker_id = ctx.broker_order_id or f"demo-{ctx.internal_order_id}"
        return ExecutionOutcome(
            internal_status="FILLED",
            reason=None,
            broker_order_id=broker_id,
            broker_raw={"adapter": self.name, "synthetic": True},
            log_messages=["Demo execution adapter: simulated full fill"],
        )
