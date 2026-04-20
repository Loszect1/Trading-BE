from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class MonitoringEvaluateRequest(BaseModel):
    account_modes: Optional[list[Literal["REAL", "DEMO"]]] = Field(
        default=None,
        description="If omitted, evaluates REAL and DEMO.",
    )


class KillSwitchRequest(BaseModel):
    account_mode: Literal["REAL", "DEMO"]
    active: bool
    reason: Optional[str] = Field(default=None, max_length=2000)
