from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class ShortTermCycleRunRequest(BaseModel):
    """Optional overrides for a manual or dry-run cycle."""

    # 0 means "no cap" (scan all available symbols from HOSE/HNX/UPCOM).
    limit_symbols: int = Field(default=0, ge=0)
    exchange_scope: Literal["ALL", "HOSE", "HNX", "UPCOM"] = "ALL"
    account_mode: Literal["REAL", "DEMO"] = "DEMO"
    nav: float = Field(default=100_000_000, gt=0)
    risk_per_trade: float = Field(default=0.01, gt=0, le=0.05)
    max_daily_new_orders: int = Field(default=10, ge=1, le=100)
    enforce_vn_scan_schedule: bool = Field(
        default=False,
        description="When true, the cycle no-ops outside VN slot grid (for APScheduler alignment).",
    )
    async_for_heavy: bool = Field(
        default=True,
        description="When true, heavy ALL/unlimited manual runs are queued in background and return immediately.",
    )
    demo_session_id: str | None = Field(
        default=None,
        description="Optional FE demo session id for log scoping when account_mode=DEMO.",
    )
    real_account_available_cash_vnd: float | None = Field(
        default=None,
        gt=0,
        description="Optional REAL account available cash from FE (DNSE) for runtime NAV sizing.",
    )


class ShortTermCycleRunResponse(BaseModel):
    success: bool
    run_id: Optional[str] = None
    run_status: str
    scanned: int = 0
    buy_candidates: int = 0
    risk_rejected: int = 0
    executed: int = 0
    execution_rejected: int = 0
    errors: int = 0
    detail: dict[str, Any] = Field(default_factory=dict)


class ShortTermAutomationRunRow(BaseModel):
    id: str
    started_at: datetime
    finished_at: datetime
    run_status: str
    scanned: int
    buy_candidates: int
    risk_rejected: int
    executed: int
    execution_rejected: int
    errors: int
    detail: dict[str, Any]


class TechnicalCycleRunRequest(BaseModel):
    limit_symbols: int = Field(default=0, ge=0)
    exchange_scope: Literal["ALL", "HOSE", "HNX", "UPCOM"] = "ALL"


class TechnicalCycleRunResponse(BaseModel):
    success: bool
    run_id: Optional[str] = None
    run_status: str
    scanned: int = 0
    written: int = 0
    errors: int = 0
    detail: dict[str, Any] = Field(default_factory=dict)


class TechnicalAutomationRunRow(BaseModel):
    id: str
    started_at: datetime
    finished_at: datetime
    run_status: str
    scanned: int
    written: int
    errors: int
    detail: dict[str, Any]


class SchedulerToggleRequest(BaseModel):
    account_mode: Literal["REAL", "DEMO"] = "DEMO"
    enabled: bool


class RealScanOnlyToggleRequest(BaseModel):
    enabled: bool


class SchedulerDemoSessionRequest(BaseModel):
    demo_session_id: str | None = Field(
        default=None,
        description="Active DEMO session id used by scheduler for DEMO account mode.",
    )


class SchedulerStatusResponse(BaseModel):
    account_mode: Literal["REAL", "DEMO"]
    enabled: bool
    running: bool
    poll_seconds: int
    interval_minutes: int
    timezone: str
    on_grid: bool
    now_local: datetime
    next_grid_run_at: datetime | None = None
    active_demo_session_id: str | None = None


class SchedulerStateRow(BaseModel):
    account_mode: Literal["REAL", "DEMO"]
    enabled: bool
    updated_at: datetime


class MailSignalEntryRunOnceRequest(BaseModel):
    account_mode: Literal["REAL", "DEMO"] = "REAL"
    demo_session_id: str | None = Field(
        default=None,
        description="Optional DEMO session id when account_mode=DEMO.",
    )
    real_account_available_cash_vnd: float | None = Field(
        default=None,
        gt=0,
        description="Optional REAL account available cash from FE (DNSE) for runtime NAV sizing.",
    )


class RealRecommendationScanRequest(BaseModel):
    exchange_scope: Literal["ALL", "HOSE", "HNX", "UPCOM"] = "ALL"
    limit_symbols: int = Field(default=0, ge=0)


class RealRecommendationBuyRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=20)
    entry: float = Field(gt=0)
    take_profit: float = Field(gt=0)
    stop_loss: float = Field(gt=0)
    confidence: float = Field(default=50.0, ge=0, le=100)
    reason: str = Field(default="", max_length=2000)
    available_cash_vnd: float = Field(gt=0)
