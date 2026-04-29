from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator


SHORT_TERM_SCAN_CADENCE_MINUTES = 15
DEMO_INITIAL_BALANCE_VND = 100_000_000


class AutoTradingConfigData(BaseModel):
    """Static configuration for Auto Trading (Real/Demo) clients."""

    short_term_scan_cadence_minutes: int = Field(
        default=SHORT_TERM_SCAN_CADENCE_MINUTES,
        description="Short-term scanner cadence per TRADING_BOT_PLAN.",
    )
    demo_initial_balance_vnd: int = Field(
        default=DEMO_INITIAL_BALANCE_VND,
        description="Starting cash balance for demo accounts (VND).",
    )
    short_term_scan_cron_hint: str = Field(
        default="minute='0,15,30,45', hour='9-11,13-14', day_of_week='mon-fri', timezone='Asia/Ho_Chi_Minh'",
        description="Suggested APScheduler/cron expression for VN market hours.",
    )
    demo_session_header: str = Field(
        default="X-Demo-Session-Id",
        description="HTTP header used to isolate in-memory demo ledger state.",
    )


class DemoPositionSnapshot(BaseModel):
    symbol: str
    quantity: int = Field(..., ge=0)
    average_cost: float = Field(..., ge=0)
    opened_at: datetime


class DemoTradeHistoryItem(BaseModel):
    trade_id: str
    created_at: datetime
    side: Literal["BUY", "SELL"]
    symbol: str
    quantity: int = Field(..., ge=1)
    price: float = Field(..., gt=0)
    notional: float = Field(..., ge=0)
    realized_pnl_on_trade: float
    cash_after: float


class DemoTradeRequest(BaseModel):
    side: Literal["BUY", "SELL"]
    symbol: str = Field(..., min_length=1, max_length=20)
    quantity: int = Field(..., ge=1, le=1_000_000)
    price: float = Field(..., gt=0)
    strategy_type: Literal["SHORT_TERM", "LONG_TERM", "TECHNICAL"] = "SHORT_TERM"
    market_context: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        cleaned = value.strip().upper()
        if not cleaned.replace("-", "").isalnum():
            raise ValueError("symbol must be alphanumeric (hyphen allowed)")
        return cleaned


class ExperienceAnalysisStatus(BaseModel):
    recorded: bool
    skipped_reason: Optional[str] = None
    experience_row: Optional[dict[str, Any]] = None
    error_detail: Optional[str] = None


class DemoTradeResponseData(BaseModel):
    trade_id: str
    session_id: str
    side: Literal["BUY", "SELL"]
    symbol: str
    quantity: int
    price: float
    cash_after: float
    position: Optional[DemoPositionSnapshot] = None
    realized_pnl_on_trade: float
    cumulative_realized_pnl: float
    experience_analysis: ExperienceAnalysisStatus


class DemoAccountResponseData(BaseModel):
    session_id: str
    cash_balance: float
    positions: list[DemoPositionSnapshot]
    realized_pnl: float
    unrealized_pnl: float
    equity_approx_vnd: float
    marks_used: dict[str, float] = Field(
        default_factory=dict,
        description="Mark prices applied per symbol when computing unrealized PnL.",
    )
    trade_history: list[DemoTradeHistoryItem] = Field(
        default_factory=list,
        description="Most recent demo trades (newest first).",
    )
    trade_history_total: int = Field(default=0, ge=0)
    trade_history_limit: int = Field(default=50, ge=1)
    trade_history_offset: int = Field(default=0, ge=0)


class DemoHoldingOverviewItem(BaseModel):
    symbol: str
    quantity: int = Field(..., ge=0)
    average_buy_price: float = Field(..., ge=0)
    position_value: float = Field(..., ge=0)
    take_profit_price: float | None = Field(default=None, gt=0)
    stoploss_price: float | None = Field(default=None, gt=0)
    settled_quantity: int = Field(default=0, ge=0)
    pending_settlement_quantity: int = Field(default=0, ge=0)
    next_settle_date: datetime | None = None
    is_t2_sell_allowed: bool = False
    opened_at: datetime


class DemoStrategyCashOverviewItem(BaseModel):
    strategy_code: Literal["SHORT_TERM", "MAIL_SIGNAL", "UNALLOCATED"]
    allocation_pct: float = Field(..., ge=0.0, le=1.0)
    cash_value: float = Field(..., ge=0.0)
    used_cash_value: float = Field(default=0.0, ge=0.0)
    remaining_cash_value: float = Field(default=0.0, ge=0.0)


class DemoSessionOverviewData(BaseModel):
    session_id: str
    is_active: bool = Field(
        ...,
        description="True when the session has at least one trade or one open holding.",
    )
    initial_balance: float = Field(..., ge=0)
    cash_balance: float = Field(..., ge=0)
    stock_value: float = Field(..., ge=0)
    total_assets: float = Field(..., ge=0)
    realized_pnl: float
    trade_count: int = Field(..., ge=0)
    holdings_count: int = Field(..., ge=0)
    holdings: list[DemoHoldingOverviewItem] = Field(default_factory=list)
    tp_slot_pct: float = Field(default=0.3, ge=0.1, le=0.9)
    strategy_cash_overview: list[DemoStrategyCashOverviewItem] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class DemoStrategyCashTransferRequest(BaseModel):
    from_strategy: Literal["UNALLOCATED", "SHORT_TERM", "MAIL_SIGNAL"] = "UNALLOCATED"
    to_strategy: Literal["SHORT_TERM", "MAIL_SIGNAL", "UNALLOCATED"]
    amount_vnd: float = Field(..., gt=0.0)


class DemoStrategyCashTransferData(BaseModel):
    session_id: str
    transferred_to: Literal["SHORT_TERM", "MAIL_SIGNAL", "UNALLOCATED"]
    amount_vnd: float = Field(..., gt=0.0)


class DemoTpSlotPctUpdateRequest(BaseModel):
    tp_slot_pct: float = Field(..., ge=0.1, le=0.9)


class DemoExitSymbolLevelsUpsertRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    take_profit_price: float = Field(..., gt=0.0)
    stoploss_price: float = Field(..., gt=0.0)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        cleaned = value.strip().upper()
        if not cleaned.replace("-", "").isalnum():
            raise ValueError("symbol must be alphanumeric (hyphen allowed)")
        return cleaned
