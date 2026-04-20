from __future__ import annotations

from datetime import date
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class RiskEvaluateRequest(BaseModel):
    account_mode: Literal["REAL", "DEMO"] = "DEMO"
    symbol: str = Field(..., min_length=1, max_length=20)
    nav: float = Field(..., gt=0)
    risk_per_trade: float = Field(default=0.01, gt=0, le=0.05)
    entry_price: float = Field(..., gt=0)
    stoploss_price: float = Field(..., gt=0)
    daily_new_orders: int = Field(default=0, ge=0)
    max_daily_new_orders: int = Field(default=10, ge=1, le=100)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()


class SettlementCheckRequest(BaseModel):
    account_mode: Literal["REAL", "DEMO"] = "DEMO"
    symbol: str = Field(..., min_length=1, max_length=20)
    quantity: int = Field(..., gt=0)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()


class ExecutionPlaceRequest(BaseModel):
    account_mode: Literal["REAL", "DEMO"] = "DEMO"
    symbol: str = Field(..., min_length=1, max_length=20)
    side: Literal["BUY", "SELL"]
    quantity: int = Field(..., gt=0)
    price: float = Field(..., gt=0)
    idempotency_key: Optional[str] = Field(default=None, min_length=6, max_length=128)
    risk_per_trade: float = Field(default=0.01, gt=0, le=0.05)
    nav: float = Field(default=100_000_000, gt=0)
    stoploss_price: Optional[float] = Field(default=None, gt=0)
    auto_process: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()


class ExecutionCancelRequest(BaseModel):
    order_id: str = Field(..., min_length=8)
    reason: str = Field(default="manual_cancel", min_length=3, max_length=200)


class PositionRow(BaseModel):
    symbol: str
    total_qty: int
    available_qty: int
    pending_settlement_qty: int
    avg_price: float


class PortfolioSummary(BaseModel):
    account_mode: Literal["REAL", "DEMO"]
    total_symbols: int
    total_qty: int
    total_available_qty: int
    total_pending_settlement_qty: int


class SettlementRow(BaseModel):
    symbol: str
    buy_trade_date: date
    settle_date: date
    qty: int
    available_qty: int
    pending_settlement_qty: int
    avg_price: float
