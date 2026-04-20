from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.experience_service import (
    create_experience_from_trade,
    ensure_experience_table,
    list_experience,
)

router = APIRouter(prefix="/experience", tags=["experience"])


class ExperienceAnalyzeRequest(BaseModel):
    trade_id: str = Field(..., min_length=1)
    account_mode: Literal["REAL", "DEMO"]
    symbol: str = Field(..., min_length=1, max_length=20)
    strategy_type: Literal["SHORT_TERM", "LONG_TERM", "TECHNICAL"]
    entry_time: Optional[datetime] = None
    exit_time: Optional[datetime] = None
    pnl_value: float
    pnl_percent: float
    market_context: dict[str, Any] = Field(default_factory=dict)
    confidence_after_review: float = Field(default=70.0, ge=0, le=100)


@router.get("")
def get_experience(
    account_mode: Optional[Literal["REAL", "DEMO"]] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    strategy_type: Optional[Literal["SHORT_TERM", "LONG_TERM", "TECHNICAL"]] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    try:
        ensure_experience_table()
        data = list_experience(
            account_mode=account_mode,
            symbol=symbol,
            strategy_type=strategy_type,
            limit=limit,
        )
        return {"success": True, "count": len(data), "data": data}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Experience list error: {str(exc)}") from exc


@router.post("/analyze")
def analyze_experience(payload: ExperienceAnalyzeRequest) -> dict:
    try:
        ensure_experience_table()
        inserted = create_experience_from_trade(payload.model_dump())
        return {"success": True, "data": inserted}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Experience analyze error: {str(exc)}") from exc
