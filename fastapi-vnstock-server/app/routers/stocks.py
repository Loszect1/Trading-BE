from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.services.long_term_strategy_service import (
    analyze_symbol_for_research,
    get_latest_symbol_score,
)

router = APIRouter(prefix="/stocks", tags=["stocks"])


@router.post("/{symbol}/long-term-analysis")
def post_symbol_long_term_analysis(symbol: str) -> dict[str, Any]:
    try:
        return analyze_symbol_for_research(symbol, mode="MANUAL", include_ai=True, persist=True)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Long-term analysis failed: {exc}") from exc


@router.get("/{symbol}/long-term-score")
def get_symbol_long_term_score(symbol: str) -> dict[str, Any]:
    try:
        return get_latest_symbol_score(symbol)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Long-term score lookup failed: {exc}") from exc
