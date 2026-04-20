from __future__ import annotations

from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query

from app.services.signal_engine_service import (
    generate_all_signals,
    list_signals,
    run_long_term,
    run_short_term,
    run_technical,
)

router = APIRouter(tags=["signal-engine"])


@router.post("/scanner/short-term/run")
def scanner_short_term_run(
    limit_symbols: int = Query(default=30, ge=5, le=100),
) -> dict:
    try:
        data = run_short_term(limit_symbols)
        return {"success": True, "count": len(data), "data": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Short-term scanner error: {str(exc)}") from exc


@router.post("/scanner/long-term/run")
def scanner_long_term_run(
    limit_symbols: int = Query(default=30, ge=5, le=100),
) -> dict:
    try:
        data = run_long_term(limit_symbols)
        return {"success": True, "count": len(data), "data": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Long-term scanner error: {str(exc)}") from exc


@router.post("/scanner/technical/run")
def scanner_technical_run(
    limit_symbols: int = Query(default=30, ge=5, le=100),
) -> dict:
    try:
        data = run_technical(limit_symbols)
        return {"success": True, "count": len(data), "data": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Technical scanner error: {str(exc)}") from exc


@router.post("/signals/generate")
def signals_generate() -> dict:
    try:
        return {"success": True, "data": generate_all_signals()}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Generate signals error: {str(exc)}") from exc


@router.get("/signals")
def signals_list(
    strategy_type: Optional[Literal["SHORT_TERM", "LONG_TERM", "TECHNICAL"]] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict:
    try:
        rows = list_signals(
            strategy_type=strategy_type,
            symbol=symbol.upper() if symbol else None,
            limit=limit,
        )
        return {"success": True, "count": len(rows), "data": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"List signals error: {str(exc)}") from exc
