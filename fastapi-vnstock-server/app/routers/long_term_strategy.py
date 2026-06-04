from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.long_term_strategy_service import (
    get_long_term_scan,
    list_long_term_rankings,
    run_long_term_scan,
)

router = APIRouter(prefix="/strategy/long-term", tags=["long-term-strategy"])


class LongTermScanRequest(BaseModel):
    universe_size: int = Field(default=100, ge=1, le=100)
    candidate_limit: int = Field(default=400, ge=1, le=600)


@router.post("/scans")
def post_long_term_scan(body: LongTermScanRequest = LongTermScanRequest()) -> dict[str, Any]:
    try:
        return run_long_term_scan(universe_size=body.universe_size, candidate_limit=body.candidate_limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Long-term scan failed: {exc}") from exc


@router.get("/rankings")
def get_long_term_rankings(
    run_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=100),
    rating: str | None = Query(default=None),
    sector: str | None = Query(default=None),
) -> dict[str, Any]:
    try:
        return {"success": True, "data": list_long_term_rankings(run_id=run_id, limit=limit, rating=rating, sector=sector)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read long-term rankings: {exc}") from exc


@router.get("/scans/{run_id}")
def get_long_term_scan_by_id(run_id: str) -> dict[str, Any]:
    try:
        return {"success": True, "data": get_long_term_scan(run_id)}
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read long-term scan: {exc}") from exc
