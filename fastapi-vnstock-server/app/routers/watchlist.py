from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services.news_mail_service import get_watchlist_alerts

router = APIRouter(prefix="/watchlist", tags=["watchlist"])


@router.get("/{watchlist_id}/alerts")
def watchlist_alerts(
    watchlist_id: str,
    symbols: str = Query("", description="Comma-separated watchlist symbols."),
    limit: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    try:
        symbol_list = [item.strip().upper() for item in symbols.split(",") if item.strip()]
        return get_watchlist_alerts(watchlist_id=watchlist_id, symbols=symbol_list, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to read watchlist alerts: {exc}") from exc
