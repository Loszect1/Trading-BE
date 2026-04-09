from fastapi import APIRouter, HTTPException, Query

from app.services.vnstock_service import VNStockService

router = APIRouter(prefix="/market", tags=["market"])
vnstock_service = VNStockService()


@router.get("/history")
def get_price_history(
    symbol: str = Query(..., min_length=3, max_length=10),
    start: str = Query(..., description="Format YYYY-MM-DD"),
    end: str = Query(..., description="Format YYYY-MM-DD"),
    interval: str = Query("1D"),
) -> dict:
    try:
        data = vnstock_service.get_price_history(
            symbol=symbol.upper(),
            start=start,
            end=end,
            interval=interval,
        )
        return {
            "symbol": symbol.upper(),
            "start": start,
            "end": end,
            "interval": interval,
            "rows": len(data),
            "data": data,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch market data: {str(exc)}",
        ) from exc
