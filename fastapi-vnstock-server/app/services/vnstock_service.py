from __future__ import annotations

from typing import Any, Dict, List

from vnstock import Vnstock, register_user

from app.core.config import settings


class VNStockService:
    def __init__(self) -> None:
        self._api_key_registered = False

    def _ensure_api_key(self) -> None:
        if self._api_key_registered:
            return

        if settings.vnstock_api_key:
            register_user(api_key=settings.vnstock_api_key)
            self._api_key_registered = True

    def get_price_history(
        self,
        symbol: str,
        start: str,
        end: str,
        interval: str = "1D",
    ) -> List[Dict[str, Any]]:
        self._ensure_api_key()

        stock = Vnstock().stock(symbol=symbol, source="VCI")
        price_df = stock.quote.history(start=start, end=end, interval=interval)
        return price_df.to_dict(orient="records")
