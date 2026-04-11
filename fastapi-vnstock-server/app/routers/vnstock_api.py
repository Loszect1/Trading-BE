from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.config import settings
from app.services.redis_cache import RedisCacheService
from app.services.vnstock_api_service import VNStockApiService

router = APIRouter(prefix="/vnstock-api", tags=["vnstock-api"])
vnstock_api_service = VNStockApiService()
redis_cache_service = RedisCacheService()


class QuoteRequest(BaseModel):
    source: str = "VCI"
    symbol: str = Field(..., min_length=1, max_length=10)
    random_agent: bool = False
    show_log: bool = False
    method_kwargs: Dict[str, Any] = Field(default_factory=dict)


class CompanyRequest(BaseModel):
    source: str = "VCI"
    symbol: str = Field(..., min_length=1, max_length=10)
    random_agent: bool = False
    show_log: bool = False
    method_kwargs: Dict[str, Any] = Field(default_factory=dict)


class FinancialRequest(BaseModel):
    source: str = "VCI"
    symbol: str = Field(..., min_length=1, max_length=10)
    period: str = "quarter"
    get_all: bool = True
    show_log: bool = False
    method_kwargs: Dict[str, Any] = Field(default_factory=dict)


class ListingRequest(BaseModel):
    source: str = "KBS"
    random_agent: bool = False
    show_log: bool = False
    method_kwargs: Dict[str, Any] = Field(default_factory=dict)
    #: Bỏ qua Redis cho symbols-by-exchange / symbols-by-industries, gọi lại nguồn dữ liệu.
    force_refresh: bool = False


def _listing_kwargs_for_vnstock(payload: ListingRequest) -> Dict[str, Any]:
    data = payload.model_dump()
    data.pop("force_refresh", None)
    return data


class TradingRequest(BaseModel):
    source: str = "KBS"
    symbol: str = Field(..., min_length=1, max_length=10)
    random_agent: bool = False
    show_log: bool = False
    method_kwargs: Dict[str, Any] = Field(default_factory=dict)


def _run_safe(callback):
    try:
        return {"success": True, "data": callback()}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _listing_exchange_industry_cache_key(method: str, payload: ListingRequest) -> str:
    """Khóa Redis cho listing sàn / ngành (payload.method_kwargs ảnh hưởng kết quả)."""
    source = (payload.source or "KBS").strip().upper()
    if method == "symbols_by_industries":
        lang = (payload.method_kwargs or {}).get("lang", "vi")
        lang_str = lang if isinstance(lang, str) else str(lang)
        return f"listing:{method}:{source}:lang={lang_str}"
    if method == "symbols_by_exchange":
        get_all = (payload.method_kwargs or {}).get("get_all", False)
        return f"listing:{method}:{source}:get_all={bool(get_all)}"
    raise AssertionError(f"unexpected listing cache method: {method}")


# Quote APIs
@router.post("/quote/history")
def quote_history(payload: QuoteRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_quote("history", **payload.model_dump())
    )


@router.post("/quote/intraday")
def quote_intraday(payload: QuoteRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_quote("intraday", **payload.model_dump())
    )


@router.post("/quote/price-depth")
def quote_price_depth(payload: QuoteRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_quote("price_depth", **payload.model_dump())
    )


# Company APIs
@router.post("/company/overview")
def company_overview(payload: CompanyRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_company("overview", **payload.model_dump())
    )


@router.post("/company/shareholders")
def company_shareholders(payload: CompanyRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_company("shareholders", **payload.model_dump())
    )


@router.post("/company/officers")
def company_officers(payload: CompanyRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_company("officers", **payload.model_dump())
    )


@router.post("/company/subsidiaries")
def company_subsidiaries(payload: CompanyRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_company("subsidiaries", **payload.model_dump())
    )


@router.post("/company/affiliate")
def company_affiliate(payload: CompanyRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_company("affiliate", **payload.model_dump())
    )


@router.post("/company/news")
def company_news(payload: CompanyRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_company("news", **payload.model_dump())
    )


@router.post("/company/events")
def company_events(payload: CompanyRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_company("events", **payload.model_dump())
    )


# Financial APIs
@router.post("/financial/balance-sheet")
def financial_balance_sheet(payload: FinancialRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_financial("balance_sheet", **payload.model_dump())
    )


@router.post("/financial/income-statement")
def financial_income_statement(payload: FinancialRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_financial("income_statement", **payload.model_dump())
    )


@router.post("/financial/cash-flow")
def financial_cash_flow(payload: FinancialRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_financial("cash_flow", **payload.model_dump())
    )


@router.post("/financial/ratio")
def financial_ratio(payload: FinancialRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_financial("ratio", **payload.model_dump())
    )


# Listing APIs
@router.post("/listing/all-symbols")
def listing_all_symbols(payload: ListingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_listing("all_symbols", **_listing_kwargs_for_vnstock(payload))
    )


@router.post("/listing/symbols-by-industries")
def listing_symbols_by_industries(payload: ListingRequest) -> dict:
    cache_key = _listing_exchange_industry_cache_key("symbols_by_industries", payload)
    if not payload.force_refresh:
        cached = redis_cache_service.get_json(cache_key)
        if cached is not None:
            return cached
    result = _run_safe(
        lambda: vnstock_api_service.call_listing(
            "symbols_by_industries", **_listing_kwargs_for_vnstock(payload)
        )
    )
    redis_cache_service.set_json(
        cache_key,
        result,
        max(1, int(settings.listing_exchange_industry_redis_ttl_seconds)),
    )
    return result


@router.post("/listing/symbols-by-exchange")
def listing_symbols_by_exchange(payload: ListingRequest) -> dict:
    cache_key = _listing_exchange_industry_cache_key("symbols_by_exchange", payload)
    if not payload.force_refresh:
        cached = redis_cache_service.get_json(cache_key)
        if cached is not None:
            return cached
    result = _run_safe(
        lambda: vnstock_api_service.call_listing(
            "symbols_by_exchange", **_listing_kwargs_for_vnstock(payload)
        )
    )
    redis_cache_service.set_json(
        cache_key,
        result,
        max(1, int(settings.listing_exchange_industry_redis_ttl_seconds)),
    )
    return result


@router.post("/listing/industries-icb")
def listing_industries_icb(payload: ListingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_listing("industries_icb", **_listing_kwargs_for_vnstock(payload))
    )


@router.post("/listing/symbols-by-group")
def listing_symbols_by_group(payload: ListingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_listing("symbols_by_group", **_listing_kwargs_for_vnstock(payload))
    )


@router.post("/listing/all-future-indices")
def listing_all_future_indices(payload: ListingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_listing("all_future_indices", **_listing_kwargs_for_vnstock(payload))
    )


@router.post("/listing/all-government-bonds")
def listing_all_government_bonds(payload: ListingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_listing("all_government_bonds", **_listing_kwargs_for_vnstock(payload))
    )


@router.post("/listing/all-covered-warrant")
def listing_all_covered_warrant(payload: ListingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_listing("all_covered_warrant", **_listing_kwargs_for_vnstock(payload))
    )


@router.post("/listing/all-bonds")
def listing_all_bonds(payload: ListingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_listing("all_bonds", **_listing_kwargs_for_vnstock(payload))
    )


# Trading APIs
@router.post("/trading/trading-stats")
def trading_stats(payload: TradingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_trading("trading_stats", **payload.model_dump())
    )


@router.post("/trading/side-stats")
def trading_side_stats(payload: TradingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_trading("side_stats", **payload.model_dump())
    )


@router.post("/trading/price-board")
def trading_price_board(payload: TradingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_trading("price_board", **payload.model_dump())
    )


@router.post("/trading/price-history")
def trading_price_history(payload: TradingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_trading("price_history", **payload.model_dump())
    )


@router.post("/trading/foreign-trade")
def trading_foreign_trade(payload: TradingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_trading("foreign_trade", **payload.model_dump())
    )


@router.post("/trading/prop-trade")
def trading_prop_trade(payload: TradingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_trading("prop_trade", **payload.model_dump())
    )


@router.post("/trading/insider-deal")
def trading_insider_deal(payload: TradingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_trading("insider_deal", **payload.model_dump())
    )


@router.post("/trading/order-stats")
def trading_order_stats(payload: TradingRequest) -> dict:
    return _run_safe(
        lambda: vnstock_api_service.call_trading("order_stats", **payload.model_dump())
    )
