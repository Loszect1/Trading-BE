from __future__ import annotations

from typing import Any, Literal, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from vnstock.connector.dnse import Trade
from app.core.config import settings

router = APIRouter(prefix="/dnse", tags=["dnse-trading"])


@router.get("/defaults")
def dnse_defaults() -> dict:
    """Public hints for FE (sub-account example from server env). No secrets."""
    sub = (settings.dnse_default_sub_account or "").strip() or None
    return {"success": True, "data": {"default_sub_account": sub}}


class DNSELoginRequest(BaseModel):
    username: Optional[str] = Field(default=None, min_length=1)
    password: Optional[str] = Field(default=None, min_length=1)
    #: JWT from POST /dnse/auth/login — when set, username/password are not required.
    access_token: Optional[str] = Field(default=None)


class DNSETradingAuthRequest(DNSELoginRequest):
    otp: str = Field(..., min_length=4, max_length=12)
    smart_otp: bool = True


class DNSESubAccountRequest(DNSELoginRequest):
    sub_account: str = Field(..., min_length=1)


class DNSELoanPackagesRequest(DNSESubAccountRequest):
    asset_type: Literal["stock", "derivative"] = "stock"


class DNSETradeCapacityRequest(DNSELoanPackagesRequest):
    symbol: str = Field(..., min_length=1, max_length=20)
    price: float = Field(..., gt=0)
    loan_package_id: Optional[int] = None


class DNSEPlaceOrderRequest(DNSETradingAuthRequest):
    sub_account: str = Field(..., min_length=1)
    symbol: str = Field(..., min_length=1, max_length=20)
    side: Literal["buy", "sell"]
    quantity: int = Field(..., gt=0)
    price: float = Field(..., gt=0)
    order_type: str = Field(..., min_length=1, max_length=6)
    loan_package_id: Optional[int] = None
    asset_type: Literal["stock", "derivative"] = "stock"


class DNSEOrderListRequest(DNSELoginRequest):
    sub_account: str = Field(..., min_length=1)
    asset_type: Literal["stock", "derivative"] = "stock"


class DNSEOrderDetailRequest(DNSEOrderListRequest):
    order_id: str = Field(..., min_length=1)


class DNSECancelOrderRequest(DNSETradingAuthRequest):
    order_id: str = Field(..., min_length=1)
    sub_account: str = Field(..., min_length=1)
    asset_type: Literal["stock", "derivative"] = "stock"


class DNSEDealsListRequest(DNSELoginRequest):
    sub_account: str = Field(..., min_length=1)
    asset_type: Literal["stock", "derivative"] = "stock"


def _normalize_response(data: Any) -> Any:
    if data is None:
        return None
    if hasattr(data, "to_dict"):
        try:
            return data.to_dict(orient="records")
        except TypeError:
            return data.to_dict()
    return data


def _dnse_error(prefix: str, exc: Exception) -> HTTPException:
    return HTTPException(status_code=500, detail=f"{prefix}: {str(exc)}")


def _login_trade(username: str, password: str) -> Trade:
    trade = Trade()
    token = trade.login(username, password)
    if not token:
        raise HTTPException(status_code=401, detail="DNSE login failed")
    return trade


def _trade_from_payload(payload: DNSELoginRequest) -> Trade:
    token = (payload.access_token or "").strip()
    if token:
        trade = Trade()
        trade.token = token
        return trade
    username, password = _resolve_credentials(payload.username, payload.password)
    return _login_trade(username, password)


def _resolve_credentials(
    username: Optional[str],
    password: Optional[str],
) -> tuple[str, str]:
    resolved_username = (username or settings.dnse_username or "").strip()
    resolved_password = (password or settings.dnse_password or "").strip()
    if not resolved_username or not resolved_password:
        raise HTTPException(
            status_code=400,
            detail="DNSE credentials missing. Set DNSE_USERNAME/DNSE_PASSWORD in .env or pass username/password in request.",
        )
    return resolved_username, resolved_password


def _login_and_verify_trading(payload: DNSETradingAuthRequest) -> tuple[Trade, Any]:
    trade = _trade_from_payload(payload)
    trading_token = trade.get_trading_token(otp=payload.otp, smart_otp=payload.smart_otp)
    if not trading_token:
        raise HTTPException(status_code=401, detail="DNSE trading token authentication failed")
    return trade, trading_token


@router.post("/auth/login")
def dnse_login(payload: DNSELoginRequest) -> dict:
    try:
        username, password = _resolve_credentials(payload.username, payload.password)
        trade = _login_trade(username, password)
        return {"success": True, "token": _normalize_response(getattr(trade, "token", None))}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE login error", exc) from exc


@router.post("/auth/email-otp")
def dnse_email_otp(payload: DNSELoginRequest) -> dict:
    try:
        trade = _trade_from_payload(payload)
        result = trade.email_otp()
        return {"success": True, "data": _normalize_response(result)}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE email OTP error", exc) from exc


@router.post("/auth/trading-token")
def dnse_trading_token(payload: DNSETradingAuthRequest) -> dict:
    try:
        _, trading_token = _login_and_verify_trading(payload)
        return {"success": True, "trading_token": _normalize_response(trading_token)}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE trading token error", exc) from exc


@router.post("/account")
def dnse_account(payload: DNSELoginRequest) -> dict:
    try:
        trade = _trade_from_payload(payload)
        return {"success": True, "data": _normalize_response(trade.account())}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE account error", exc) from exc


@router.post("/sub-accounts")
def dnse_sub_accounts(payload: DNSELoginRequest) -> dict:
    try:
        trade = _trade_from_payload(payload)
        return {"success": True, "data": _normalize_response(trade.sub_accounts())}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE sub-accounts error", exc) from exc


@router.post("/account-balance")
def dnse_account_balance(payload: DNSESubAccountRequest) -> dict:
    try:
        trade = _trade_from_payload(payload)
        result = trade.account_balance(sub_account=payload.sub_account)
        return {"success": True, "data": _normalize_response(result)}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE account balance error", exc) from exc


@router.post("/loan-packages")
def dnse_loan_packages(payload: DNSELoanPackagesRequest) -> dict:
    try:
        trade = _trade_from_payload(payload)
        result = trade.loan_packages(
            sub_account=payload.sub_account,
            asset_type=payload.asset_type,
        )
        return {"success": True, "data": _normalize_response(result)}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE loan packages error", exc) from exc


@router.post("/trade-capacities")
def dnse_trade_capacities(payload: DNSETradeCapacityRequest) -> dict:
    try:
        trade = _trade_from_payload(payload)
        result = trade.trade_capacities(
            symbol=payload.symbol.upper(),
            price=payload.price,
            sub_account=payload.sub_account,
            asset_type=payload.asset_type,
            loan_package_id=payload.loan_package_id,
        )
        return {"success": True, "data": _normalize_response(result)}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE trade capacities error", exc) from exc


@router.post("/orders/place")
def dnse_place_order(payload: DNSEPlaceOrderRequest) -> dict:
    try:
        trade, _ = _login_and_verify_trading(payload)
        result = trade.place_order(
            sub_account=payload.sub_account,
            symbol=payload.symbol.upper(),
            side=payload.side,
            quantity=payload.quantity,
            price=payload.price,
            order_type=payload.order_type.upper(),
            loan_package_id=payload.loan_package_id,
            asset_type=payload.asset_type,
        )
        if result is None:
            raise HTTPException(status_code=400, detail="Order rejected by DNSE or invalid payload")
        return {"success": True, "data": _normalize_response(result)}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE place order error", exc) from exc


@router.post("/orders/list")
def dnse_order_list(payload: DNSEOrderListRequest) -> dict:
    try:
        trade = _trade_from_payload(payload)
        result = trade.order_list(sub_account=payload.sub_account, asset_type=payload.asset_type)
        return {"success": True, "data": _normalize_response(result)}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE order list error", exc) from exc


@router.post("/orders/detail")
def dnse_order_detail(payload: DNSEOrderDetailRequest) -> dict:
    try:
        trade = _trade_from_payload(payload)
        result = trade.order_detail(
            order_id=payload.order_id,
            sub_account=payload.sub_account,
            asset_type=payload.asset_type,
        )
        return {"success": True, "data": _normalize_response(result)}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE order detail error", exc) from exc


@router.post("/orders/cancel")
def dnse_cancel_order(payload: DNSECancelOrderRequest) -> dict:
    try:
        trade, _ = _login_and_verify_trading(payload)
        result = trade.cancel_order(
            order_id=payload.order_id,
            sub_account=payload.sub_account,
            asset_type=payload.asset_type,
        )
        return {"success": True, "data": _normalize_response(result)}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE cancel order error", exc) from exc


@router.post("/deals/list")
def dnse_deals_list(payload: DNSEDealsListRequest) -> dict:
    try:
        trade = _trade_from_payload(payload)
        result = trade.deals_list(sub_account=payload.sub_account, asset_type=payload.asset_type)
        return {"success": True, "data": _normalize_response(result)}
    except HTTPException:
        raise
    except Exception as exc:
        raise _dnse_error("DNSE deals list error", exc) from exc
