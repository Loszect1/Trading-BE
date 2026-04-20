from __future__ import annotations

from typing import Literal, Optional

from fastapi import APIRouter, HTTPException, Query

from app.schemas.trading_core import (
    ExecutionCancelRequest,
    ExecutionPlaceRequest,
    PortfolioSummary,
    PositionRow,
    RiskEvaluateRequest,
    SettlementCheckRequest,
    SettlementRow,
)
from app.services.trading_core_service import (
    cancel_order,
    check_settlement,
    evaluate_risk,
    list_orders,
    list_risk_events,
    get_portfolio_summary,
    get_order_events,
    get_positions,
    get_settlement_rows,
    place_order,
    process_order,
    reconcile_order,
)

router = APIRouter(tags=["trading-core"])


@router.get("/risk/events")
def risk_events(
    account_mode: Optional[Literal["REAL", "DEMO"]] = Query(default=None),
    event_type: Optional[str] = Query(default=None, max_length=80),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict:
    try:
        rows = list_risk_events(account_mode, limit=limit, event_type=event_type)
        return {"success": True, "data": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Risk events read error: {str(exc)}") from exc


@router.post("/risk/evaluate")
def risk_evaluate(payload: RiskEvaluateRequest) -> dict:
    try:
        return {"success": True, "data": evaluate_risk(payload.model_dump())}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Risk evaluate error: {str(exc)}") from exc


@router.post("/risk/check-settlement")
def risk_check_settlement(payload: SettlementCheckRequest) -> dict:
    try:
        data = check_settlement(payload.account_mode, payload.symbol, payload.quantity)
        return {"success": True, "data": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Settlement check error: {str(exc)}") from exc


@router.post("/execution/place")
def execution_place(payload: ExecutionPlaceRequest) -> dict:
    try:
        if payload.side == "BUY":
            stoploss = payload.stoploss_price if payload.stoploss_price else payload.price * 0.97
            risk_result = evaluate_risk(
                {
                    "account_mode": payload.account_mode,
                    "symbol": payload.symbol,
                    "nav": payload.nav,
                    "risk_per_trade": payload.risk_per_trade,
                    "entry_price": payload.price,
                    "stoploss_price": stoploss,
                    "daily_new_orders": 0,
                    "max_daily_new_orders": 10,
                }
            )
            if not risk_result["pass"]:
                return {
                    "success": False,
                    "data": {
                        "status": "REJECTED",
                        "reason": f"risk_reject:{risk_result['reason']}",
                    },
                }
        if payload.side == "SELL":
            settlement_result = check_settlement(payload.account_mode, payload.symbol, payload.quantity)
            if not settlement_result["pass"]:
                return {
                    "success": False,
                    "data": {
                        "status": "REJECTED",
                        "reason": "settlement_guard_failed",
                        **settlement_result,
                    },
                }
        row = place_order(payload.model_dump())
        if str(row.get("reason") or "") == "kill_switch_active":
            return {"success": False, "data": row}
        return {"success": True, "data": row}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Execution place error: {str(exc)}") from exc


@router.post("/execution/process/{order_id}")
def execution_process(order_id: str) -> dict:
    try:
        data = process_order(order_id)
        return {"success": bool(data.get("processed")), "data": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Execution process error: {str(exc)}") from exc


@router.post("/execution/reconcile/{order_id}")
def execution_reconcile(order_id: str) -> dict:
    """Poll broker for PARTIAL/ACK/SENT/NEW orders (REAL + dnse_live resumes via process_order)."""
    try:
        data = reconcile_order(order_id)
        return {"success": bool(data.get("processed")), "data": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Execution reconcile error: {str(exc)}") from exc


@router.post("/execution/cancel")
def execution_cancel(payload: ExecutionCancelRequest) -> dict:
    try:
        result = cancel_order(payload.order_id, payload.reason)
        return {"success": bool(result.get("cancelled")), "data": result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Execution cancel error: {str(exc)}") from exc


@router.get("/orders")
def orders(
    account_mode: Literal["REAL", "DEMO"] = Query(default="DEMO"),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict:
    try:
        rows = list_orders(account_mode, limit)
        return {"success": True, "data": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Orders read error: {str(exc)}") from exc


@router.get("/orders/{order_id}/events")
def order_events(order_id: str) -> dict:
    try:
        rows = get_order_events(order_id)
        return {"success": True, "data": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Order events read error: {str(exc)}") from exc


@router.get("/positions")
def positions(
    account_mode: Literal["REAL", "DEMO"] = Query(default="DEMO"),
) -> dict:
    try:
        rows = [PositionRow.model_validate(item).model_dump() for item in get_positions(account_mode)]
        return {"success": True, "data": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Positions read error: {str(exc)}") from exc


@router.get("/positions/settlement")
def positions_settlement(
    account_mode: Literal["REAL", "DEMO"] = Query(default="DEMO"),
    symbol: Optional[str] = Query(default=None),
) -> dict:
    try:
        rows = [
            SettlementRow.model_validate(item).model_dump(mode="json")
            for item in get_settlement_rows(account_mode, symbol.upper() if symbol else None)
        ]
        return {"success": True, "data": rows}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Settlement read error: {str(exc)}") from exc


@router.get("/portfolio/summary")
def portfolio_summary(
    account_mode: Literal["REAL", "DEMO"] = Query(default="DEMO"),
) -> dict:
    try:
        data = PortfolioSummary.model_validate(get_portfolio_summary(account_mode)).model_dump()
        return {"success": True, "data": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Portfolio summary error: {str(exc)}") from exc
