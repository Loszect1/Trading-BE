from __future__ import annotations

import json
import logging
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Header, HTTPException, Query

from app.schemas.auto_trading import (
    AutoTradingConfigData,
    DemoAccountResponseData,
    DemoSessionOverviewData,
    DemoPositionSnapshot,
    DemoStrategyCashTransferData,
    DemoStrategyCashTransferRequest,
    DemoTradeRequest,
    DemoTradeResponseData,
    ExperienceAnalysisStatus,
)
from app.services.demo_trading_service import (
    create_demo_session,
    delete_demo_session,
    execute_demo_trade,
    get_demo_account_snapshot,
    get_demo_session_overview,
    transfer_demo_strategy_cash_from_unallocated,
    list_demo_sessions,
    normalize_demo_session_id,
)
from app.services.experience_service import create_experience_from_trade, ensure_experience_table

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auto-trading", tags=["auto-trading"])


def _resolve_demo_session_id(x_demo_session_id: Optional[str]) -> str:
    return normalize_demo_session_id(x_demo_session_id)


def _parse_optional_marks_json(raw: Optional[str]) -> dict[str, float]:
    if raw is None or not raw.strip():
        return {}
    try:
        parsed: Any = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail="marks must be a JSON object of symbol->price floats",
        ) from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="marks must be a JSON object")
    out: dict[str, float] = {}
    for key, val in parsed.items():
        sym = str(key).strip().upper()
        try:
            price = float(val)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=f"invalid mark for {sym!r}") from exc
        if price <= 0:
            raise HTTPException(status_code=400, detail=f"mark for {sym!r} must be positive")
        out[sym] = price
    return out


@router.get("/config")
def get_auto_trading_config() -> dict[str, Any]:
    try:
        payload = AutoTradingConfigData()
        return {"success": True, "data": payload.model_dump()}
    except Exception as exc:
        logger.exception("auto_trading.config_failed")
        raise HTTPException(status_code=500, detail="Failed to load auto-trading config") from exc


@router.post("/demo/new-session")
def post_new_demo_session() -> dict[str, Any]:
    try:
        session_id = create_demo_session()
        return {"success": True, "data": {"session_id": session_id}}
    except Exception as exc:
        logger.exception("auto_trading.new_demo_session_failed")
        raise HTTPException(status_code=500, detail="Failed to create demo session") from exc


@router.get("/demo/sessions")
def get_demo_sessions(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    try:
        data = list_demo_sessions(limit=limit, offset=offset)
        return {"success": True, "data": data}
    except Exception as exc:
        logger.exception("auto_trading.list_demo_sessions_failed")
        raise HTTPException(status_code=500, detail="Failed to list demo sessions") from exc


@router.delete("/demo/session-current")
def delete_current_demo_session(
    x_demo_session_id: Annotated[Optional[str], Header(alias="X-Demo-Session-Id")] = None,
) -> dict[str, Any]:
    session_id = _resolve_demo_session_id(x_demo_session_id)
    try:
        deleted = delete_demo_session(session_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Demo session not found")
        return {"success": True, "data": {"deleted_session_id": session_id}}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("auto_trading.delete_demo_session_failed", extra={"session_id": session_id})
        raise HTTPException(status_code=500, detail="Failed to delete demo session") from exc


@router.post("/demo/trades")
def post_demo_trade(
    body: DemoTradeRequest,
    x_demo_session_id: Annotated[Optional[str], Header(alias="X-Demo-Session-Id")] = None,
) -> dict[str, Any]:
    session_id = _resolve_demo_session_id(x_demo_session_id)
    try:
        result = execute_demo_trade(session_id, body)
    except ValueError as exc:
        code = str(exc)
        if code == "INSUFFICIENT_CASH":
            raise HTTPException(status_code=400, detail="Insufficient cash for this buy") from exc
        if code == "INSUFFICIENT_POSITION":
            raise HTTPException(status_code=400, detail="Cannot sell more than open position") from exc
        raise HTTPException(status_code=400, detail="Demo trade rejected") from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("auto_trading.demo_trade_failed", extra={"session_id": session_id})
        raise HTTPException(status_code=500, detail="Demo trade execution failed") from exc

    experience_status = ExperienceAnalysisStatus(recorded=False, skipped_reason="not_applicable")
    if result.experience_candidate is not None:
        try:
            ensure_experience_table()
            inserted = create_experience_from_trade(result.experience_candidate)
            experience_status = ExperienceAnalysisStatus(recorded=True, experience_row=dict(inserted))
        except Exception as exc:
            logger.warning(
                "auto_trading.experience_insert_failed",
                extra={"session_id": session_id, "trade_id": result.trade_id, "error": str(exc)},
            )
            experience_status = ExperienceAnalysisStatus(
                recorded=False,
                error_detail="Experience analysis could not be persisted",
            )

    position_model = (
        DemoPositionSnapshot.model_validate(result.position_snapshot)
        if result.position_snapshot
        else None
    )
    response_data = DemoTradeResponseData(
        trade_id=result.trade_id,
        session_id=result.session_id,
        side=result.side,
        symbol=result.symbol,
        quantity=result.quantity,
        price=result.price,
        cash_after=result.cash_after,
        position=position_model,
        realized_pnl_on_trade=result.realized_pnl_on_trade,
        cumulative_realized_pnl=result.cumulative_realized_pnl,
        experience_analysis=experience_status,
    )
    return {"success": True, "data": response_data.model_dump(mode="json")}


@router.get("/demo/account")
def get_demo_account(
    marks: Optional[str] = Query(
        default=None,
        description='Optional JSON map of symbol -> last price, e.g. {"VCB":95000}',
    ),
    history_limit: int = Query(default=50, ge=1, le=200),
    history_offset: int = Query(default=0, ge=0),
    x_demo_session_id: Annotated[Optional[str], Header(alias="X-Demo-Session-Id")] = None,
) -> dict[str, Any]:
    session_id = _resolve_demo_session_id(x_demo_session_id)
    try:
        mark_prices = _parse_optional_marks_json(marks)
        snapshot = get_demo_account_snapshot(
            session_id,
            mark_prices or None,
            history_limit=history_limit,
            history_offset=history_offset,
        )
        data = DemoAccountResponseData.model_validate(snapshot)
        return {"success": True, "data": data.model_dump(mode="json")}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("auto_trading.demo_account_failed", extra={"session_id": session_id})
        raise HTTPException(status_code=500, detail="Failed to read demo account") from exc


@router.get("/demo/overview")
def get_demo_overview(
    x_demo_session_id: Annotated[Optional[str], Header(alias="X-Demo-Session-Id")] = None,
) -> dict[str, Any]:
    session_id = _resolve_demo_session_id(x_demo_session_id)
    try:
        overview = get_demo_session_overview(session_id)
        data = DemoSessionOverviewData.model_validate(overview)
        return {"success": True, "data": data.model_dump(mode="json")}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("auto_trading.demo_overview_failed", extra={"session_id": session_id})
        raise HTTPException(status_code=500, detail="Failed to read demo overview") from exc


@router.post("/demo/strategy-cash/transfer")
def post_demo_strategy_cash_transfer(
    body: DemoStrategyCashTransferRequest,
    x_demo_session_id: Annotated[Optional[str], Header(alias="X-Demo-Session-Id")] = None,
) -> dict[str, Any]:
    session_id = _resolve_demo_session_id(x_demo_session_id)
    try:
        row = transfer_demo_strategy_cash_from_unallocated(
            session_id=session_id,
            to_strategy=body.to_strategy,
            amount_vnd=body.amount_vnd,
            from_strategy=body.from_strategy,
        )
        data = DemoStrategyCashTransferData.model_validate(row)
        return {"success": True, "data": data.model_dump(mode="json")}
    except ValueError as exc:
        code = str(exc)
        if code == "INSUFFICIENT_UNALLOCATED_CASH":
            raise HTTPException(status_code=400, detail="Source strategy cash is not enough") from exc
        if code in {"INVALID_TRANSFER_AMOUNT", "INVALID_TARGET_STRATEGY"}:
            raise HTTPException(status_code=400, detail="Invalid transfer payload") from exc
        if code in {"INVALID_SOURCE_STRATEGY", "INVALID_TRANSFER_SAME_STRATEGY"}:
            raise HTTPException(status_code=400, detail="Invalid transfer direction") from exc
        raise HTTPException(status_code=400, detail="Transfer rejected") from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("auto_trading.demo_strategy_cash_transfer_failed", extra={"session_id": session_id})
        raise HTTPException(status_code=500, detail="Failed to transfer strategy cash") from exc
