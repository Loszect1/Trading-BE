from __future__ import annotations

from typing import Literal, Optional, Annotated

from fastapi import APIRouter, Body, HTTPException, Query, Header

from app.schemas.monitoring import KillSwitchRequest, MonitoringEvaluateRequest
from app.services.monitoring_service import (
    build_account_monitoring_dashboard,
    evaluate_alerts_for_modes,
    get_monitoring_summary_all,
    list_recent_runtime_logs,
    list_recent_alerts,
)
from app.services.trading_core_service import set_kill_switch

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


@router.get("/summary")
def monitoring_summary(
    account_mode: Optional[Literal["REAL", "DEMO"]] = Query(
        default=None,
        description="When set, returns merged operations dashboard (trading summary + KPI + health). "
        "When omitted, returns aggregate for both modes (legacy).",
    ),
    sub_account: Optional[str] = Query(
        default=None,
        description="Optional DNSE sub-account for REAL KPI server-side valuation.",
    ),
    x_dnse_access_token: Annotated[Optional[str], Header(alias="X-Dnse-Access-Token")] = None,
) -> dict:
    try:
        if account_mode is not None:
            data = build_account_monitoring_dashboard(
                str(account_mode).upper(),
                sub_account=sub_account,
                dnse_access_token=x_dnse_access_token,
            )
            data["recent_alerts"] = list_recent_alerts(limit=30)
            return {"success": True, "data": data}
        summary = get_monitoring_summary_all()
        summary["recent_alerts"] = list_recent_alerts(limit=30)
        return {"success": True, "data": summary}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Monitoring summary error: {str(exc)}") from exc


@router.post("/evaluate-alerts")
def monitoring_evaluate_alerts(body: MonitoringEvaluateRequest = Body(...)) -> dict:
    try:
        data = evaluate_alerts_for_modes(body.account_modes)
        return {"success": True, "data": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Alert evaluation error: {str(exc)}") from exc


@router.post("/kill-switch")
def monitoring_kill_switch(body: KillSwitchRequest) -> dict:
    try:
        row = set_kill_switch(body.account_mode, body.active, body.reason)
        return {"success": True, "data": row}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Kill switch update error: {str(exc)}") from exc


@router.get("/runtime-logs")
def monitoring_runtime_logs(
    account_mode: Optional[Literal["REAL", "DEMO"]] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict:
    try:
        data = list_recent_runtime_logs(account_mode=account_mode, limit=limit)
        return {"success": True, "data": data, "limit": limit, "account_mode": account_mode}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Runtime logs error: {str(exc)}") from exc
