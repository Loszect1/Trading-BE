"""Operational endpoints for scanner scheduling (short-term cadence)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from zoneinfo import ZoneInfo

from app.core.config import get_vn_market_holiday_dates, settings
from app.services.short_term_scan_schedule import (
    default_session_windows,
    next_run_datetimes,
    schedule_day_preview,
)

router = APIRouter(prefix="/scanner", tags=["scanner"])


class SessionScheduleBlock(BaseModel):
    name: str
    start_local: str = Field(description="HH:MM local")
    end_local: str = Field(description="HH:MM local (inclusive last slot)")
    slot_times_local: List[str] = Field(description="HH:MM for each run in this session")


class ShortTermScheduleResponse(BaseModel):
    timezone: str
    interval_minutes: int
    reference_after: datetime
    upcoming_runs: List[datetime]
    sessions: List[SessionScheduleBlock]
    day_preview_date: date
    day_preview_is_trading_weekday: bool
    day_preview_slots: List[datetime]


def _fmt_hm(t) -> str:
    return t.strftime("%H:%M")


@router.get("/short-term/schedule", response_model=ShortTermScheduleResponse)
def get_short_term_scan_schedule(
    after: Optional[datetime] = Query(
        default=None,
        description="Return runs strictly after this instant (ISO 8601). "
        "Naive values are interpreted as wall time in the configured market timezone. "
        "If omitted, current UTC time is used.",
    ),
    limit: int = Query(
        default=32,
        ge=1,
        le=200,
        description="Maximum number of upcoming runs to return",
    ),
    preview_date: Optional[date] = Query(
        default=None,
        description="Optional calendar date (YYYY-MM-DD) for same-day slot listing in market TZ",
    ),
) -> ShortTermScheduleResponse:
    """
    Inspect short-term scanner cadence: VN regular sessions, configurable interval (default 15m).
    """
    interval = settings.short_term_scan_interval_minutes
    tz_name = settings.short_term_scan_timezone
    ref = after if after is not None else datetime.now(tz=timezone.utc)
    holiday_dates = get_vn_market_holiday_dates()

    try:
        upcoming = next_run_datetimes(
            after=ref,
            count=limit,
            interval_minutes=interval,
            timezone_name=tz_name,
            holiday_dates=holiday_dates,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    zi = ZoneInfo(tz_name)
    if ref.tzinfo is None:
        ref_local_cal = ref.replace(tzinfo=zi)
    else:
        ref_local_cal = ref.astimezone(zi)
    pv = preview_date or ref_local_cal.date()

    morning, afternoon = default_session_windows()
    sessions: List[SessionScheduleBlock] = []
    for w in (morning, afternoon):
        try:
            st_list = w.slot_times(interval)
        except ValueError as e:
            raise HTTPException(status_code=500, detail=f"Invalid schedule config: {e}") from e
        sessions.append(
            SessionScheduleBlock(
                name=w.name,
                start_local=_fmt_hm(w.start_local),
                end_local=_fmt_hm(w.end_local),
                slot_times_local=[_fmt_hm(t) for t in st_list],
            )
        )

    try:
        is_weekday, day_slots = schedule_day_preview(
            pv, interval_minutes=interval, timezone_name=tz_name, holiday_dates=holiday_dates
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e

    return ShortTermScheduleResponse(
        timezone=tz_name,
        interval_minutes=interval,
        reference_after=ref,
        upcoming_runs=upcoming,
        sessions=sessions,
        day_preview_date=pv,
        day_preview_is_trading_weekday=is_weekday,
        day_preview_slots=day_slots,
    )
