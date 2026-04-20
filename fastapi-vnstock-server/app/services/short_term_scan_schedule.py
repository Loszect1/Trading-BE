"""Short-term market scanner schedule: VN equities regular sessions, configurable cadence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from collections.abc import Set as AbstractSet
from typing import List, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# Ho Chi Minh regular session boundaries (local time), inclusive end minute for each slot grid.
_MORNING: Tuple[str, time, time] = ("morning", time(9, 0), time(11, 30))
_AFTERNOON: Tuple[str, time, time] = ("afternoon", time(13, 0), time(14, 45))
DEFAULT_SESSIONS: Tuple[Tuple[str, time, time], ...] = (_MORNING, _AFTERNOON)


def _minutes_since_midnight(t: time) -> int:
    return t.hour * 60 + t.minute


def slot_times_for_session(
    session_start: time, session_end: time, interval_minutes: int
) -> List[time]:
    """Return ascending local times on the interval grid from start through end (inclusive)."""
    if interval_minutes < 1:
        raise ValueError("interval_minutes must be >= 1")
    start_m = _minutes_since_midnight(session_start)
    end_m = _minutes_since_midnight(session_end)
    if end_m < start_m:
        raise ValueError("session_end must be >= session_start")
    span = end_m - start_m
    if span % interval_minutes != 0:
        raise ValueError(
            "session span must be divisible by interval_minutes so the closing time is a slot"
        )
    out: List[time] = []
    for m in range(start_m, end_m + 1, interval_minutes):
        h, mm = divmod(m, 60)
        out.append(time(h, mm))
    return out


def trading_day_slot_times(interval_minutes: int) -> List[Tuple[str, time]]:
    """All short-term scan slot times for one VN regular trading day (ordered)."""
    ordered: List[Tuple[str, time]] = []
    for name, st, en in DEFAULT_SESSIONS:
        for t in slot_times_for_session(st, en, interval_minutes):
            ordered.append((name, t))
    return ordered


def is_vn_equities_weekday(d: date, holiday_dates: AbstractSet[date] | None = None) -> bool:
    """Monday–Friday excluding optional holiday set."""
    if d.weekday() >= 5:
        return False
    if holiday_dates and d in holiday_dates:
        return False
    return True


def localize_reference(dt: datetime, tz: ZoneInfo) -> datetime:
    """Interpret naive `dt` as local wall time in `tz`; aware values are converted."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def next_run_datetimes(
    *,
    after: datetime,
    count: int,
    interval_minutes: int,
    timezone_name: str,
    holiday_dates: AbstractSet[date] | None = None,
    max_calendar_days: int = 40,
) -> List[datetime]:
    """
    Next `count` scan run instants strictly after `after`, on weekdays only,
    during configured sessions in the given timezone.
    """
    if count < 1:
        raise ValueError("count must be >= 1")
    if count > 500:
        raise ValueError("count must be <= 500")
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as e:
        raise ValueError(f"Unknown timezone: {timezone_name!r}") from e

    ref = localize_reference(after, tz)
    slots_template = trading_day_slot_times(interval_minutes)
    results: List[datetime] = []
    day = ref.date()
    scanned = 0
    while scanned < max_calendar_days:
        if is_vn_equities_weekday(day, holiday_dates):
            for _session, t in slots_template:
                candidate = datetime(
                    day.year, day.month, day.day, t.hour, t.minute, tzinfo=tz
                )
                if candidate > ref:
                    results.append(candidate)
                    if len(results) >= count:
                        return results
        day += timedelta(days=1)
        scanned += 1
    raise RuntimeError(
        "exhausted max_calendar_days without collecting enough slots; check inputs"
    )


def schedule_day_preview(
    on: date, *, interval_minutes: int, timezone_name: str, holiday_dates: AbstractSet[date] | None = None
) -> Tuple[bool, List[datetime]]:
    """Whether `on` is a weekday and all slot datetimes that day (possibly empty if weekend)."""
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as e:
        raise ValueError(f"Unknown timezone: {timezone_name!r}") from e
    if not is_vn_equities_weekday(on, holiday_dates):
        return False, []
    slots_template = trading_day_slot_times(interval_minutes)
    times = [
        datetime(on.year, on.month, on.day, t.hour, t.minute, tzinfo=tz)
        for _s, t in slots_template
    ]
    return True, times


@dataclass(frozen=True)
class SessionWindow:
    name: str
    start_local: time
    end_local: time

    def slot_times(self, interval_minutes: int) -> List[time]:
        return slot_times_for_session(self.start_local, self.end_local, interval_minutes)


def default_session_windows() -> Tuple[SessionWindow, SessionWindow]:
    m = SessionWindow(_MORNING[0], _MORNING[1], _MORNING[2])
    a = SessionWindow(_AFTERNOON[0], _AFTERNOON[1], _AFTERNOON[2])
    return m, a


def validate_interval_for_default_sessions(interval_minutes: int) -> None:
    """Raise ValueError if interval does not align with fixed session boundaries."""
    for _name, st, en in DEFAULT_SESSIONS:
        slot_times_for_session(st, en, interval_minutes)


def is_instant_on_short_term_scan_grid(
    local_instant: datetime, *, interval_minutes: int, holiday_dates: AbstractSet[date] | None = None
) -> bool:
    """
    True when `local_instant` (timezone-aware, interpreted in its own tz) falls on a
    configured session slot boundary (e.g. 09:00, 09:15, …) on a weekday.
    Seconds/microseconds are ignored for grid alignment.
    """
    if interval_minutes < 1:
        raise ValueError("interval_minutes must be >= 1")
    if local_instant.tzinfo is None:
        raise ValueError("local_instant must be timezone-aware")
    if not is_vn_equities_weekday(local_instant.date(), holiday_dates):
        return False
    wall = time(local_instant.hour, local_instant.minute)
    for _name, st, en in DEFAULT_SESSIONS:
        slots = slot_times_for_session(st, en, interval_minutes)
        if wall in slots:
            return True
    return False


def is_now_on_short_term_scan_grid(
    interval_minutes: int,
    timezone_name: str,
    holiday_dates: AbstractSet[date] | None = None,
) -> bool:
    """Convenience: current time in `timezone_name` on the scan slot grid."""
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as e:
        raise ValueError(f"Unknown timezone: {timezone_name!r}") from e
    now_local = datetime.now(tz=tz)
    return is_instant_on_short_term_scan_grid(
        now_local,
        interval_minutes=interval_minutes,
        holiday_dates=holiday_dates,
    )
