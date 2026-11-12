"""Pure calling-window maths. No I/O, no clock: every function takes `t` explicitly.

All instants are timezone-aware datetimes. Windows are interpreted in IST.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .config import Window

IST = timezone(timedelta(hours=5, minutes=30))


def to_ist(t: datetime) -> datetime:
    if t.tzinfo is None:
        raise ValueError("naive datetime: every instant in Switchboard must be timezone-aware")
    return t.astimezone(IST)


def _bounds(day: datetime, w: Window, end_guard_s: float) -> tuple[datetime, datetime]:
    s = day.replace(hour=w.start.hour, minute=w.start.minute, second=0, microsecond=0)
    e = day.replace(hour=w.end.hour, minute=w.end.minute, second=0, microsecond=0)
    return s, e - timedelta(seconds=end_guard_s)


def is_open(t: datetime, w: Window, end_guard_s: float = 0.0) -> bool:
    """True if a call may be STARTED at `t`.

    `end_guard_s` shrinks the end of the window. We never start a call in the last few simulated
    seconds before close: the request still has to travel to the provider, and at 600x speed
    every real millisecond is 0.6 simulated seconds.
    """
    lt = to_ist(t)
    if lt.weekday() not in w.days:
        return False
    s, e = _bounds(lt, w, end_guard_s)
    return s <= lt < e


def next_open(t: datetime, w: Window, end_guard_s: float = 0.0) -> datetime:
    """The earliest instant >= t at which `is_open` is true."""
    lt = to_ist(t)
    if not w.days or w.start >= w.end:
        raise ValueError(f"window {w} never opens")
    for d in range(8):
        day = lt + timedelta(days=d)
        if day.weekday() not in w.days:
            continue
        s, e = _bounds(day, w, end_guard_s)
        if d == 0:
            if lt < s:
                return s
            if lt < e:
                return lt
            continue
        return s
    raise AssertionError("unreachable: a non-empty weekly window opens within 8 days")


def window_close(t: datetime, w: Window) -> datetime | None:
    """When the window that is open at `t` closes, or None if it is closed now. Used by the dashboard."""
    if not is_open(t, w):
        return None
    return _bounds(to_ist(t), w, 0)[1]
