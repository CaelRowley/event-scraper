"""Datetime normalisation: Europe/Berlin via zoneinfo (never fixed offsets), German formats,
nightlife_date for club-night grouping, >24h range detection."""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from dateutil import parser as duparser

from ..models import Occurrence

GERMAN_MONTHS = {
    "januar": 1, "februar": 2, "märz": 3, "maerz": 3, "april": 4, "mai": 5, "juni": 6,
    "juli": 7, "august": 8, "september": 9, "oktober": 10, "november": 11, "dezember": 12,
}

# "Sa., 14.06.2026, 20 Uhr" / "14.06.2026 20:00" / "14.6.26"
_DE_DATE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})")
_DE_MONTHNAME = re.compile(
    r"(\d{1,2})\.?\s*(" + "|".join(GERMAN_MONTHS) + r")\w*\s*(\d{4})?", re.IGNORECASE
)
# dot-separated times ("20.30 Uhr") must carry the Uhr suffix or they collide with dd.mm dates
_TIME = re.compile(
    r"(\d{1,2}):(\d{2})(?:\s*uhr)?|(\d{1,2})\.(\d{2})\s*uhr|(\d{1,2})\s*uhr", re.IGNORECASE
)
_ISO_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_dt(value, tz_name: str = "Europe/Berlin") -> tuple[datetime | None, bool]:
    """Interpret a raw datetime value. Returns (aware datetime in venue tz, date_only flag).

    Naive values are localised with zoneinfo — DST-correct, unlike fixed offsets.
    Date-only values get 00:00 but are flagged so callers never display a fabricated time.
    """
    tz = ZoneInfo(tz_name)
    if value is None:
        return None, False
    if isinstance(value, datetime):
        dt = value
        return (dt if dt.tzinfo else dt.replace(tzinfo=tz)), False
    if isinstance(value, date):
        return datetime.combine(value, time(0, 0), tzinfo=tz), True
    s = str(value).strip()
    if not s:
        return None, False

    if _ISO_DATE_ONLY.match(s):
        d = date.fromisoformat(s)
        return datetime.combine(d, time(0, 0), tzinfo=tz), True

    # ISO-ish first (covers JSON-LD startDate with or without offset)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return (dt if dt.tzinfo else dt.replace(tzinfo=tz)), False
    except ValueError:
        pass

    # German textual formats
    d = _parse_german_date(s)
    if d:
        t, time_found = _parse_time(s)
        return datetime.combine(d, t, tzinfo=tz), not time_found

    # Last resort: dateutil with day-first (German dd.mm ordering)
    try:
        dt = duparser.parse(s, dayfirst=True, fuzzy=True)
        return (dt if dt.tzinfo else dt.replace(tzinfo=tz)), False
    except (ValueError, OverflowError):
        return None, False


def _parse_german_date(s: str) -> date | None:
    m = _DE_DATE.search(s)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:
            year += 2000
        try:
            return date(year, month, day)
        except ValueError:
            return None
    m = _DE_MONTHNAME.search(s)
    if m:
        day = int(m.group(1))
        month = GERMAN_MONTHS[m.group(2).lower()]
        year = int(m.group(3)) if m.group(3) else datetime.now().year
        try:
            return date(year, month, day)
        except ValueError:
            return None
    return None


def _parse_time(s: str) -> tuple[time, bool]:
    m = _TIME.search(s)
    if not m:
        return time(0, 0), False
    if m.group(1) is not None:
        hh, mm = int(m.group(1)), int(m.group(2))
    elif m.group(3) is not None:
        hh, mm = int(m.group(3)), int(m.group(4))
    else:
        hh, mm = int(m.group(5)), 0
    if hh > 23 or mm > 59:
        return time(0, 0), False
    return time(hh, mm), True


def nightlife_date(local_dt: datetime) -> str:
    """Local date of (start − 6h): a 01:00 club start belongs to the previous evening."""
    return (local_dt - timedelta(hours=6)).date().isoformat()


def to_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_occurrence(start, end=None, doors=None, tz_name: str = "Europe/Berlin",
                    status: str = "scheduled") -> tuple[Occurrence | None, bool, tuple | None]:
    """Build an Occurrence from raw values.

    Returns (occurrence, is_range, (range_start, range_end)).
    is_range is True when duration > 24h (exhibition-style) — caller stores the range
    on the event instead of exploding daily occurrences.
    """
    start_dt, date_only = parse_dt(start, tz_name)
    if start_dt is None:
        return None, False, None
    end_dt, _ = parse_dt(end, tz_name)
    doors_dt, _ = parse_dt(doors, tz_name)

    if end_dt is not None and end_dt < start_dt:
        # "Sat 23:59 – Sun 08:00" style listings sometimes drop the day from the end time
        if (start_dt - end_dt) < timedelta(hours=24):
            end_dt += timedelta(days=1)
        else:
            end_dt = None

    if end_dt is not None and (end_dt - start_dt) > timedelta(hours=24):
        rng = (start_dt.date().isoformat(), end_dt.date().isoformat())
        occ = Occurrence(
            starts_at_utc=to_utc_iso(start_dt),
            starts_at_local=start_dt.isoformat(),
            nightlife_date=nightlife_date(start_dt),
            ends_at_utc=to_utc_iso(end_dt),
            time_unknown=date_only,
            status=status,
        )
        return occ, True, rng

    occ = Occurrence(
        starts_at_utc=to_utc_iso(start_dt),
        starts_at_local=start_dt.isoformat(),
        nightlife_date=nightlife_date(start_dt),
        ends_at_utc=to_utc_iso(end_dt) if end_dt else None,  # never invent durations
        doors_at_local=doors_dt.isoformat() if doors_dt else None,
        time_unknown=date_only,
        status=status,
    )
    return occ, False, None
