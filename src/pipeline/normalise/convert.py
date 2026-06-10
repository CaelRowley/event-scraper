"""RawEvent → canonical Event conversion."""

from __future__ import annotations

import logging

from ..models import Event, RawEvent
from .dates import make_occurrence
from .prices import parse_price

log = logging.getLogger(__name__)

_STATUS_MAP = {
    "eventscheduled": "scheduled",
    "eventcancelled": "cancelled",
    "eventpostponed": "postponed",
    "eventrescheduled": "rescheduled",
    "eventmovedonline": "scheduled",
}
_MODE_MAP = {
    "offlineeventattendancemode": "offline",
    "onlineeventattendancemode": "online",
    "mixedeventattendancemode": "mixed",
}


def _schema_enum(value: str | None, table: dict, default: str) -> str:
    if not value:
        return default
    key = value.rsplit("/", 1)[-1].lower()
    return table.get(key, default)


def to_event(raw: RawEvent, city: str, tz_name: str = "Europe/Berlin") -> Event | None:
    """Returns None when the event has no parseable start — unusable for a dated feed."""
    status = _schema_enum(raw.event_status, _STATUS_MAP, raw.event_status or "scheduled")
    mode = _schema_enum(raw.attendance_mode, _MODE_MAP, raw.attendance_mode or "offline")

    occ, is_range, rng = make_occurrence(raw.start, raw.end, raw.doors, tz_name, status)
    if occ is None:
        log.debug("dropping %s %s — unparseable start %r", raw.source, raw.source_event_id, raw.start)
        return None
    occurrences = [occ]
    for extra in raw.extra_starts:
        extra_occ, extra_range, _ = make_occurrence(extra, None, None, tz_name, status)
        if extra_occ and not extra_range:
            occurrences.append(extra_occ)

    price, sold_out = parse_price(
        raw.price_text, structured_value=raw.price_value, is_free_hint=raw.is_free
    )

    ev = Event(
        id="",
        source=raw.source,
        source_event_id=raw.source_event_id,
        source_url=raw.source_url,
        title=raw.title.strip(),
        city=city,
        source_category_raw=raw.category_raw or raw.schema_type,
        is_range=is_range,
        range_start=rng[0] if rng else None,
        range_end=rng[1] if rng else None,
        event_status=status,
        attendance_mode=mode,
        venue_name=(raw.venue_name or "").strip() or None,
        address={
            k: v for k, v in {
                "street": raw.street, "postal_code": raw.postal_code,
                "city": raw.city or city.title(), "country": "DE",
            }.items() if v
        },
        lat=raw.lat,
        lon=raw.lon,
        price=price,
        image_url=raw.image_url,
        occurrences=occurrences,
        description=(raw.description or "")[:2000] or None,
    )
    if sold_out:
        ev.tags.append("sold-out")
    if price.is_free:
        ev.tags.append("free-entry")
    return ev
