"""Canonical data models. One Event per (source, source_event_id); one Occurrence per dated instance."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Price:
    min: float | None = None
    max: float | None = None
    presale: float | None = None
    door: float | None = None
    reduced: float | None = None
    currency: str = "EUR"
    is_free: bool = False
    type: str = "unknown"  # fixed|from|range|donation|free|unknown
    text: str | None = None  # raw — recovery path for mis-parses

    def to_json(self) -> dict:
        return dataclasses.asdict(self)


@dataclass
class RawEvent:
    """Loosely-typed event as emitted by a source adapter. Normalisation tightens it."""

    source: str
    source_event_id: str
    source_url: str
    title: str
    start: str | datetime | None = None  # raw value; normaliser interprets
    end: str | datetime | None = None
    doors: str | datetime | None = None
    date_only: bool = False
    venue_name: str | None = None
    street: str | None = None
    postal_code: str | None = None
    city: str | None = None
    lat: float | None = None
    lon: float | None = None
    price_text: str | None = None
    price_value: float | None = None  # structured price if source provides one
    is_free: bool | None = None
    image_url: str | None = None
    category_raw: str | None = None  # whatever the source said
    schema_type: str | None = None  # JSON-LD @type
    description: str | None = None  # used for classification only — never exported/stored verbatim
    event_status: str = "scheduled"
    attendance_mode: str = "offline"
    extra_starts: list = field(default_factory=list)  # additional occurrence datetimes
    payload: dict = field(default_factory=dict)  # raw payload for raw_snapshots


@dataclass
class Occurrence:
    starts_at_utc: str
    starts_at_local: str
    nightlife_date: str
    ends_at_utc: str | None = None
    doors_at_local: str | None = None
    time_unknown: bool = False
    status: str = "scheduled"


@dataclass
class Event:
    """Normalised canonical event (cluster-head fields live here after dedup)."""

    id: str
    source: str
    source_event_id: str
    source_url: str
    title: str
    city: str
    canonical_id: str = ""
    category: str = "other"
    category_confidence: float = 0.0
    category_tier: str = "fallback_other"
    tags: list[str] = field(default_factory=list)
    source_category_raw: str | None = None
    is_range: bool = False
    range_start: str | None = None
    range_end: str | None = None
    event_status: str = "scheduled"
    attendance_mode: str = "offline"
    venue_name: str | None = None
    venue_id: str | None = None
    address: dict = field(default_factory=dict)
    lat: float | None = None
    lon: float | None = None
    price: Price = field(default_factory=Price)
    image_url: str | None = None
    occurrences: list[Occurrence] = field(default_factory=list)
    description: str | None = None  # in-memory only, for classification

    def content_hash(self) -> str:
        """Hash of the normalised payload — gates downstream work on re-crawls."""
        basis = {
            "title": self.title,
            "occ": [(o.starts_at_utc, o.ends_at_utc, o.status) for o in self.occurrences],
            "venue": self.venue_name,
            "price": self.price.to_json(),
            "image": self.image_url,
            "status": self.event_status,
            "range": (self.is_range, self.range_start, self.range_end),
        }
        return "sha256:" + hashlib.sha256(
            json.dumps(basis, sort_keys=True, default=str).encode()
        ).hexdigest()
