"""kulturdaten.berlin — official open-data API (CC-BY, keyless). The cultural backbone.

Endpoint (verified 2026-06-10): GET https://api-v2.kulturdaten.berlin/api/events
  - ?startDate=YYYY-MM-DD filters to events starting on/after that date
  - results are startDate-ascending; pageSize up to 500 works
  - only /api/events — /events and /api/v2/events 404
Event records reference attractions (title, description, category tags, website) and
locations (venue name, address) by ID; details are fetched capped-per-run and cached
in adapter-local tables, so coverage converges over a few runs.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import date, timedelta

from ..fetch import RateSpec
from ..models import RawEvent
from .base import SourceAdapter, ensure_scheme

log = logging.getLogger(__name__)

API = "https://api-v2.kulturdaten.berlin/api"

# attraction.category.* → our taxonomy. Unknown values are logged (telemetry-driven table).
TAG_MAP = {
    "Exhibitions": "art_exhibitions",
    "Art": "art_exhibitions",
    "Concerts": "live_music",
    "Music": "live_music",
    "Theatre": "theatre_performance",
    "Theater": "theatre_performance",
    "Dance": "theatre_performance",
    "Opera": "theatre_performance",
    "Stage": "theatre_performance",
    "Comedy": "comedy",
    "Film": "film_cinema",
    "Cinema": "film_cinema",
    "Literature": "talks_literature",
    "Readings": "talks_literature",
    "Lectures": "talks_literature",
    "Talks": "talks_literature",
    "Education": "workshops_classes",
    "Workshops": "workshops_classes",
    "Courses": "workshops_classes",
    "Markets": "markets_fairs",
    "Festivals": "festivals",
    "Children": "family_kids",
    "Family": "family_kids",
    "Youth": "family_kids",
    "Sports": "sports_fitness",
    "Food": "food_drink",
    # observed in run telemetry 2026-06-10:
    "Stages": "theatre_performance",
    "Politics": "community_causes",
    "InformationEvents": "community_causes",
    "Health": "community_causes",
    "Police": "community_causes",
    # "Recreation" and "Walks" stay unmapped — too generic; keywords/LLM decide
}

CACHE_SCHEMA = """
CREATE TABLE IF NOT EXISTS kd_attractions(id TEXT PRIMARY KEY, payload TEXT, fetched_at TEXT);
CREATE TABLE IF NOT EXISTS kd_locations(id TEXT PRIMARY KEY, payload TEXT, fetched_at TEXT);
"""


class KulturdatenAdapter(SourceAdapter):
    slug = "kulturdaten"
    rate = RateSpec(min_interval=0.5, jitter=(0.1, 0.4))
    cheap_delta = True
    detail_budget = 600  # attraction detail fetches per run; cache converges over runs

    def __init__(self, fetcher, conn):
        super().__init__(fetcher, conn)
        conn.executescript(CACHE_SCHEMA)
        self._detail_spent = 0

    # --- detail caches ----------------------------------------------------------

    def _cached(self, table: str, ref_id: str, endpoint: str, key: str) -> dict | None:
        row = self.conn.execute(f"SELECT payload FROM {table} WHERE id=?", (ref_id,)).fetchone()
        if row:
            return json.loads(row["payload"])
        if self._detail_spent >= self.detail_budget:
            return None
        try:
            resp = self.get(f"{API}/{endpoint}/{ref_id}", check_robots=False)
            if resp.status_code != 200:
                return None
            payload = resp.json().get("data", {}).get(key, {})
        except Exception as exc:  # noqa: BLE001 — one bad record must not kill the run
            log.debug("kd detail %s/%s failed: %s", endpoint, ref_id, exc)
            return None
        self._detail_spent += 1
        from ..db import now_iso
        self.conn.execute(
            f"INSERT OR REPLACE INTO {table}(id,payload,fetched_at) VALUES(?,?,?)",
            (ref_id, json.dumps(payload, ensure_ascii=False), now_iso()),
        )
        return payload

    # --- main -------------------------------------------------------------------

    def fetch_events(self, window_days: int = 14, limit: int | None = None) -> Iterator[RawEvent]:
        today = date.today()
        window_end = (today + timedelta(days=window_days)).isoformat()
        page, yielded = 1, 0
        while True:
            resp = self.get(
                f"{API}/events", params={"page": page, "pageSize": 500, "startDate": today.isoformat()},
                check_robots=False,
            )
            resp.raise_for_status()
            data = resp.json()["data"]
            events = data.get("events", [])
            if not events:
                return
            for entry in events:
                start_date = entry.get("schedule", {}).get("startDate", "")
                if start_date > window_end:
                    return  # results are startDate-ascending
                raw = self._to_raw(entry)
                if raw:
                    yielded += 1
                    yield raw
                    if limit and yielded >= limit:
                        return
            if page * 500 >= data.get("totalCount", 0):
                return
            page += 1

    def _to_raw(self, entry: dict) -> RawEvent | None:
        ident = entry.get("identifier")
        sched = entry.get("schedule", {})
        attractions = entry.get("attractions") or []
        locations = entry.get("locations") or []
        if not ident or not sched.get("startDate") or not attractions:
            return None

        title = self._label(attractions[0])
        if not title:
            return None
        venue_name = self._label(locations[0]) if locations else None

        attr = self._cached("kd_attractions", attractions[0].get("referenceId", ""), "attractions", "attraction") or {}
        loc = self._cached("kd_locations", locations[0].get("referenceId", ""), "locations", "location") if locations else None
        loc = loc or {}
        address = loc.get("address", {})

        start_time = sched.get("startTime") or "00:00:00"
        date_only = start_time == "00:00:00"
        start = sched["startDate"] if date_only else f"{sched['startDate']}T{start_time}"
        end = None
        if sched.get("endDate") and sched["endDate"] != sched["startDate"]:
            end_time = sched.get("endTime") or "00:00:00"
            end = f"{sched['endDate']}T{end_time}"
        elif sched.get("endTime") and sched["endTime"] not in ("00:00:00", start_time):
            end = f"{sched['startDate']}T{sched['endTime']}"

        ticket_type = (entry.get("admission") or {}).get("ticketType", "")
        is_free = ticket_type == "ticketType.freeOfCharge" or None

        tags = attr.get("tags") or []
        category_raw = next((t.rsplit(".", 1)[-1] for t in tags if t.startswith("attraction.category.")), None)

        status = "scheduled"
        if entry.get("scheduleStatus") == "event.cancelled":
            status = "cancelled"

        external = next((l.get("url") for l in attr.get("externalLinks") or [] if l.get("url")), None)
        return RawEvent(
            source=self.slug,
            source_event_id=ident,
            # human page when the attraction has one; the raw API record only as last resort
            source_url=ensure_scheme(attr.get("website")) or ensure_scheme(external)
            or f"{API}/events/{ident}",
            title=title,
            start=start,
            end=end,
            date_only=date_only,
            venue_name=venue_name,
            street=address.get("streetAddress"),
            postal_code=address.get("postalCode"),
            city=address.get("addressLocality") or "Berlin",
            is_free=is_free,
            category_raw=category_raw,
            description=(attr.get("description", {}) or {}).get("de"),
            description_public=True,  # CC-BY open data — exportable with attribution
            event_status=status,
            payload={"event": entry, "description": (attr.get("description", {}) or {}).get("de", "")[:1500]},
        )

    @staticmethod
    def _label(ref: dict) -> str | None:
        label = ref.get("referenceLabel") or {}
        return (label.get("de") or label.get("en") or "").strip() or None

    def map_category(self, raw: RawEvent) -> str | None:
        if raw.category_raw is None:
            return None
        mapped = TAG_MAP.get(raw.category_raw)
        if mapped is None:
            log.info("kulturdaten: unmapped category tag %r", raw.category_raw)
        return mapped
