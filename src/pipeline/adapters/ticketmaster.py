"""Ticketmaster Discovery API — official free key (5,000 calls/day, 5 req/s, size*page<1000).

ToS forbids caching beyond reasonable periods → we store minimal fields and refresh
every run. Supplement only: Eventim dominates German ticketing and is closed.
Skips itself cleanly when TICKETMASTER_KEY is unset.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from datetime import date, datetime, timedelta

from ..fetch import RateSpec
from ..models import RawEvent
from .base import SourceAdapter
from ..categorise.taxonomy import map_ticketmaster

log = logging.getLogger(__name__)

API = "https://app.ticketmaster.com/discovery/v2/events.json"


class TicketmasterAdapter(SourceAdapter):
    slug = "ticketmaster"
    rate = RateSpec(min_interval=0.25, jitter=(0.05, 0.2))  # well under 5 req/s
    cheap_delta = False

    def fetch_events(self, window_days: int = 14, limit: int | None = None) -> Iterator[RawEvent]:
        key = os.environ.get("TICKETMASTER_KEY")
        if not key:
            log.info("ticketmaster skipped: no TICKETMASTER_KEY")
            return
        today = datetime.now()
        params = {
            "apikey": key,
            "city": "Berlin",
            "countryCode": "DE",
            "size": 100,
            "page": 0,
            "startDateTime": today.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDateTime": (today + timedelta(days=window_days)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sort": "date,asc",
        }
        yielded = 0
        while params["page"] * params["size"] < 1000:  # documented deep-paging cap
            resp = self.get(API, params=params, check_robots=False)
            if resp.status_code == 401:
                log.error("ticketmaster: key rejected")
                return
            resp.raise_for_status()
            body = resp.json()
            events = (body.get("_embedded") or {}).get("events", [])
            if not events:
                return
            for entry in events:
                raw = self._to_raw(entry)
                if raw:
                    yielded += 1
                    yield raw
                    if limit and yielded >= limit:
                        return
            page_info = body.get("page", {})
            if params["page"] + 1 >= page_info.get("totalPages", 0):
                return
            params["page"] += 1

    def _to_raw(self, entry: dict) -> RawEvent | None:
        name = (entry.get("name") or "").strip()
        dates = (entry.get("dates") or {}).get("start", {})
        start = dates.get("dateTime") or dates.get("localDate")
        if not name or not start:
            return None
        venues = (entry.get("_embedded") or {}).get("venues") or [{}]
        venue = venues[0]
        loc = venue.get("location") or {}
        cls = (entry.get("classifications") or [{}])[0]
        segment = (cls.get("segment") or {}).get("name")
        genre = (cls.get("genre") or {}).get("name")
        prices = entry.get("priceRanges") or []
        price_min = min((p.get("min") for p in prices if p.get("min") is not None), default=None)
        price_max = max((p.get("max") for p in prices if p.get("max") is not None), default=None)
        price_text = None
        if price_min is not None:
            price_text = (f"{price_min:g}–{price_max:g} €"
                          if price_max and price_max != price_min else f"{price_min:g} €")
        images = entry.get("images") or []
        image = next((i.get("url") for i in sorted(images, key=lambda i: -(i.get("width") or 0))), None)

        def _coord(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        return RawEvent(
            source=self.slug,
            source_event_id=str(entry.get("id")),
            source_url=entry.get("url") or "",
            title=name,
            start=start,
            date_only="T" not in str(start),
            venue_name=venue.get("name"),
            street=(venue.get("address") or {}).get("line1"),
            postal_code=venue.get("postalCode"),
            city=(venue.get("city") or {}).get("name") or "Berlin",
            lat=_coord(loc.get("latitude")),
            lon=_coord(loc.get("longitude")),
            price_text=price_text,
            price_value=price_min if price_min == price_max else None,
            image_url=image,
            category_raw=f"{segment}|{genre}" if segment else None,
            event_status="cancelled" if dates.get("dateTBD") is False and
                          (entry.get("dates", {}).get("status", {}).get("code") == "cancelled")
                          else "scheduled",
            payload={"tm": {k: entry.get(k) for k in ("id", "name", "url", "dates", "classifications")}},
        )

    def map_category(self, raw: RawEvent) -> str | None:
        if not raw.category_raw:
            return None
        segment, _, genre = raw.category_raw.partition("|")
        return map_ticketmaster(segment, genre or None)
