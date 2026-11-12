"""Resident Advisor — OFF by default (`sources.py`: needs_permission).

ra.co's Terms of Use (4.4(a), 4.4(f)) allow automated access, and extraction for
commercial purposes, only under a written agreement with RA. This adapter queries
the GraphQL endpoint RA's own website uses, which is undocumented and used to be
reached by sending a browser User-Agent and a ra.co Referer. Both are gone: it now
identifies itself like every other request, and it does not run unless RA has
agreed and `ra` is named in PIPELINE_PERMITTED_SOURCES.

POST https://ra.co/graphql. Berlin = area 34. ~70 listings/day.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, timedelta

from ..fetch import RateSpec
from ..models import RawEvent
from .base import SourceAdapter

log = logging.getLogger(__name__)

GRAPHQL = "https://ra.co/graphql"
AREA_BERLIN = 34
PAGE_SIZE = 50

QUERY = """
query($filters:FilterInputDtoInput,$pageSize:Int,$page:Int){
  eventListings(filters:$filters,pageSize:$pageSize,page:$page){
    data{ id listingDate event{
      id title contentUrl startTime endTime cost isTicketed flyerFront
      images{ filename type }
      venue{ id name contentUrl area{name} location{latitude longitude} }
    }}
    totalResults
  }
}
"""


class ResidentAdvisorAdapter(SourceAdapter):
    slug = "ra"
    category_prior = "club_nightlife"
    rate = RateSpec(min_interval=1.0, jitter=(0.2, 0.8))
    cheap_delta = True

    def fetch_events(self, window_days: int = 14, limit: int | None = None) -> Iterator[RawEvent]:
        today = date.today()
        filters = {
            "areas": {"eq": AREA_BERLIN},
            "listingDate": {
                "gte": today.isoformat(),
                "lte": (today + timedelta(days=window_days)).isoformat(),
            },
        }
        page, seen, yielded = 1, 0, 0
        total = None
        while total is None or seen < total:
            resp = self.post(
                GRAPHQL,
                json={"query": QUERY, "variables": {"filters": filters, "pageSize": PAGE_SIZE, "page": page}},
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            body = resp.json()
            if body.get("errors"):
                raise RuntimeError(f"RA GraphQL errors: {body['errors'][:1]}")
            listing = body["data"]["eventListings"]
            total = listing["totalResults"]
            entries = listing["data"]
            if not entries:
                return
            for entry in entries:
                seen += 1
                raw = self._to_raw(entry)
                if raw:
                    yielded += 1
                    yield raw
                    if limit and yielded >= limit:
                        return
            page += 1

    def _to_raw(self, entry: dict) -> RawEvent | None:
        ev = entry.get("event") or {}
        if not ev.get("id") or not ev.get("title") or not ev.get("startTime") or not ev.get("contentUrl"):
            return None
        venue = ev.get("venue") or {}
        loc = venue.get("location") or {}
        cost = (ev.get("cost") or "").strip()
        price_value = None
        if cost:
            try:
                price_value = float(cost.replace(",", "."))
            except ValueError:
                pass
        # flyerFront is usually null in listings; images[] carries full URLs (verified)
        images = ev.get("images") or []
        flyer = next((i.get("filename") for i in images if i.get("type") == "FLYERFRONT"), None) \
            or next((i.get("filename") for i in images), None) or ev.get("flyerFront")
        if flyer and not flyer.startswith("http"):
            flyer = f"https://imgproxy.ra.co/_/quality:66/{flyer}"

        return RawEvent(
            source=self.slug,
            source_event_id=str(ev["id"]),
            source_url=f"https://ra.co{ev.get('contentUrl', '')}",
            title=ev["title"],
            start=ev["startTime"],  # naive local ISO — zoneinfo-localised downstream
            end=ev.get("endTime"),
            venue_name=venue.get("name"),
            city="Berlin",
            lat=loc.get("latitude"),
            lon=loc.get("longitude"),
            price_value=price_value,
            price_text=cost or None,
            image_url=flyer,
            payload={"listing": entry},
        )
