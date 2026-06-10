"""eventbrite.de — server-rendered `__SERVER_DATA__` blob on /d/ listing pages (verified).

The public search API died in 2019; the robots-allowed path is the /d/ directory pages.
The ~1 MB blob must be brace-matched (a lazy regex truncates it). Events live in
data['buckets'][n]['events'] with category tags, venue, geo, and dates inline.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, timedelta

from ..extract.braces import extract_json_object
from ..fetch import BROWSER_UA, RateSpec
from ..models import RawEvent
from .base import SourceAdapter

log = logging.getLogger(__name__)

LISTING = "https://www.eventbrite.de/d/germany--berlin/events/?page={page}"
MAX_PAGES = 5

# EventbriteCategory/<id> → taxonomy. 103 (Music) and 105 (Perf & Visual Arts) split on
# subcategory; ids verified against research (full list re-check flagged pre-M6).
CATEGORY_MAP = {
    "104": "film_cinema",
    "108": "sports_fitness",
    "110": "food_drink",
    "115": "family_kids",
    "101": "community_causes",
    "102": "community_causes",
    "111": "community_causes",
    "112": "community_causes",
    "113": "community_causes",
    "114": "community_causes",
}
_CLUB_SUBCAT = ("edm", "electronic", "dance", "dj", "techno", "house", "club")
_ART_SUBCAT = ("fine art", "kunst", "design", "photograph", "sculpt", "paint")


class EventbriteAdapter(SourceAdapter):
    slug = "eventbrite"
    rate = RateSpec(min_interval=3.0, jitter=(1.0, 2.5))
    user_agent = BROWSER_UA

    def fetch_events(self, window_days: int = 14, limit: int | None = None) -> Iterator[RawEvent]:
        window_end = (date.today() + timedelta(days=window_days)).isoformat()
        seen: set[str] = set()
        yielded = 0
        for page in range(1, MAX_PAGES + 1):
            url = LISTING.format(page=page)
            try:
                resp = self.get(url)
                resp.raise_for_status()
                data = extract_json_object(resp.text, "__SERVER_DATA__")
            except Exception as exc:  # noqa: BLE001
                log.warning("eventbrite page %d failed: %s", page, exc)
                break
            if not data:
                log.warning("eventbrite page %d: no __SERVER_DATA__ blob", page)
                break
            new_on_page = 0
            for bucket in data.get("buckets", []):
                for entry in bucket.get("events") or []:
                    eid = str(entry.get("id") or "")
                    if not eid or eid in seen:
                        continue
                    seen.add(eid)
                    raw = self._to_raw(entry)
                    if raw is None or (raw.start and str(raw.start)[:10] > window_end):
                        continue
                    new_on_page += 1
                    yielded += 1
                    yield raw
                    if limit and yielded >= limit:
                        return
            if new_on_page == 0:
                return  # pagination exhausted

    def _to_raw(self, entry: dict) -> RawEvent | None:
        name = (entry.get("name") or "").strip()
        start_date = entry.get("start_date")
        if not name or not start_date or entry.get("is_online_event"):
            return None
        start_time = entry.get("start_time") or ""
        start = f"{start_date}T{start_time}" if start_time else start_date
        end = None
        if entry.get("end_date") and not entry.get("hide_end_date"):
            end_time = entry.get("end_time") or ""
            end = f"{entry['end_date']}T{end_time}" if end_time else entry["end_date"]

        venue = entry.get("primary_venue") or {}
        addr = venue.get("address") or {}

        def _coord(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None

        cat_id, subcat = None, ""
        for tag in entry.get("tags") or []:
            if tag.get("prefix") == "EventbriteCategory":
                cat_id = (tag.get("tag") or "").rsplit("/", 1)[-1]
            elif tag.get("prefix") == "EventbriteSubCategory":
                subcat = tag.get("display_name") or ""

        return RawEvent(
            source=self.slug,
            source_event_id=str(entry["id"]),
            source_url=entry.get("url") or "",
            title=name,
            start=start,
            end=end,
            date_only=not start_time,
            venue_name=venue.get("name"),
            street=addr.get("address_1"),
            postal_code=addr.get("postal_code"),
            city=addr.get("city") or "Berlin",
            lat=_coord(addr.get("latitude")),
            lon=_coord(addr.get("longitude")),
            is_free=entry.get("is_free") or None,
            image_url=((entry.get("image") or {}).get("url")),
            category_raw=f"{cat_id}|{subcat}" if cat_id else None,
            description=(entry.get("summary") or "")[:1500] or None,
            event_status="cancelled" if entry.get("is_cancelled") else "scheduled",
            payload={"eb": {k: entry.get(k) for k in
                            ("id", "name", "url", "start_date", "start_time", "tags", "summary")}},
        )

    def map_category(self, raw: RawEvent) -> str | None:
        if not raw.category_raw:
            return None
        cat_id, _, subcat = raw.category_raw.partition("|")
        sub = subcat.lower()
        if cat_id == "103":
            return "club_nightlife" if any(w in sub for w in _CLUB_SUBCAT) else "live_music"
        if cat_id == "105":
            return "art_exhibitions" if any(w in sub for w in _ART_SUBCAT) else "theatre_performance"
        return CATEGORY_MAP.get(cat_id)
