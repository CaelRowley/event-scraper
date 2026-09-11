"""WordPress 'The Events Calendar' (Tribe) public REST API.

Several Berlin comedy/cabaret venues run the Events Calendar plugin, which exposes an
unauthenticated JSON feed at /wp-json/tribe/events/v1/events with full date, venue,
price, and image fields — no scraping needed. One client drives them all; each instance
pins a category_prior (a comedy club's listings are all comedy; a burlesque house's are
all theatre/performance) and an optional house_venue for feeds whose events omit the
venue object (single-venue sites list events without repeating their own address).
"""

from __future__ import annotations

import html
import logging
import re
from collections.abc import Iterator
from datetime import date, timedelta

from ..fetch import BROWSER_UA, RateSpec
from ..models import RawEvent
from .base import SourceAdapter

log = logging.getLogger(__name__)

_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(_TAG_RE.sub(" ", text))).strip()


def _coord(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _price(cost_details: dict, cost: str | None) -> tuple[float | None, str | None]:
    """Prefer the structured numeric values; fall back to the raw cost string."""
    nums: list[float] = []
    for v in cost_details.get("values") or []:
        try:
            nums.append(float(str(v).replace(",", ".")))
        except (TypeError, ValueError):
            continue
    nums = sorted(set(nums))
    if len(nums) >= 2:
        return None, f"{nums[0]:g}–{nums[-1]:g} €"  # range → let parser split
    if len(nums) == 1:
        return nums[0], None  # structured single value beats text
    return None, (cost or "").strip() or None


class TribeEventsAdapter(SourceAdapter):
    base_url: str = ""  # e.g. https://www.comedyclubberlin.com (no trailing slash)
    house_venue: str | None = None  # used when an event omits its venue object
    page_budget = 6  # API pages per run; PER_PAGE=50 → ~300 events cap
    PER_PAGE = 50
    rate = RateSpec(min_interval=2.0, jitter=(0.5, 1.5))

    def fetch_events(self, window_days: int = 14, limit: int | None = None) -> Iterator[RawEvent]:
        today = date.today()
        url = (
            f"{self.base_url}/wp-json/tribe/events/v1/events"
            f"?per_page={self.PER_PAGE}&start_date={today.isoformat()}"
            f"&end_date={(today + timedelta(days=window_days)).isoformat()}"
        )
        yielded = 0
        for _ in range(self.page_budget):
            try:
                resp = self.get(url)
                if resp.status_code != 200:
                    log.warning("%s: HTTP %s on %s", self.slug, resp.status_code, url)
                    return
                data = resp.json()
            except Exception as exc:  # noqa: BLE001
                log.warning("%s page failed: %s", self.slug, exc)
                return
            for entry in data.get("events") or []:
                raw = self._to_raw(entry)
                if raw is None:
                    continue
                yielded += 1
                yield raw
                if limit and yielded >= limit:
                    return
            url = data.get("next_rest_url")
            if not url:
                return

    def _to_raw(self, entry: dict) -> RawEvent | None:
        # Tribe returns titles/venue names HTML-entity-encoded ("Teacher&#8217;s Pet").
        title = _strip_html(entry.get("title") or "")
        start = entry.get("start_date")
        eid = entry.get("id")
        source_url = entry.get("url") or ""
        if not title or not start or eid is None or not source_url:
            return None
        all_day = bool(entry.get("all_day"))
        # Tribe emits "YYYY-MM-DD HH:MM:SS"; the normaliser wants ISO 'T' separators.
        start = start[:10] if all_day else start.replace(" ", "T")
        end = entry.get("end_date")
        end = (end.replace(" ", "T") if end and not all_day else None)

        venue = entry.get("venue") if isinstance(entry.get("venue"), dict) else {}
        image = entry.get("image") if isinstance(entry.get("image"), dict) else {}
        price_value, price_text = _price(entry.get("cost_details") or {}, entry.get("cost"))

        return RawEvent(
            source=self.slug,
            source_event_id=str(eid),
            source_url=source_url,
            title=title,
            start=start,
            end=end,
            date_only=all_day,
            venue_name=(_strip_html(venue["venue"]) if venue.get("venue") else self.house_venue),
            street=venue.get("address"),
            postal_code=venue.get("zip"),
            city=venue.get("city") or "Berlin",
            lat=_coord(venue.get("geo_lat")),
            lon=_coord(venue.get("geo_lng")),
            price_value=price_value,
            price_text=price_text,
            image_url=image.get("url"),
            description=_strip_html(entry.get("description") or entry.get("excerpt") or "")[:1500] or None,
            payload={"tribe": {k: entry.get(k) for k in
                               ("id", "title", "url", "start_date", "cost")}},
        )


class ComedyInEnglishAdapter(TribeEventsAdapter):
    """English-language comedy across many Berlin venues — the broad stand-up/improv feed."""

    slug = "comedy_in_english"
    category_prior = "comedy"
    base_url = "https://comedyinenglish.de"


class ComedyCafeAdapter(TribeEventsAdapter):
    """Comedy Café Berlin — improv/stand-up. Cloudflare-fronted, so use a browser UA."""

    slug = "comedy_cafe"
    category_prior = "comedy"
    base_url = "https://www.comedycafeberlin.com"
    house_venue = "Comedy Café Berlin"
    user_agent = BROWSER_UA


class PrinzipalKreuzbergAdapter(TribeEventsAdapter):
    """Prinzipal Kreuzberg — burlesque & cabaret variety (the live-performance gap)."""

    slug = "prinzipal"
    category_prior = "theatre_performance"
    base_url = "https://prinzipal-kreuzberg.com"
    house_venue = "Prinzipal Kreuzberg"
