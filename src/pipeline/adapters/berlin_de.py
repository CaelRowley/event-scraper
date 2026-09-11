"""berlin.de Simple-Search JSON endpoints — the only systematic markets/festivals source.

Verified 2026-06-10 under /sen/web/service/maerkte-feste/ (the /land/kalender/ paths 404):
  strassen-volksfeste …/index.php/index/all.json?q=   → 69 items
  weihnachtsmaerkte   …/index.php/index/all.json?q=   → 48 items
Open data (CC-BY/DL-DE). berlin.de robots.txt requires a uniquely identifying UA —
the fetcher's default UA carries project URL + contact email.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

from ..fetch import RateSpec
from ..models import RawEvent
from .base import SourceAdapter, ensure_scheme

log = logging.getLogger(__name__)

BASE = "https://www.berlin.de/sen/web/service/maerkte-feste"

# endpoint slug → (page URL, category, field names that differ between indexes)
ENDPOINTS = {
    "strassen-volksfeste": {
        "category": "festivals",
        "title": "bezeichnung",
        "postal": "plz",
    },
    "weihnachtsmaerkte": {
        "category": "markets_fairs",
        "title": "name",
        "postal": "plz_ort",
    },
}


class BerlinDeAdapter(SourceAdapter):
    slug = "berlin_de"
    rate = RateSpec(min_interval=2.0, jitter=(0.5, 1.5))
    cheap_delta = True

    def fetch_events(self, window_days: int = 14, limit: int | None = None) -> Iterator[RawEvent]:
        yielded = 0
        for endpoint, spec in ENDPOINTS.items():
            url = f"{BASE}/{endpoint}/index.php/index/all.json?q="
            try:
                resp = self.get(url)
                resp.raise_for_status()
                items = resp.json().get("index", [])
            except Exception as exc:  # noqa: BLE001 — one endpoint must not kill the other
                log.warning("berlin.de %s failed: %s", endpoint, exc)
                continue
            for item in items:
                raw = self._to_raw(item, endpoint, spec)
                if raw:
                    yielded += 1
                    yield raw
                    if limit and yielded >= limit:
                        return

    def _to_raw(self, item: dict, endpoint: str, spec: dict) -> RawEvent | None:
        title = (item.get(spec["title"]) or "").strip()
        start = (item.get("von") or "").strip()  # dd.mm.yyyy
        source_url = ensure_scheme((item.get("www") or "").strip())
        if not title or not start or not source_url:
            return None
        end = (item.get("bis") or "").strip() or None
        zeit = (item.get("zeit") or "").strip()

        return RawEvent(
            source=self.slug,
            source_event_id=f"{endpoint}-{item.get('id')}",
            source_url=source_url,
            title=title,
            start=start,
            end=end,
            date_only=True,
            venue_name=(item.get("strasse") or "").strip() or None,
            street=(item.get("strasse") or "").strip() or None,
            postal_code=(item.get(spec["postal"]) or "").strip()[:5] or None,
            city="Berlin",
            category_raw=endpoint,
            description=" ".join(
                filter(None, [zeit, (item.get("bemerkungen") or "").strip()])
            ) or None,
            payload=dict(item),
        )

    def map_category(self, raw: RawEvent) -> str | None:
        spec = ENDPOINTS.get(raw.category_raw or "")
        return spec["category"] if spec else None
