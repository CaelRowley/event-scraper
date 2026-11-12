"""berlin.de Simple-Search JSON endpoints — the only systematic markets/festivals source.

Verified 2026-06-10 under /sen/web/service/maerkte-feste/ (the /land/kalender/ paths 404):
  strassen-volksfeste …/index.php/index/all.json?q=   → 69 items
  weihnachtsmaerkte   …/index.php/index/all.json?q=   → 48 items
Both lists are Berlin Open Data datasets registered as `cc-by` (author:
Senatsverwaltung für Wirtschaft, Energie und Betriebe; read from the data registry
2026-11-12), so their opening-times and remarks text is published with that credit.
berlin.de's robots.txt carries terms for automated clients: identify uniquely in the
User-Agent and obey the Robots Exclusion Protocol. The fetcher does both.
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


_CONTACT_KEYS = frozenset({"mail", "email", "veranstalter", "telefon", "tel", "fax"})


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
            description_public=True,  # cc-by open data — published with its credit
            # The lists carry the operators' names and email addresses. We publish
            # neither, so we do not keep them either.
            payload={k: v for k, v in item.items() if k not in _CONTACT_KEYS},
        )

    def map_category(self, raw: RawEvent) -> str | None:
        spec = ENDPOINTS.get(raw.category_raw or "")
        return spec["category"] if spec else None
