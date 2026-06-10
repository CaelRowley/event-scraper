"""livegigs.de — JSON-LD embedded directly in listing pages (cheapest pattern, verified).

The /berlin page carries ~50 events spanning 2+ weeks; per-day pages /berlin/YYYY-MM-DD
fill in the rest. Events use real schema.org subtypes (MusicEvent, ComedyEvent, …) with
full addresses and structured offers — no detail-page fetches needed.
Content-Signal: search=yes, ai-train=no — we operate as a feed with attribution.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, timedelta

from ..extract.jsonld import events_from_html
from ..fetch import RateSpec
from ..models import RawEvent
from .base import SourceAdapter
from .jsonld_common import jsonld_to_raw

log = logging.getLogger(__name__)

BASE = "https://www.livegigs.de"


class LivegigsAdapter(SourceAdapter):
    slug = "livegigs"
    rate = RateSpec(min_interval=2.0, jitter=(0.5, 1.5))
    cheap_delta = False

    def fetch_events(self, window_days: int = 14, limit: int | None = None) -> Iterator[RawEvent]:
        today = date.today()
        pages = [f"{BASE}/berlin"] + [
            f"{BASE}/berlin/{(today + timedelta(days=i)).isoformat()}" for i in range(window_days)
        ]
        seen: set[str] = set()
        yielded = 0
        for page_url in pages:
            try:
                resp = self.get(page_url)
                if resp.status_code != 200:
                    continue
                nodes = events_from_html(resp.text)
            except Exception as exc:  # noqa: BLE001
                log.warning("livegigs page %s failed: %s", page_url, exc)
                continue
            for node in nodes:
                raw = jsonld_to_raw(node, source=self.slug, page_url=page_url)
                if raw is None or raw.source_event_id in seen:
                    continue
                seen.add(raw.source_event_id)
                yielded += 1
                yield raw
                if limit and yielded >= limit:
                    return
