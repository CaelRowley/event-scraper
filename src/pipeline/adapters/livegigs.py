"""livegigs.de — JSON-LD on listing + detail pages (verified).

The /berlin page embeds ~50 full Event objects spanning 2+ weeks. Per-day pages
(/berlin/YYYY-MM-DD) embed only one featured event each, but their HTML links out to
detail pages (…/slug/venue/YYYY-MM-DD) that carry full JSON-LD — those are harvested
ledger-tracked and budget-capped, so coverage converges over runs.
Content-Signal: search=yes, ai-train=no — we operate as a feed with attribution.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from datetime import date, timedelta
from urllib.parse import urljoin

from selectolax.parser import HTMLParser

from ..db import ledger_get, ledger_put
from ..extract.jsonld import events_from_html
from ..fetch import RateSpec
from ..models import RawEvent
from .base import SourceAdapter
from .jsonld_common import jsonld_to_raw

log = logging.getLogger(__name__)

BASE = "https://www.livegigs.de"
# detail pages: /{section}/{slug}/berlin-{venue}/{date}. The venue segment is
# city-prefixed — requiring "berlin-" skips the nationwide top-events block, whose
# bare /events/<id> links are mostly other cities (each costs a fetch to find out).
DETAIL_RE = re.compile(r"^/[^/]+/[^/]+/berlin-[^/]+/\d{4}-\d{2}-\d{2}$")
DETAIL_BUDGET = 150


class LivegigsAdapter(SourceAdapter):
    slug = "livegigs"
    rate = RateSpec(min_interval=2.0, jitter=(0.5, 1.5))
    cheap_delta = False

    def fetch_events(self, window_days: int = 14, limit: int | None = None) -> Iterator[RawEvent]:
        today = date.today()
        listing_pages = [f"{BASE}/berlin"] + [
            f"{BASE}/berlin/{(today + timedelta(days=i)).isoformat()}" for i in range(window_days)
        ]
        seen: set[str] = set()
        detail_links: list[str] = []
        yielded = 0

        for page_url in listing_pages:
            try:
                resp = self.get(page_url)
                if resp.status_code != 200:
                    continue
            except Exception as exc:  # noqa: BLE001
                log.warning("livegigs page %s failed: %s", page_url, exc)
                continue
            for node in events_from_html(resp.text):
                raw = jsonld_to_raw(node, source=self.slug, page_url=page_url)
                if raw is None or raw.source_event_id in seen or not self._is_berlin(raw):
                    continue
                seen.add(raw.source_event_id)
                yielded += 1
                yield raw
                if limit and yielded >= limit:
                    return
            detail_links.extend(self._harvest_links(resp.text, page_url))

        # detail pages not yet covered by listing JSON-LD, new-first via ledger
        budget = DETAIL_BUDGET
        for url in dict.fromkeys(detail_links):  # de-dupe, keep order
            if budget <= 0:
                break
            if url in seen or ledger_get(self.conn, self.slug, url) is not None:
                continue
            try:
                resp = self.get(url)
                ledger_put(self.conn, self.slug, url, status=resp.status_code)
                if resp.status_code != 200:
                    continue
            except Exception as exc:  # noqa: BLE001
                log.warning("livegigs detail %s failed: %s", url, exc)
                continue
            budget -= 1
            for node in events_from_html(resp.text):
                raw = jsonld_to_raw(node, source=self.slug, page_url=url)
                if raw is None or raw.source_event_id in seen or not self._is_berlin(raw):
                    continue
                seen.add(raw.source_event_id)
                yielded += 1
                yield raw
                if limit and yielded >= limit:
                    return

    @staticmethod
    def _is_berlin(raw: RawEvent) -> bool:
        """The /berlin listing also embeds a nationwide top-concerts block — filter it out."""
        return "berlin" in (raw.city or "").lower()

    @staticmethod
    def _harvest_links(html: str, page_url: str) -> list[str]:
        links = []
        for a in HTMLParser(html).css("a[href]"):
            href = a.attributes.get("href") or ""
            if DETAIL_RE.match(href):
                links.append(urljoin(page_url, href))
        return links
