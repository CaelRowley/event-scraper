"""Generic sitemap → detail-page → JSON-LD crawler, with per-run page budgets and
ledger-driven deltas (only NEW or lastmod-CHANGED urls are fetched).

Instances: rausgegangen.de (national aggregator — Berlin-filtered via addressLocality)
and tip-berlin.de (URL paths encode categories — free tier-1 input).
"""

from __future__ import annotations

import gzip
import logging
import re
import xml.etree.ElementTree as ET
from collections.abc import Iterator

from ..db import ledger_get, ledger_put
from ..extract.jsonld import events_from_html
from ..fetch import RateSpec
from ..models import RawEvent
from .base import SourceAdapter
from .jsonld_common import jsonld_to_raw

log = logging.getLogger(__name__)

_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"


def parse_sitemap(content: bytes) -> tuple[list[str], list[tuple[str, str | None]]]:
    """Returns (child sitemap urls, [(page url, lastmod)])."""
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return [], []
    children, pages = [], []
    if root.tag == f"{_NS}sitemapindex":
        for sm in root.iter(f"{_NS}sitemap"):
            loc = sm.findtext(f"{_NS}loc")
            if loc:
                children.append(loc.strip())
    else:
        for url in root.iter(f"{_NS}url"):
            loc = url.findtext(f"{_NS}loc")
            lastmod = url.findtext(f"{_NS}lastmod")
            if loc:
                pages.append((loc.strip(), lastmod.strip() if lastmod else None))
    return children, pages


class JsonLdSitemapAdapter(SourceAdapter):
    sitemaps: list[str] = []
    url_include: re.Pattern | None = None
    page_budget = 300  # new/changed detail pages per run
    city_filter: str | None = None  # addressLocality must contain this (national sitemaps)

    def fetch_events(self, window_days: int = 14, limit: int | None = None) -> Iterator[RawEvent]:
        targets = self._discover()
        log.info("%s: %d new/changed pages (budget %d)", self.slug, len(targets), self.page_budget)
        yielded = 0
        for url, lastmod in targets[: self.page_budget]:
            try:
                resp = self.get(url)
                if resp.status_code != 200:
                    ledger_put(self.conn, self.slug, url, lastmod=lastmod, status=resp.status_code)
                    continue
                nodes = events_from_html(resp.text)
            except PermissionError:
                raise  # robots disallows — stop the source, don't burn the budget
            except Exception as exc:  # noqa: BLE001
                log.warning("%s page %s failed: %s", self.slug, url, exc)
                continue
            ledger_put(self.conn, self.slug, url, lastmod=lastmod, status=resp.status_code)
            for node in nodes:
                raw = jsonld_to_raw(node, source=self.slug, page_url=url)
                if raw is None:
                    continue
                if self.city_filter and self.city_filter.lower() not in (raw.city or "").lower():
                    continue
                yielded += 1
                yield raw
                if limit and yielded >= limit:
                    return

    def _discover(self) -> list[tuple[str, str | None]]:
        """All event-page urls from the sitemaps that are new or whose lastmod moved."""
        queue = list(self.sitemaps)
        pages: list[tuple[str, str | None]] = []
        seen_sitemaps: set[str] = set()
        while queue:
            sm_url = queue.pop(0)
            if sm_url in seen_sitemaps:
                continue
            seen_sitemaps.add(sm_url)
            try:
                resp = self.get(sm_url)
                resp.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                log.warning("%s sitemap %s failed: %s", self.slug, sm_url, exc)
                continue
            children, found = parse_sitemap(resp.content)
            queue.extend(c for c in children if self._sitemap_relevant(c))
            pages.extend(found)

        targets = []
        for url, lastmod in pages:
            if self.url_include and not self.url_include.search(url):
                continue
            entry = ledger_get(self.conn, self.slug, url)
            if entry is None or (lastmod and entry["lastmod"] != lastmod):
                targets.append((url, lastmod))
        # newest lastmod first — the page budget should go to current events, not the archive
        targets.sort(key=lambda t: t[1] or "", reverse=True)
        return targets

    def _sitemap_relevant(self, child_url: str) -> bool:
        return "event" in child_url.lower()


class RausgegangenAdapter(JsonLdSitemapAdapter):
    slug = "rausgegangen"
    rate = RateSpec(min_interval=2.0, jitter=(0.5, 2.0))
    sitemaps = ["https://rausgegangen.de/sitemap-events.xml"]
    url_include = re.compile(r"/events?/")
    city_filter = "berlin"
    page_budget = 250


TIP_CATEGORY_PATHS = {
    "konzert": "live_music",
    "party": "club_nightlife",
    "buehne": "theatre_performance",
    "kabarett": "comedy",
    "comedy": "comedy",
    "ausstellung": "art_exhibitions",
    "kunst": "art_exhibitions",
    "film": "film_cinema",
    "kino": "film_cinema",
    "lesung": "talks_literature",
    "literatur": "talks_literature",
    "vortrag": "talks_literature",
    "workshop": "workshops_classes",
    "markt": "markets_fairs",
    "festival": "festivals",
    "sport": "sports_fitness",
    "kinder": "family_kids",
    "familie": "family_kids",
}


class TipBerlinAdapter(JsonLdSitemapAdapter):
    slug = "tip_berlin"
    rate = RateSpec(min_interval=2.5, jitter=(0.5, 2.0))
    sitemaps = ["https://www.tip-berlin.de/event-sitemap.xml"] + [
        f"https://www.tip-berlin.de/event-sitemap{i}.xml" for i in range(2, 6)
    ]
    url_include = re.compile(r"/event/")
    page_budget = 250

    def map_category(self, raw: RawEvent) -> str | None:
        path = raw.source_url.lower()
        for fragment, category in TIP_CATEGORY_PATHS.items():
            if f"/{fragment}" in path or f"-{fragment}" in path:
                return category
        return None
