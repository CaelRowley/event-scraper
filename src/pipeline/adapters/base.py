"""Adapter contract. One module per source; adapters yield RawEvents and never talk
to the network except through the shared Fetcher, which owns rate limiting, the
robots.txt check and the User-Agent — an adapter can set none of the three aside.
Whether a source runs at all is decided in `sources.py`, not here."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from urllib.parse import urlsplit

from ..fetch import Fetcher, RateSpec
from ..models import RawEvent

log = logging.getLogger(__name__)


def ensure_scheme(url: str | None) -> str | None:
    """Source data sometimes carries bare hosts ('www.x.de') — unusable as feed links
    and invisible to the image backfill's http-prefix filter. Prefix https:// when the
    value looks like a hostname; return anything else (mailto:, garbage, None) as-is."""
    if not url:
        return url
    url = url.strip()
    if url.startswith(("http://", "https://")):
        return url
    if re.match(r"^[\w-]+(\.[\w-]+)+([/?#]|$)", url):
        return f"https://{url}"
    return url


# Hosts whose URLs are data records, not pages a person can open.
_RECORD_HOSTS = ("api-v2.kulturdaten.berlin",)


def has_external_link(url: str | None) -> bool:
    """Whether an event has somewhere to send a reader: an http(s) page that is
    not a raw data record.

    An event without one is not collected at all — not converted, not stored,
    not snapshotted. The feed's promise is that every listing links to its
    original, and an event we cannot link is one we have no business showing.
    """
    if not url:
        return False
    parts = urlsplit(url.strip())
    host = (parts.hostname or "").lower()
    if parts.scheme not in ("http", "https") or "." not in host:
        return False
    return not any(host == h or host.endswith("." + h) for h in _RECORD_HOSTS)


class SourceAdapter:
    slug: str = ""
    category_prior: str | None = None  # vertical sources: every event gets this category
    rate: RateSpec = RateSpec()
    cheap_delta: bool = False  # included in `--mode delta` (midday) runs

    def __init__(self, fetcher: Fetcher, conn):
        self.fetcher = fetcher
        self.conn = conn

    def fetch_events(self, window_days: int = 14, limit: int | None = None) -> Iterator[RawEvent]:
        """Yield RawEvents for the coming window. Must be resumable and polite."""
        raise NotImplementedError

    def map_category(self, raw: RawEvent) -> str | None:
        """Source-specific category-field mapping (tier 1b). None → later tiers decide."""
        return None

    # --- helpers ---------------------------------------------------------------

    def get(self, url: str, **kw):
        return self.fetcher.get(url, rate=self.rate, **kw)

    def post(self, url: str, **kw):
        return self.fetcher.post(url, rate=self.rate, **kw)
