"""Adapter contract. One module per source; adapters yield RawEvents and never talk
to the network except through the shared Fetcher (which owns rate limiting)."""

from __future__ import annotations

import logging
from collections.abc import Iterator

from ..fetch import DEFAULT_UA, Fetcher, RateSpec
from ..models import RawEvent

log = logging.getLogger(__name__)


class SourceAdapter:
    slug: str = ""
    category_prior: str | None = None  # vertical sources: every event gets this category
    rate: RateSpec = RateSpec()
    user_agent: str = DEFAULT_UA
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
        return self.fetcher.get(url, rate=self.rate, user_agent=self.user_agent, **kw)

    def post(self, url: str, **kw):
        return self.fetcher.post(url, rate=self.rate, user_agent=self.user_agent, **kw)
