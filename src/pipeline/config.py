"""City + source registry. A city is config; adding one means listing its sources here."""

from __future__ import annotations

from dataclasses import dataclass, field

DB_PATH = "data/pipeline.db"
HTTP_CACHE_PATH = "data/http_cache.db"
PUBLIC_DIR = "public"
# Scrape one day further ahead than the feed publishes. Without the gap, the last
# day of the feed is always the day first discovered on that very run — the
# thinnest, least-verified day in it, and the one most likely to gain events
# tomorrow. The buffer day lands in the database, gets a second pass, and is
# published the next morning as a settled day.
SCRAPE_WINDOW_DAYS = 31  # how far ahead adapters are asked to look
EXPORT_WINDOW_DAYS = 30  # how far ahead the feed publishes


@dataclass
class CityConfig:
    slug: str
    tz: str
    sources: list[str] = field(default_factory=list)


CITIES = {
    "berlin": CityConfig(
        slug="berlin",
        tz="Europe/Berlin",
        # Every source the city has an adapter for. Which of them actually run
        # is not decided here: `sources.py` holds each one's verdict, and only a
        # permitted source is selected (see select_adapters). Today that is
        # kulturdaten, berlin_de, vhs, prinzipal and ticketmaster; the rest wait for
        # their site's written permission and PIPELINE_PERMITTED_SOURCES.
        sources=[
            "kulturdaten",
            "berlin_de",
            "vhs",
            "prinzipal",
            "ticketmaster",
            "ra",
            "tip_berlin",
            "livegigs",
            "comedy_in_english",
            "comedy_cafe",
            "rausgegangen",
            "eventbrite",
        ],
    ),
}


def select_adapters(city: CityConfig, *, mode: str = "full",
                    only: list[str] | None = None) -> list[tuple[str, type]]:
    """(slug, adapter_class) pairs. Construction is deferred so each runner worker
    can instantiate on its own thread-local connection (adapter __init__ runs DDL).
    mode=delta → cheap-delta sources only.

    A source that `sources.py` does not permit is never selected — naming it
    with `--source` does not override that; only its policy or
    PIPELINE_PERMITTED_SOURCES does."""
    from . import sources as policy
    from .adapters.berlin_de import BerlinDeAdapter
    from .adapters.eventbrite import EventbriteAdapter
    from .adapters.jsonld_sitemap import RausgegangenAdapter, TipBerlinAdapter
    from .adapters.kulturdaten import KulturdatenAdapter
    from .adapters.livegigs import LivegigsAdapter
    from .adapters.ra import ResidentAdvisorAdapter
    from .adapters.ticketmaster import TicketmasterAdapter
    from .adapters.vhs import VhsAdapter
    from .adapters.tribe import (
        ComedyCafeAdapter,
        ComedyInEnglishAdapter,
        PrinzipalKreuzbergAdapter,
    )

    registry = {
        "kulturdaten": KulturdatenAdapter,
        "berlin_de": BerlinDeAdapter,
        "vhs": VhsAdapter,
        "ra": ResidentAdvisorAdapter,
        "livegigs": LivegigsAdapter,
        "rausgegangen": RausgegangenAdapter,
        "tip_berlin": TipBerlinAdapter,
        "comedy_in_english": ComedyInEnglishAdapter,
        "comedy_cafe": ComedyCafeAdapter,
        "prinzipal": PrinzipalKreuzbergAdapter,
        "eventbrite": EventbriteAdapter,
        "ticketmaster": TicketmasterAdapter,
    }
    selected = []
    for slug in city.sources:
        if only and slug not in only:
            continue
        if not policy.is_permitted(slug):
            continue
        cls = registry[slug]
        if mode == "delta" and not cls.cheap_delta:
            continue
        selected.append((slug, cls))
    return selected


def build_adapters(city: CityConfig, fetcher, conn, *, mode: str = "full",
                   only: list[str] | None = None) -> list:
    """Instantiate the city's adapters on the given connection."""
    return [cls(fetcher, conn) for _, cls in select_adapters(city, mode=mode, only=only)]
