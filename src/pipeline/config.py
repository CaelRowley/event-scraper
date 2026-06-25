"""City + source registry. A city is config; adding one means listing its sources here."""

from __future__ import annotations

from dataclasses import dataclass, field

DB_PATH = "data/pipeline.db"
HTTP_CACHE_PATH = "data/http_cache.db"
PUBLIC_DIR = "public"
WINDOW_DAYS = 14


@dataclass
class CityConfig:
    slug: str
    tz: str
    sources: list[str] = field(default_factory=list)


CITIES = {
    "berlin": CityConfig(
        slug="berlin",
        tz="Europe/Berlin",
        sources=[
            "kulturdaten",
            "berlin_de",
            "ra",
            "livegigs",
            "rausgegangen",
            "tip_berlin",
            "comedy_in_english",
            "comedy_cafe",
            "prinzipal",
            "eventbrite",
            "ticketmaster",
        ],
    ),
}


def build_adapters(city: CityConfig, fetcher, conn, *, mode: str = "full",
                   only: list[str] | None = None) -> list:
    """Instantiate the city's adapters. mode=delta → cheap-delta sources only."""
    from .adapters.berlin_de import BerlinDeAdapter
    from .adapters.eventbrite import EventbriteAdapter
    from .adapters.jsonld_sitemap import RausgegangenAdapter, TipBerlinAdapter
    from .adapters.kulturdaten import KulturdatenAdapter
    from .adapters.livegigs import LivegigsAdapter
    from .adapters.ra import ResidentAdvisorAdapter
    from .adapters.ticketmaster import TicketmasterAdapter
    from .adapters.tribe import (
        ComedyCafeAdapter,
        ComedyInEnglishAdapter,
        PrinzipalKreuzbergAdapter,
    )

    registry = {
        "kulturdaten": KulturdatenAdapter,
        "berlin_de": BerlinDeAdapter,
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
    adapters = []
    for slug in city.sources:
        if only and slug not in only:
            continue
        cls = registry[slug]
        if mode == "delta" and not cls.cheap_delta:
            continue
        adapters.append(cls(fetcher, conn))
    return adapters
