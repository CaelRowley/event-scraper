"""Tiers 1–2 of the categorisation cascade.

Order: source prior > explicit source field (schema @type / TM segment / adapter table)
       > venue prior > keyword rules > LLM queue (tier 3) > fallback other.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

from ..models import Event
from .taxonomy import CATEGORIES, SCHEMA_TYPE_MAP

_KEYWORDS_PATH = Path(__file__).parent / "keywords.yaml"


@dataclass
class Classification:
    category: str
    confidence: float
    tier: str
    tags: list[str]


@lru_cache(maxsize=1)
def _compiled():
    data = yaml.safe_load(_KEYWORDS_PATH.read_text())
    families = {
        cat: re.compile("|".join(f"(?:{p})" for p in pats), re.IGNORECASE)
        for cat, pats in data["families"].items()
    }
    tags = {
        tag: re.compile("|".join(f"(?:{p})" for p in pats), re.IGNORECASE)
        for tag, pats in data.get("tags", {}).items()
    }
    return families, tags


def keyword_tags(text: str) -> list[str]:
    _, tag_res = _compiled()
    return [tag for tag, rx in tag_res.items() if rx.search(text)]


def classify(ev: Event, *, source_prior: str | None = None,
             mapped_category: str | None = None,
             venue_prior: str | None = None) -> Classification | None:
    """Tiers 1–2. Returns None when the event must go to the LLM queue."""
    text = f"{ev.title} {ev.description or ''}"
    tags = keyword_tags(text)

    # Tier 1a: vertical source prior (RA → club_nightlife, livegigs → live_music, …)
    if source_prior in CATEGORIES:
        return Classification(source_prior, 1.0, "source_prior", tags)

    # Tier 1b: adapter-mapped source category field (TM segment table, EB category, URL paths…)
    if mapped_category in CATEGORIES:
        return Classification(mapped_category, 0.95, "source_field", tags)

    # Tier 1c: schema.org @type (generic "Event" falls through — the big leak)
    schema_type = (ev.source_category_raw or "").rsplit("/", 1)[-1]
    if schema_type == "DanceEvent":
        cat = "club_nightlife" if venue_prior == "club_nightlife" else "theatre_performance"
        return Classification(cat, 0.85, "source_field", tags)
    if schema_type in SCHEMA_TYPE_MAP:
        return Classification(SCHEMA_TYPE_MAP[schema_type], 0.9, "source_field", tags)

    # Tier 2a: venue prior (Berghain ⇒ club, Philharmonie ⇒ live music, …)
    if venue_prior in CATEGORIES:
        return Classification(venue_prior, 0.8, "venue_prior", tags)

    # Tier 2b: keyword rules — fire only on an unambiguous single-family match
    families, _ = _compiled()
    hits = [cat for cat, rx in families.items() if rx.search(text)]
    if len(hits) == 1:
        return Classification(hits[0], 0.7, "keyword", tags)

    return None  # ambiguous (0 or 2+ families) → tier 3 LLM queue


# Tie-break preference for rules-only mode, most-specific first: when "Konzert +
# Aftershow-Party" matches two families with equal counts, prefer the earlier entry.
FAMILY_PRIORITY = [
    "comedy", "film_cinema", "markets_fairs", "talks_literature", "workshops_classes",
    "art_exhibitions", "theatre_performance", "family_kids", "sports_fitness",
    "food_drink", "live_music", "club_nightlife", "festivals", "community_causes",
]


def classify_best_effort(ev: Event, *, source_prior: str | None = None,
                         mapped_category: str | None = None,
                         venue_prior: str | None = None) -> Classification | None:
    """Rules-only mode: like classify(), but multi-family keyword matches are resolved
    by match count (priority order breaks ties) instead of being deferred to the LLM.
    Low confidence + its own tier, so the AI tier can upgrade these later."""
    cls = classify(ev, source_prior=source_prior, mapped_category=mapped_category,
                   venue_prior=venue_prior)
    if cls is not None:
        return cls
    text = f"{ev.title} {ev.description or ''}"
    families, _ = _compiled()
    scores = {cat: len(rx.findall(text)) for cat, rx in families.items()}
    scores = {cat: n for cat, n in scores.items() if n}
    if not scores:
        return None  # zero signal — stays "other" either way
    best = max(scores, key=lambda c: (scores[c], -FAMILY_PRIORITY.index(c)))
    return Classification(best, 0.5, "keyword_multi", keyword_tags(text))
