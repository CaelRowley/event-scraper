"""Category-matched stock placeholder images for events with no real image.

Deterministic per event: the lock/seed derives from the event id, so every card gets
a *different* image (hash-distributed across ~100k variants) that is *stable* across
reloads and re-exports. Providers need no API key:

- loremflickr: CC images matched to a per-category keyword ("concert" → bands).
  Caveat: images are CC-licensed via Flickr and hotlinked without per-image
  attribution — fine for a demo, review before a public launch (or switch provider).
- picsum: deterministic generic photos (no keyword matching, permissively licensed).
- none: disable (frontend falls back to category icons).

The feed marks these as `placeholder_url`, never `image_url` — UIs can tell stock
from real and label it.
"""

from __future__ import annotations

import hashlib

KEYWORDS = {
    "live_music": "concert,band",
    "club_nightlife": "nightclub,dj",
    "theatre_performance": "theatre,stage",
    "comedy": "standup,microphone",
    "art_exhibitions": "art,gallery",
    "film_cinema": "cinema",
    "talks_literature": "books,lecture",
    "workshops_classes": "workshop",
    "markets_fairs": "fleamarket,market",
    "food_drink": "streetfood",
    "festivals": "festival,crowd",
    "sports_fitness": "sports",
    "family_kids": "carousel,playground",
    "community_causes": "community,people",
    "other": "berlin,city",
}

WIDTH, HEIGHT = 640, 360


def placeholder_url(event_id: str, category: str, provider: str = "loremflickr") -> str | None:
    if provider == "none":
        return None
    # stable, well-distributed variant id per event — no visible duplicates, no reshuffling
    lock = int(hashlib.sha1(event_id.encode()).hexdigest()[:8], 16) % 99991
    if provider == "picsum":
        return f"https://picsum.photos/seed/{event_id}/{WIDTH}/{HEIGHT}"
    keyword = KEYWORDS.get(category, KEYWORDS["other"])
    return f"https://loremflickr.com/{WIDTH}/{HEIGHT}/{keyword}?lock={lock}"
