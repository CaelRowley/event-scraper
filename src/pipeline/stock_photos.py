"""Wikimedia Commons stand-in photos for events with no image of their own.

An event card with no picture is a dead card, and roughly a third of what we
collect arrives without one. Commons is the only free image source that needs no
account or key, so a themed photo — the venue, failing that the category — fills
the gap.

This used to run inside Gobento, once per web request, against a cache table it
scanned in full every time. That was the wrong place twice over: the answer is
identical for every visitor, and it was being recomputed per visitor. It belongs
here, resolved once per event and published with the feed.

Two rules carried over from that implementation, both learned the hard way:

  * **Never persist the emergency fallback.** A throttled lookup returns the one
    hardcoded photo; storing that froze whole batches of events onto the same
    disco ball, permanently, because a stored value is never retried. A miss is
    left unstored so the next run tries again, and the event shows its
    deterministic placeholder in the meantime.
  * **No two events share a photo.** A query like "art gallery exhibition"
    matches the same top result for every art event, so candidates already
    claimed are skipped in favour of the next-ranked one.

Licensing: Commons files are free but mostly CC-BY(-SA), which does *not* waive
attribution. The author/licence is recorded alongside the URL and published in
the feed so the UI can credit it.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import httpx

from .categorise.taxonomy import LABELS
from .db import now_iso

log = logging.getLogger(__name__)

COMMONS_API = "https://commons.wikimedia.org/w/api.php"
IMAGE_MIME_ALLOWLIST = frozenset({"image/jpeg", "image/png", "image/webp"})

# Wikimedia's API etiquette asks for an identifiable UA; generic ones get
# throttled first.
USER_AGENT = "events-pipeline/0.1 (+https://github.com/CaelRowley/event-scraper)"

# A run resolves at most this many events. A cold database has thousands of
# imageless events and each costs up to a handful of API calls, which would blow
# the job's time budget; coverage builds up over successive runs instead.
LOOKUP_BUDGET = 250

# How long before a miss is worth another try.
RETRY_DAYS = 14

# Last resort when Commons returns nothing at all. Deliberately NOT persisted —
# see the module docstring.
FALLBACK_PHOTO = "https://upload.wikimedia.org/wikipedia/commons/2/29/Disco_ball4.jpg"

# Broad terms per category, tried after the event's own title/venue. Without
# them every unmatched event collapses into one generic pool: a concert, a kids'
# workshop and an exhibition have nothing in common, so a shared pool both
# starves relevance and multiplies collisions.
CATEGORY_QUERIES = {
    "art_exhibitions": ["art gallery exhibition", "museum art"],
    "club_nightlife": ["nightclub dance party", "DJ club night"],
    "family_kids": ["children playing family", "kids activity"],
    "community_causes": ["community gathering people", "volunteers group"],
    "workshops_classes": ["workshop class learning", "craft workshop"],
    "theatre_performance": ["theatre stage performance", "actors stage"],
    "live_music": ["concert live music", "band stage performance"],
    "festivals": ["festival crowd outdoor", "street festival"],
    "talks_literature": ["lecture audience", "book reading library"],
    "film_cinema": ["cinema movie theatre", "film screening"],
    "comedy": ["comedy stage microphone", "stand-up comedy"],
    "sports_fitness": ["sports fitness people", "gym exercise"],
    "markets_fairs": ["market stalls fair", "street market"],
    "food_drink": ["restaurant food dining", "cafe drinks"],
}


def query_candidates(event) -> list[str]:
    """Search terms, most specific first — the first query that hits wins."""
    # `category_label` is derived at export time, not stored, so build it here
    # from the same table rather than expecting it on the row.
    label = LABELS.get(event["category"])
    ordered = [
        " ".join(x for x in (label, event["title"]) if x),
        label,
        event["venue_name"],
        *CATEGORY_QUERIES.get(event["category"], []),
        event["category"],
        "party celebration event",
    ]
    seen, out = set(), []
    for q in ordered:
        if q and q not in seen:
            seen.add(q)
            out.append(q)
    return out


def _strip_tracking(url: str) -> str:
    """Drop Commons' analytics querystring from a thumb URL.

    Its API appends `?utm_source=…` to every `thumburl`; the resize is already in
    the path. Left on, privacy-focused browsers and ad-blockers read it as a
    tracking pixel and silently drop the request — which looks like "the photo is
    just blank", with nothing anywhere to explain why.
    """
    return url.split("?", 1)[0]


def _attribution(info: dict) -> dict | None:
    """Author + licence from a file's extmetadata, for the CC-BY credit line."""
    meta = info.get("extmetadata") or {}

    def field(key):
        value = (meta.get(key) or {}).get("value")
        return value.strip() if isinstance(value, str) and value.strip() else None

    artist, licence = field("Artist"), field("LicenseShortName")
    if not (artist or licence):
        return None
    return {
        "author": artist,
        "license": licence,
        "license_url": field("LicenseUrl"),
        "source": info.get("descriptionurl"),
    }


def search_many(client: httpx.Client, query: str) -> list[tuple[str, dict | None]]:
    """Every allowed-mime candidate for one query, ranked by relevance.

    The full ranked list, not just the top hit: one query commonly matches many
    events, so the caller needs alternatives to avoid handing them all the same
    photo.
    """
    params = {
        "action": "query",
        "generator": "search",
        # filetype:bitmap keeps out the SVG diagrams, PDFs and logos Commons
        # otherwise happily returns.
        "gsrsearch": f"{query} filetype:bitmap",
        "gsrnamespace": "6",  # File:
        "gsrlimit": "5",
        "prop": "imageinfo",
        "iiprop": "url|mime|extmetadata",
        "iiurlwidth": "800",  # gives us a resized `thumburl`
        "format": "json",
    }
    try:
        resp = client.get(COMMONS_API, params=params, timeout=8.0)
    except httpx.HTTPError as err:
        log.debug("commons query %r failed: %s", query, err)
        return []
    if resp.status_code != 200:
        return []

    pages = (resp.json().get("query") or {}).get("pages")
    if not pages:
        return []

    # `pages` is keyed by page id, so its order is not relevance order — each
    # page's own `index` is. Sort by it or we return the least relevant of five.
    out = []
    for page in sorted(pages.values(), key=lambda p: p.get("index", 99)):
        info = (page.get("imageinfo") or [{}])[0]
        if info.get("mime") not in IMAGE_MIME_ALLOWLIST:
            continue
        url = info.get("thumburl") or info.get("url")
        if url:
            out.append((_strip_tracking(url), _attribution(info)))
    return out


def resolve(client: httpx.Client, event, claimed: set[str]) -> tuple[str, dict | None] | None:
    """Find an unclaimed photo for one event, or None if Commons had nothing.

    Returns None rather than the emergency fallback so the caller can decline to
    store a miss — see the module docstring.
    """
    best = None
    for query in query_candidates(event):
        for url, attribution in search_many(client, query):
            if best is None:
                best = (url, attribution)
            if url not in claimed:
                return url, attribution
    # Everything relevant is taken: a repeat still beats a blank card.
    return best


def _due(last_attempt: str | None, now: datetime) -> bool:
    if not last_attempt:
        return True
    try:
        seen = datetime.fromisoformat(last_attempt.replace("Z", "+00:00"))
    except ValueError:
        return True
    return now - seen > timedelta(days=RETRY_DAYS)


def backfill_stock_photos(conn, city: str, budget: int = LOOKUP_BUDGET) -> dict:
    """Resolve Commons photos for imageless events in the feed window."""
    now = datetime.now(timezone.utc)

    claimed = {
        r["stock_image_url"]
        for r in conn.execute(
            "SELECT stock_image_url FROM events WHERE stock_image_url IS NOT NULL"
        )
    }

    rows = conn.execute(
        """SELECT e.id, e.title, e.category, e.venue_name,
                  e.stock_image_url, e.stock_attempted_at
             FROM events e
            WHERE e.city = ?
              AND (e.image_url IS NULL OR e.image_url = '')
              AND e.stock_image_url IS NULL
              AND e.link_dead_at IS NULL
            ORDER BY e.first_seen_at DESC""",
        (city,),
    ).fetchall()

    due = [r for r in rows if _due(r["stock_attempted_at"], now)][:budget]
    if not due:
        return {"considered": len(rows), "attempted": 0, "resolved": 0}

    resolved = 0
    with httpx.Client(headers={"User-Agent": USER_AGENT}, follow_redirects=True) as client:
        for row in due:
            hit = resolve(client, row, claimed)
            # Record the attempt either way — that is what stops a hopeless event
            # being retried on every single run.
            conn.execute(
                "UPDATE events SET stock_attempted_at=? WHERE id=?", (now_iso(), row["id"])
            )
            if not hit:
                continue
            url, attribution = hit
            conn.execute(
                "UPDATE events SET stock_image_url=?, stock_attribution=? WHERE id=?",
                (url, json.dumps(attribution, ensure_ascii=False) if attribution else None, row["id"]),
            )
            claimed.add(url)
            resolved += 1
    conn.commit()

    log.info("stock photos: %d considered, %d attempted, %d resolved",
             len(rows), len(due), resolved)
    return {"considered": len(rows), "attempted": len(due), "resolved": resolved}
