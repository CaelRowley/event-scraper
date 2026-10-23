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

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx

from .categorise.taxonomy import LABELS
from .db import now_iso
from .fetch import Fetcher, RateSpec

log = logging.getLogger(__name__)

COMMONS_API = "https://commons.wikimedia.org/w/api.php"
IMAGE_MIME_ALLOWLIST = frozenset({"image/jpeg", "image/png", "image/webp"})

# Wikimedia's API etiquette asks for an identifiable UA; generic ones get
# throttled first.
USER_AGENT = "events-pipeline/0.1 (+https://github.com/CaelRowley/event-scraper)"

# A run resolves at most this many events. A cold database has thousands of
# imageless events and each costs up to a handful of API calls, so this is a time
# budget, not a correctness one; coverage builds up over successive runs.
#
# 250 took 2m33s of a 37-minute run against a 75-minute job limit, and every one
# of the 250 resolved — the ceiling was the budget, not Commons. At ~0.6s an
# event 1000 costs about ten minutes and clears a 3,500-event backlog in four
# runs rather than fourteen, which matters now that a missing photo is a drawn
# panel rather than a stock photo pretending otherwise.
LOOKUP_BUDGET = 1000

# How long before a miss is worth another try.
RETRY_DAYS = 14

# Lookups overlap this many at a time. Commons is one host, so the Fetcher's
# per-domain spacing still meters request *starts*; the gain is not waiting on
# each response before issuing the next.
MAX_WORKERS = 4
COMMONS_RATE = RateSpec(min_interval=0.25, jitter=(0.0, 0.1))

# (The hardcoded last-resort photo that used to live here is gone: it was never
# referenced, and the client now draws a panel instead of showing someone else's
# disco ball.)

# Broad terms per category, tried after the event's own title/venue. Without
# them every unmatched event collapses into one generic pool: a concert, a kids'
# workshop and an exhibition have nothing in common, so a shared pool both
# starves relevance and multiplies collisions.
#
# Several per category, and each returns up to SEARCH_LIMIT photos: with two
# queries × five results a category ran dry after ten photos and every event
# past that showed the same one.
CATEGORY_QUERIES = {
    "art_exhibitions": ["art gallery exhibition", "museum art", "contemporary art installation",
                        "painting exhibition visitors", "sculpture museum", "Berlin museum interior"],
    "club_nightlife": ["nightclub dance party", "DJ club night", "dance floor lights",
                       "techno party", "DJ mixer turntables", "Berlin nightlife"],
    "family_kids": ["children playing family", "kids activity", "children museum workshop",
                    "playground children", "puppet theatre children", "family outing park"],
    "community_causes": ["community gathering people", "volunteers group", "neighbourhood meeting",
                         "community garden", "public discussion panel", "Berlin library"],
    "workshops_classes": ["workshop class learning", "craft workshop", "pottery class",
                          "painting class", "sewing workshop", "language course classroom"],
    "theatre_performance": ["theatre stage performance", "actors stage", "ballet performance",
                            "opera stage", "dance performance stage", "theatre auditorium"],
    "live_music": ["concert live music", "band stage performance", "jazz concert",
                   "orchestra concert hall", "singer microphone concert", "guitarist live"],
    "festivals": ["festival crowd outdoor", "street festival", "open air festival",
                  "carnival parade", "festival stage night", "Berlin street fair"],
    "talks_literature": ["lecture audience", "book reading library", "author reading",
                         "panel discussion audience", "bookshop interior", "library reading room"],
    "film_cinema": ["cinema movie theatre", "film screening", "cinema auditorium seats",
                    "open air cinema", "film projector", "Berlin cinema"],
    "comedy": ["comedy stage microphone", "stand-up comedy", "comedian performing",
               "audience laughing", "cabaret stage", "improv theatre"],
    "sports_fitness": ["sports fitness people", "gym exercise", "running race city",
                       "yoga class", "football match stadium", "cycling race"],
    "markets_fairs": ["market stalls fair", "street market", "flea market",
                      "farmers market vegetables", "christmas market", "Berlin market hall"],
    "food_drink": ["restaurant food dining", "cafe drinks", "street food stall",
                   "wine tasting", "craft beer bar", "food market"],
    "other": ["Berlin street life", "people city event", "Berlin architecture",
              "crowd gathering", "Berlin Kreuzberg", "Berlin Spree river"],
}

# Results per Commons query (its API allows up to 50).
SEARCH_LIMIT = 40


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


class _CommonsClient:
    """The pipeline `Fetcher` behind the `client.get(url, params=, timeout=)` shape
    the lookups use — so they share its RFC-9111 cache (a repeated query costs
    no request) and its retries, without every call site spelling the options.

    `check_robots=False` on purpose: Commons' robots.txt disallows `/w/` for
    crawlers, but this is the API it publishes for exactly this use. We are
    identified to it by User-Agent as its etiquette asks.
    """

    def __init__(self, fetcher: Fetcher):
        self._fetcher = fetcher

    def get(self, url: str, **kwargs) -> httpx.Response:
        return self._fetcher.get(url, rate=COMMONS_RATE, user_agent=USER_AGENT,
                                 check_robots=False, **kwargs)


def search_many(client, query: str) -> list[tuple[str, dict | None]]:
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
        "gsrlimit": str(SEARCH_LIMIT),
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


def candidates(client, event, claimed: set[str]) -> list[tuple[str, dict | None]]:
    """Ranked photos for one event, stopping at the first query with an unclaimed hit.

    The whole ranked list comes back, not just the winner: lookups run
    concurrently, so `claimed` is a snapshot, and the caller re-picks against
    the live set once results are in.
    """
    out: list[tuple[str, dict | None]] = []
    for query in query_candidates(event):
        hits = search_many(client, query)
        out.extend(hits)
        if any(url not in claimed for url, _ in hits):
            break
    return out


def pick(ranked: list[tuple[str, dict | None]], claimed: set[str],
         key: str = "") -> tuple[str, dict | None] | None:
    """First unclaimed candidate; failing that a repeat — still better than a blank card.

    The repeat is spread across the candidates by `key` (the event id) instead of
    always the top hit, so an exhausted pool shows many photos, not one.
    """
    for url, attribution in ranked:
        if url not in claimed:
            return url, attribution
    if not ranked:
        return None
    return ranked[int(hashlib.sha1(key.encode()).hexdigest()[:8], 16) % len(ranked)]


def resolve(client, event, claimed: set[str]) -> tuple[str, dict | None] | None:
    """Find an unclaimed photo for one event, or None if Commons had nothing.

    Returns None rather than the emergency fallback so the caller can decline to
    store a miss — see the module docstring.
    """
    return pick(candidates(client, event, claimed), claimed)


def _due(last_attempt: str | None, now: datetime) -> bool:
    if not last_attempt:
        return True
    try:
        seen = datetime.fromisoformat(last_attempt.replace("Z", "+00:00"))
    except ValueError:
        return True
    return now - seen > timedelta(days=RETRY_DAYS)


# Bump to re-resolve every event that shares its stock photo with another, once.
# 2: the pool grew from 5 results per query to SEARCH_LIMIT.
POOL_VERSION = 2


def _release_duplicates(conn) -> int:
    """Clear stock photos assigned from an older, smaller pool where they collide.

    A stored photo is never retried, so without this the events that got a repeat
    under the old pool would keep it forever. One event per photo keeps it; the
    rest go back into the queue. Runs once per POOL_VERSION.
    """
    row = conn.execute("SELECT value FROM kv WHERE key='stock_pool_version'").fetchone()
    if row and int(row["value"]) >= POOL_VERSION:
        return 0
    cur = conn.execute(
        """UPDATE events SET stock_image_url=NULL, stock_attribution=NULL, stock_attempted_at=NULL
            WHERE stock_image_url IS NOT NULL
              AND rowid NOT IN (SELECT MIN(rowid) FROM events
                                 WHERE stock_image_url IS NOT NULL GROUP BY stock_image_url)"""
    )
    conn.execute(
        "INSERT INTO kv(key, value) VALUES('stock_pool_version', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (str(POOL_VERSION),),
    )
    conn.commit()
    return cur.rowcount


def backfill_stock_photos(conn, city: str, budget: int = LOOKUP_BUDGET,
                          fetcher: Fetcher | None = None) -> dict:
    """Resolve Commons photos for imageless events in the feed window.

    Pass the run's `Fetcher` to share its HTTP cache and rate limiting; without
    one a plain client is used (tests, ad-hoc runs).
    """
    now = datetime.now(timezone.utc)
    released = _release_duplicates(conn)

    today = now.date().isoformat()

    # Only photos actually on show count as taken: once an event is over, gets its
    # own image or drops out, its stand-in is free for another event.
    claimed = {
        r["stock_image_url"]
        for r in conn.execute(
            """SELECT e.stock_image_url FROM events e
                WHERE e.stock_image_url IS NOT NULL AND e.link_dead_at IS NULL
                  AND (e.image_url IS NULL OR e.image_url = '')
                  AND EXISTS (SELECT 1 FROM occurrences o
                               WHERE o.event_id = e.id AND o.nightlife_date >= ?)""",
            (today,),
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
              AND EXISTS (SELECT 1 FROM occurrences o
                           WHERE o.event_id = e.id AND o.nightlife_date >= ?)
            ORDER BY e.first_seen_at DESC""",
        (city, today),
    ).fetchall()

    due = [r for r in rows if _due(r["stock_attempted_at"], now)][:budget]
    if not due:
        return {"considered": len(rows), "attempted": 0, "resolved": 0, "released": released}

    resolved = 0
    plain = None
    if fetcher is None:
        plain = httpx.Client(headers={"User-Agent": USER_AGENT}, follow_redirects=True)
    client = plain if plain is not None else _CommonsClient(fetcher)
    try:
        # HTTP fans out; the SQLite connection stays on this thread. `claimed` is
        # read by the workers as a snapshot and re-checked here in order, so two
        # concurrent lookups landing on the same photo still end up distinct.
        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="commons") as pool:
            ranked = list(pool.map(lambda row: candidates(client, row, claimed), due))
        for row, options in zip(due, ranked):
            hit = pick(options, claimed, key=row["id"])
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
    finally:
        if plain is not None:
            plain.close()
    conn.commit()

    log.info("stock photos: %d considered, %d attempted, %d resolved, %d released",
             len(rows), len(due), resolved, released)
    return {"considered": len(rows), "attempted": len(due), "resolved": resolved,
            "released": released}
