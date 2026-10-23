"""Image backfill: for feed events with no image, fetch the source page and pull
JSON-LD image → og:image → twitter:image → link rel=image_src.

Polite and bounded: robots-checked, per-domain throttled via the shared Fetcher,
budget-capped per run, and every attempt is ledger-recorded (source slug "imgscan").
Misses are retried after a cooling-off period so a temporary block or a page that
gets its artwork later is not permanently stuck without an image.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlsplit

from selectolax.lexbor import LexborHTMLParser as HTMLParser

from .db import ledger_get, ledger_put
from .extract.jsonld import events_from_html
from .fetch import HostBlockedError, RateSpec
from .probe import fan_out, select

log = logging.getLogger(__name__)

# Pages scanned per run, and the most any one host may take of that. Hosts run in
# parallel (probe.py), so a run costs about PER_HOST × that host's spacing rather
# than the sum. berlin.de's calendar — where kulturdaten events now link — is the
# big one: ~900 pages, so the cap is what keeps it from setting the phase length.
SCAN_BUDGET = 300
PER_HOST = 100
SCAN_RETRY_DAYS = 7
RATE = RateSpec(min_interval=1.0, jitter=(0.3, 1.0))

# pages that are not scannable HTML or are known to block us
SKIP_HOSTS = ("ra.co", "api-v2.kulturdaten.berlin", "app.ticketmaster.com")
_BAD_HINTS = ("logo", "favicon", "placeholder", "default-share", ".svg", "qrcode",
              "/icon/", "/icons/", "piktogramm")


def find_image(html: str, base_url: str) -> str | None:
    """Best page-level image, JSON-LD first, then social meta tags."""
    for node in events_from_html(html):
        img = node.get("image")
        if isinstance(img, list) and img:
            img = img[0]
        if isinstance(img, dict):
            img = img.get("url") or img.get("contentUrl")
        if img:
            url = urljoin(base_url, str(img))
            if _plausible(url):
                return url

    tree = HTMLParser(html)
    for selector, attr in (
        ('meta[property="og:image"]', "content"),
        ('meta[property="og:image:url"]', "content"),
        ('meta[name="twitter:image"]', "content"),
        ('link[rel="image_src"]', "href"),
    ):
        for node in tree.css(selector):
            value = (node.attributes.get(attr) or "").strip()
            if value:
                url = urljoin(base_url, value)
                if _plausible(url):
                    return url

    # last tier: content images — many sites (tip-berlin, livegigs) put only their site
    # logo in og:image but a real hero image in the article body
    return _content_image(tree, base_url)


def _int(value) -> int | None:
    try:
        return int(str(value).rstrip("px"))
    except (TypeError, ValueError):
        return None


def _content_image(tree: HTMLParser, base_url: str) -> str | None:
    """Largest plausible <img> in the most article-like scope. Conservative:
    size attributes below 200×150 are rejected, logo-ish names already filtered."""
    # [role=main] last: berlin.de's calendar (kulturdaten's event pages) has no
    # article/main/figure element, only an ARIA landmark around the photo.
    for scope in ("article img", "main img", "figure img", '[role="main"] img'):
        candidates: list[tuple[int, int, str]] = []
        for img in tree.css(scope):
            src = (img.attributes.get("src") or img.attributes.get("data-src") or "").strip()
            if not src:
                continue
            url = urljoin(base_url, src)
            if not _plausible(url):
                continue
            w, h = _int(img.attributes.get("width")), _int(img.attributes.get("height"))
            if (w is not None and w < 200) or (h is not None and h < 150):
                continue
            candidates.append((-(w or 0) * (h or 0), len(candidates), url))
        if candidates:
            return min(candidates)[2]  # biggest declared area, document order on ties
    return None


def _plausible(url: str) -> bool:
    if not url.startswith(("http://", "https://")):
        return False
    return not any(hint in url.lower() for hint in _BAD_HINTS)


def _propagate_shared_urls(conn) -> int:
    """Recurring events share a source_url (e.g. one exhibition listed per date) —
    copy an already-known image to every imageless sibling before scanning anything."""
    cur = conn.execute(
        """UPDATE events SET image_url = (
               SELECT e2.image_url FROM events e2
               WHERE e2.source_url = events.source_url
                 AND e2.image_url IS NOT NULL AND e2.image_url != '' LIMIT 1)
           WHERE (image_url IS NULL OR image_url = '')
             AND EXISTS (SELECT 1 FROM events e3
                         WHERE e3.source_url = events.source_url
                           AND e3.image_url IS NOT NULL AND e3.image_url != '')"""
    )
    return cur.rowcount


def scan_is_due(ledger_row, now: datetime | None = None) -> bool:
    """Whether a source page has never been scanned or its retry TTL expired."""
    if ledger_row is None:
        return True
    last_seen = ledger_row["last_seen_at"]
    if not last_seen:
        return True
    try:
        checked_at = datetime.fromisoformat(str(last_seen).replace("Z", "+00:00"))
    except ValueError:
        return True
    current = now or datetime.now(timezone.utc)
    return checked_at <= current - timedelta(days=SCAN_RETRY_DAYS)


def _scan(fetcher, url: str) -> tuple[int | None, str | None]:
    """(ledger status, image) for one page. Runs on a probe worker — no SQLite.

    Status is the HTTP code, -1 for a robots refusal, -2 for a failed fetch, and
    None when the host's circuit breaker tripped (nothing to record).
    """
    try:
        if not fetcher.allowed(url):
            return -1, None
        resp = fetcher.get(url, rate=RATE)
        if resp.status_code != 200 or "html" not in resp.headers.get("content-type", "html"):
            return resp.status_code, None
        return resp.status_code, find_image(resp.text, str(resp.url))
    except HostBlockedError:
        return None, None
    except Exception as exc:  # noqa: BLE001 — a dead organiser site must not kill the run
        log.debug("imgscan %s failed: %s", url, exc)
        return -2, None


def backfill_images(conn, fetcher, city: str, budget: int = SCAN_BUDGET) -> dict:
    """Scan source pages of imageless feed-window events, soonest first.
    A found image is applied to every event sharing the URL; misses retry weekly."""
    propagated = _propagate_shared_urls(conn)
    conn.commit()
    today = datetime.now(timezone.utc).date().isoformat()
    rows = conn.execute(
        """SELECT e.source_url, MIN(o.starts_at_utc) AS next_start
           FROM events e JOIN occurrences o ON o.event_id = e.id
           WHERE e.city=? AND e.canonical_id=e.id AND o.nightlife_date >= ?
             AND (e.image_url IS NULL OR e.image_url='')
             AND e.source_url LIKE 'http%'
           GROUP BY e.source_url ORDER BY next_start""",
        (city, today),
    ).fetchall()

    stats = {"candidates": len(rows), "scanned": 0, "found": 0,
             "events_updated": propagated}
    due = [
        r["source_url"] for r in rows
        if not any(urlsplit(r["source_url"]).netloc.endswith(h) for h in SKIP_HOSTS)
        and scan_is_due(ledger_get(conn, "imgscan", r["source_url"]))
    ]
    batch = select(due, budget=budget, per_host=PER_HOST)
    for url, (status, image) in fan_out(batch, lambda u: _scan(fetcher, u), fetcher=fetcher):
        if status is None:
            continue  # host gave up on mid-run — not a verdict on the page; retry next run
        ledger_put(conn, "imgscan", url, status=status)
        if status != -1:  # a robots refusal costs no request
            stats["scanned"] += 1
        if image:
            cur = conn.execute(
                "UPDATE events SET image_url=? WHERE source_url=? "
                "AND (image_url IS NULL OR image_url='')",
                (image, url),
            )
            stats["found"] += 1
            stats["events_updated"] += cur.rowcount
        conn.commit()  # per page — no write transaction spans the next result
    log.info("image backfill: %s", stats)
    return stats
