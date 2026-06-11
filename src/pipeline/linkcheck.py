"""Dead-link detection for feed events.

Conservative by design — a wrongly-purged live event is worse than a lingering dead
link. Only hard "gone" statuses (404/410) count as strikes; blocks (401/403/429),
server errors, and transport failures are inconclusive. An event is pulled from the
feed after STRIKES_TO_KILL strikes at least MIN_STRIKE_GAP hours apart, and is
flagged (link_dead_at), never deleted — history and dedup state stay intact.
If a dead event headed a merged cluster, a surviving member is promoted.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

from .db import now_iso
from .dedup import SOURCE_PRIORITY
from .fetch import BROWSER_UA, RateSpec

log = logging.getLogger(__name__)

CHECK_BUDGET = 250
RECHECK_DAYS = 3
STRIKES_TO_KILL = 2
MIN_STRIKE_GAP = timedelta(hours=20)
DEAD_STATUSES = {404, 410}
RATE = RateSpec(min_interval=1.0, jitter=(0.2, 0.8))

# ra.co serves HTML only to browsers (Cloudflare) — its GraphQL API is the liveness
# source of truth there, so link-checking its pages would only produce false deaths.
SKIP_HOSTS = ("ra.co",)
BROWSER_UA_HOSTS = ("eventbrite.de", "eventbrite.com")

SCHEMA = """
CREATE TABLE IF NOT EXISTS link_checks(
  url TEXT PRIMARY KEY,
  last_status INTEGER,
  strikes INTEGER NOT NULL DEFAULT 0,
  last_checked_at TEXT NOT NULL,
  last_strike_at TEXT
);
"""


def _probe(fetcher, url: str) -> int | None:
    """Final HTTP status for the URL, or None on transport failure.
    HEAD first; any >=400 HEAD is confirmed with GET (some servers reject HEAD)."""
    ua = BROWSER_UA if any(h in urlsplit(url).netloc for h in BROWSER_UA_HOSTS) else None
    try:
        resp = fetcher.head(url, rate=RATE, user_agent=ua, check_robots=False)
        if resp.status_code < 400:
            return resp.status_code
        resp = fetcher.get(url, rate=RATE, user_agent=ua, check_robots=False)
        return resp.status_code
    except Exception as exc:  # noqa: BLE001 — transport errors are inconclusive, not strikes
        log.debug("linkcheck probe %s failed: %s", url, exc)
        return None


def check_links(conn, fetcher, city: str, budget: int = CHECK_BUDGET) -> dict:
    conn.executescript(SCHEMA)
    today = datetime.now(timezone.utc).date().isoformat()
    due_before = (datetime.now(timezone.utc) - timedelta(days=RECHECK_DAYS)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    rows = conn.execute(
        """SELECT DISTINCT e.source_url
           FROM events e JOIN occurrences o ON o.event_id = e.id
           LEFT JOIN link_checks lc ON lc.url = e.source_url
           WHERE e.city=? AND e.canonical_id=e.id AND e.link_dead_at IS NULL
             AND o.nightlife_date >= ? AND e.source_url LIKE 'http%'
             AND (lc.url IS NULL OR lc.last_checked_at < ?)
           ORDER BY lc.last_checked_at IS NOT NULL, lc.last_checked_at""",
        (city, today, due_before),
    ).fetchall()

    stats = {"due": len(rows), "checked": 0, "strikes": 0, "killed": 0, "promoted": 0}
    for row in rows:
        if stats["checked"] >= budget:
            break
        url = row["source_url"]
        if any(urlsplit(url).netloc.endswith(h) for h in SKIP_HOSTS):
            continue
        status = _probe(fetcher, url)
        stats["checked"] += 1
        now = now_iso()
        prev = conn.execute("SELECT * FROM link_checks WHERE url=?", (url,)).fetchone()

        if status in DEAD_STATUSES:
            strikes = 1
            strike_at = now
            if prev and prev["strikes"] and prev["last_strike_at"]:
                gap = datetime.fromisoformat(now.replace("Z", "+00:00")) - datetime.fromisoformat(
                    prev["last_strike_at"].replace("Z", "+00:00")
                )
                strikes = prev["strikes"] + 1 if gap >= MIN_STRIKE_GAP else prev["strikes"]
                strike_at = now if gap >= MIN_STRIKE_GAP else prev["last_strike_at"]
            stats["strikes"] += 1
        elif status is not None and status < 400:
            strikes, strike_at = 0, None  # alive — clean slate
        else:
            strikes = prev["strikes"] if prev else 0  # inconclusive — no change
            strike_at = prev["last_strike_at"] if prev else None

        conn.execute(
            """INSERT INTO link_checks(url, last_status, strikes, last_checked_at, last_strike_at)
               VALUES(?,?,?,?,?)
               ON CONFLICT(url) DO UPDATE SET last_status=excluded.last_status,
                 strikes=excluded.strikes, last_checked_at=excluded.last_checked_at,
                 last_strike_at=excluded.last_strike_at""",
            (url, status, strikes, now, strike_at),
        )

        if strikes >= STRIKES_TO_KILL:
            stats["killed"] += _kill(conn, url, now)
            stats["promoted"] += _promote_survivors(conn)
    log.info("link check: %s", stats)
    return stats


IMAGE_RECHECK_DAYS = 7
IMAGE_BUDGET = 150

IMAGE_SCHEMA = """
CREATE TABLE IF NOT EXISTS image_checks(
  url TEXT PRIMARY KEY, last_status INTEGER, last_checked_at TEXT NOT NULL
);
"""


def check_images(conn, fetcher, city: str, budget: int = IMAGE_BUDGET) -> dict:
    """Verify hotlinked image URLs still resolve to images. A hard-dead or non-image
    URL is nulled — the export-time placeholder takes over. Blocks/timeouts are left
    alone (the frontend's onerror fallback already covers them gracefully)."""
    conn.executescript(IMAGE_SCHEMA)
    today = datetime.now(timezone.utc).date().isoformat()
    due_before = (datetime.now(timezone.utc) - timedelta(days=IMAGE_RECHECK_DAYS)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    rows = conn.execute(
        """SELECT DISTINCT e.image_url FROM events e
           JOIN occurrences o ON o.event_id = e.id
           LEFT JOIN image_checks ic ON ic.url = e.image_url
           WHERE e.city=? AND e.canonical_id=e.id AND e.link_dead_at IS NULL
             AND e.image_url LIKE 'http%' AND o.nightlife_date >= ?
             AND (ic.url IS NULL OR ic.last_checked_at < ?)
           ORDER BY ic.last_checked_at IS NOT NULL, ic.last_checked_at""",
        (city, today, due_before),
    ).fetchall()

    stats = {"due": len(rows), "checked": 0, "nulled": 0}
    for row in rows:
        if stats["checked"] >= budget:
            break
        url = row["image_url"]
        status: int | None
        content_type = ""
        try:
            resp = fetcher.head(url, rate=RATE, check_robots=False)
            status = resp.status_code
            content_type = resp.headers.get("content-type", "") if hasattr(resp, "headers") else ""
        except Exception:  # noqa: BLE001 — transport errors are inconclusive
            status = None
        stats["checked"] += 1
        conn.execute(
            "INSERT INTO image_checks(url, last_status, last_checked_at) VALUES(?,?,?) "
            "ON CONFLICT(url) DO UPDATE SET last_status=excluded.last_status, "
            "last_checked_at=excluded.last_checked_at",
            (url, status, now_iso()),
        )
        dead = status in DEAD_STATUSES or (
            status is not None and status < 400 and content_type
            and not content_type.startswith("image/")
        )
        if dead:
            cur = conn.execute("UPDATE events SET image_url=NULL WHERE image_url=?", (url,))
            stats["nulled"] += cur.rowcount
    log.info("image check: %s", stats)
    return stats


def _kill(conn, url: str, now: str) -> int:
    cur = conn.execute(
        "UPDATE events SET link_dead_at=? WHERE source_url=? AND link_dead_at IS NULL",
        (now, url),
    )
    return cur.rowcount


def _promote_survivors(conn) -> int:
    """Clusters whose head just died but that contain a live member: promote the
    best-priority live member to cluster head so the event stays in the feed."""
    promoted = 0
    heads = conn.execute(
        """SELECT DISTINCT canonical_id FROM events
           WHERE canonical_id IN (SELECT id FROM events WHERE link_dead_at IS NOT NULL)
             AND link_dead_at IS NULL"""
    ).fetchall()
    for row in heads:
        members = conn.execute(
            "SELECT id, source_slug FROM events WHERE canonical_id=? AND link_dead_at IS NULL",
            (row["canonical_id"],),
        ).fetchall()
        if not members:
            continue
        new_head = min(members, key=lambda m: SOURCE_PRIORITY.get(m["source_slug"], 99))["id"]
        conn.execute(
            "UPDATE events SET canonical_id=? WHERE canonical_id=?",
            (new_head, row["canonical_id"]),
        )
        promoted += 1
    return promoted
