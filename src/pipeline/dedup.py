"""Cross-source dedup: blocked fuzzy matching + union-find clustering.

Block on (nightlife_date, city) — date must be in the key (recurring series are
different events). Never match on title alone ("Open Mic Night" trap).
"""

from __future__ import annotations

import logging
import math
import re
import unicodedata
from datetime import datetime, timezone

from rapidfuzz import fuzz

from .db import now_iso
from .venues import norm_name

log = logging.getLogger(__name__)

TITLE_THRESHOLD = 90
VENUE_THRESHOLD = 85
NEAR_MISS_BAND = (80, 90)
GEO_METERS = 250
GEO_TIGHT_METERS = 100
START_DELTA_SECONDS = 15 * 60

# Source priority for cluster head: structured API > JSON-LD > HTML.
SOURCE_PRIORITY = {
    "kulturdaten": 0, "ra": 1, "ticketmaster": 2, "berlin_de": 3, "askapunk": 4,
    "eventbrite": 5, "livegigs": 6, "rausgegangen": 7, "tip_berlin": 8, "gratis_berlin": 9,
}

_NOISE = re.compile(
    r"^(sold\s*out|ausverkauft|abgesagt|cancelled|verlegt|postponed)\s*[:!\-–]\s*"
    r"|^.{1,40}?\s(?:presents|präsentiert|praesentiert)\s*[:\-–]?\s*",
    re.IGNORECASE,
)


def norm_title(title: str) -> str:
    s = _NOISE.sub("", title.strip())
    s = unicodedata.normalize("NFC", s).casefold()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if not unicodedata.combining(c))
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class _UnionFind:
    def __init__(self):
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def _candidates(conn, city: str) -> dict[str, list[dict]]:
    """Non-range events with future occurrences, grouped by nightlife_date block."""
    today = datetime.now(timezone.utc).date().isoformat()
    rows = conn.execute(
        """SELECT e.id, e.source_slug, e.title, e.venue_name, e.venue_id, e.lat, e.lon,
                  o.nightlife_date, o.starts_at_utc
           FROM events e JOIN occurrences o ON o.event_id = e.id
           WHERE e.city=? AND e.is_range=0 AND o.nightlife_date >= ?""",
        (city, today),
    ).fetchall()
    blocks: dict[str, list[dict]] = {}
    for r in rows:
        d = dict(r)
        d["title_norm"] = norm_title(d["title"])
        d["venue_norm"] = norm_name(d["venue_name"]) if d["venue_name"] else ""
        d["start_ts"] = datetime.fromisoformat(d["starts_at_utc"].replace("Z", "+00:00")).timestamp()
        blocks.setdefault(d["nightlife_date"], []).append(d)
    return blocks


def _is_match(a: dict, b: dict) -> tuple[bool, float, float]:
    title_score = fuzz.token_set_ratio(a["title_norm"], b["title_norm"])
    venue_score = (
        fuzz.token_set_ratio(a["venue_norm"], b["venue_norm"])
        if a["venue_norm"] and b["venue_norm"] else 0.0
    )
    same_venue_id = a["venue_id"] and a["venue_id"] == b["venue_id"]
    geo_close = geo_tight = False
    if a["lat"] and a["lon"] and b["lat"] and b["lon"]:
        dist = _haversine_m(a["lat"], a["lon"], b["lat"], b["lon"])
        geo_close, geo_tight = dist <= GEO_METERS, dist <= GEO_TIGHT_METERS

    if title_score >= TITLE_THRESHOLD and (venue_score >= VENUE_THRESHOLD or same_venue_id or geo_close):
        return True, title_score, venue_score
    # cross-language catch: same place + same time beats divergent DE/EN titles
    if (same_venue_id or geo_tight) and abs(a["start_ts"] - b["start_ts"]) <= START_DELTA_SECONDS:
        return True, title_score, venue_score
    return False, title_score, venue_score


def run_dedup(conn, city: str) -> dict:
    conn.execute("DELETE FROM dedup_near_misses")  # per-run review artifact
    blocks = _candidates(conn, city)
    uf = _UnionFind()
    members: dict[str, dict] = {}
    near_misses = 0

    for _date, items in blocks.items():
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                a, b = items[i], items[j]
                if a["id"] == b["id"]:
                    continue
                matched, ts, vs = _is_match(a, b)
                members[a["id"]], members[b["id"]] = a, b
                if matched:
                    uf.union(a["id"], b["id"])
                elif NEAR_MISS_BAND[0] <= ts < NEAR_MISS_BAND[1] and a["source_slug"] != b["source_slug"]:
                    conn.execute(
                        "INSERT INTO dedup_near_misses(event_a, event_b, title_score, venue_score, noted_at) "
                        "VALUES(?,?,?,?,?)",
                        (a["id"], b["id"], ts, vs, now_iso()),
                    )
                    near_misses += 1

    clusters: dict[str, list[str]] = {}
    for eid in uf.parent:
        clusters.setdefault(uf.find(eid), []).append(eid)

    # How many events each id is *currently* canonical for, i.e. which ids are
    # already published heads. Read before the loop rewrites any of it.
    incumbents = {
        r["canonical_id"]: r["n"]
        for r in conn.execute(
            "SELECT canonical_id, COUNT(*) AS n FROM events "
            "WHERE city=? AND canonical_id != id GROUP BY canonical_id",
            (city,),
        )
    }

    merged = 0
    now = now_iso()
    for cluster_ids in clusters.values():
        if len(cluster_ids) < 2:
            continue
        head = _pick_head(cluster_ids, members, incumbents)
        for eid in cluster_ids:
            conn.execute("UPDATE events SET canonical_id=? WHERE id=?", (head, eid))
            row = conn.execute("SELECT source_slug, source_url FROM events WHERE id=?", (eid,)).fetchone()
            conn.execute(
                "INSERT OR IGNORE INTO merged_sources(canonical_id, source_slug, source_url) VALUES(?,?,?)",
                (head, row["source_slug"], row["source_url"]),
            )
            if eid != head:
                # Record the loser so a downstream reference to it still resolves,
                # and re-point any alias that used to lead here (chains collapse to
                # one hop, so a lookup is always a single read).
                conn.execute(
                    "INSERT INTO event_aliases(alias_id, canonical_id, noted_at) VALUES(?,?,?) "
                    "ON CONFLICT(alias_id) DO UPDATE SET canonical_id=excluded.canonical_id",
                    (eid, head, now),
                )
                conn.execute(
                    "UPDATE event_aliases SET canonical_id=? WHERE canonical_id=?", (head, eid)
                )
        _fill_head_from_members(conn, head, [e for e in cluster_ids if e != head])
        merged += len(cluster_ids) - 1

    # events whose cluster dissolved (e.g. a member was re-matched elsewhere) stay self-canonical
    conn.execute(
        "UPDATE events SET canonical_id=id WHERE canonical_id NOT IN (SELECT id FROM events)"
    )
    return {"blocks": len(blocks), "merged": merged, "near_misses": near_misses}


def _pick_head(cluster_ids: list[str], members: dict[str, dict], incumbents: dict[str, int]) -> str:
    """Choose the id a cluster collapses onto — incumbent first, then source richness.

    Source priority alone used to decide this, re-evaluated from scratch every
    run. That is unstable in a way that reaches users: the day a higher-priority
    source starts carrying an event we already had, the head changes, the old id
    stops being exported, and every stored reference to it downstream (Gobento
    bookmarks, saved plans) dangles. The event is the same event; only our
    arbitrary choice of representative moved.

    So an id that is already the head of a cluster keeps the job, even against a
    richer source — the *content* still gets upgraded either way, because
    `_fill_head_from_members` copies the better fields onto whichever id wins.
    Priority only breaks ties among ids with no incumbency, and the id itself
    breaks the remaining ties (ULIDs sort by creation time, so the oldest wins —
    stability again, and it makes the run reproducible).
    """
    return min(
        cluster_ids,
        key=lambda e: (-incumbents.get(e, 0), SOURCE_PRIORITY.get(members[e]["source_slug"], 99), e),
    )


def _fill_head_from_members(conn, head_id: str, member_ids: list[str]) -> None:
    """Fill the head's NULL fields from members (price text, image, geo)."""
    head = conn.execute("SELECT * FROM events WHERE id=?", (head_id,)).fetchone()
    updates: dict[str, object] = {}
    for mid in member_ids:
        m = conn.execute("SELECT * FROM events WHERE id=?", (mid,)).fetchone()
        for col in ("image_url", "lat", "lon", "venue_id"):
            if head[col] in (None, "") and m[col] not in (None, "") and col not in updates:
                updates[col] = m[col]
        if (head["price_json"] or "").find('"type": "unknown"') >= 0 and m["price_json"] and \
           m["price_json"].find('"type": "unknown"') < 0 and "price_json" not in updates:
            updates["price_json"] = m["price_json"]
    if updates:
        sets = ", ".join(f"{k}=?" for k in updates)
        conn.execute(f"UPDATE events SET {sets} WHERE id=?", (*updates.values(), head_id))
