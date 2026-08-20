"""SQLite storage layer. All SQL lives here — the one module to swap if Postgres ever arrives."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .ids import event_id
from .models import Event, Occurrence, Price

SCHEMA = """
CREATE TABLE IF NOT EXISTS crawl_ledger(
  source_slug TEXT NOT NULL,
  url TEXT NOT NULL,
  etag TEXT, last_modified TEXT, lastmod TEXT,
  content_hash TEXT, last_status INTEGER,
  first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
  PRIMARY KEY(source_slug, url)
);
CREATE TABLE IF NOT EXISTS raw_snapshots(
  id INTEGER PRIMARY KEY,
  source_slug TEXT NOT NULL, source_event_id TEXT NOT NULL,
  payload_json TEXT NOT NULL, fetched_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snapshots_src ON raw_snapshots(source_slug, source_event_id);
CREATE TABLE IF NOT EXISTS venues(
  id TEXT PRIMARY KEY, canonical_name TEXT NOT NULL, aliases_json TEXT DEFAULT '[]',
  lat REAL, lon REAL, street TEXT, postal_code TEXT, city TEXT,
  osm_id TEXT, category_prior TEXT
);
CREATE TABLE IF NOT EXISTS events(
  id TEXT PRIMARY KEY, canonical_id TEXT NOT NULL, city TEXT NOT NULL,
  source_slug TEXT NOT NULL, source_event_id TEXT NOT NULL, source_url TEXT NOT NULL,
  title TEXT NOT NULL,
  category TEXT NOT NULL DEFAULT 'other',
  category_tier TEXT NOT NULL DEFAULT 'fallback_other',
  category_confidence REAL NOT NULL DEFAULT 0,
  tags_json TEXT DEFAULT '[]', source_category_raw TEXT,
  is_range INTEGER DEFAULT 0, range_start TEXT, range_end TEXT,
  event_status TEXT DEFAULT 'scheduled', attendance_mode TEXT DEFAULT 'offline',
  venue_id TEXT, venue_name TEXT, address_json TEXT DEFAULT '{}', lat REAL, lon REAL,
  price_json TEXT, image_url TEXT, content_hash TEXT,
  first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
  UNIQUE(source_slug, source_event_id)
);
CREATE INDEX IF NOT EXISTS idx_events_city ON events(city);
CREATE INDEX IF NOT EXISTS idx_events_canonical ON events(canonical_id);
CREATE TABLE IF NOT EXISTS occurrences(
  id TEXT PRIMARY KEY, event_id TEXT NOT NULL REFERENCES events(id),
  starts_at_utc TEXT NOT NULL, ends_at_utc TEXT,
  starts_at_local TEXT NOT NULL, doors_at_local TEXT,
  nightlife_date TEXT NOT NULL, time_unknown INTEGER DEFAULT 0,
  status TEXT DEFAULT 'scheduled',
  UNIQUE(event_id, starts_at_utc)
);
CREATE INDEX IF NOT EXISTS idx_occ_date ON occurrences(nightlife_date);
CREATE TABLE IF NOT EXISTS merged_sources(
  canonical_id TEXT NOT NULL, source_slug TEXT NOT NULL, source_url TEXT NOT NULL,
  PRIMARY KEY(canonical_id, source_slug, source_url)
);
CREATE TABLE IF NOT EXISTS llm_queue(
  event_id TEXT PRIMARY KEY, queued_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dedup_near_misses(
  id INTEGER PRIMARY KEY, event_a TEXT NOT NULL, event_b TEXT NOT NULL,
  title_score REAL, venue_score REAL, noted_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, value TEXT);
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(path: str | Path) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    # multi-connection concurrency (one conn per worker thread): writers queue at the
    # WAL write lock instead of failing, and NORMAL is durable enough under WAL
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    _migrate(conn)
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(events)")}
    if "description" not in cols:
        # public description for open-licensed sources only (CC-BY kulturdaten etc.)
        conn.execute("ALTER TABLE events ADD COLUMN description TEXT")
    if "link_dead_at" not in cols:
        # set when the source link is confirmed dead; excluded from export, never deleted
        conn.execute("ALTER TABLE events ADD COLUMN link_dead_at TEXT")


# --- crawl ledger -----------------------------------------------------------

def ledger_get(conn, source: str, url: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM crawl_ledger WHERE source_slug=? AND url=?", (source, url)
    ).fetchone()


def ledger_put(conn, source: str, url: str, *, etag=None, last_modified=None,
               lastmod=None, content_hash=None, status=None) -> None:
    now = now_iso()
    conn.execute(
        """INSERT INTO crawl_ledger(source_slug,url,etag,last_modified,lastmod,content_hash,last_status,first_seen_at,last_seen_at)
           VALUES(?,?,?,?,?,?,?,?,?)
           ON CONFLICT(source_slug,url) DO UPDATE SET
             etag=COALESCE(excluded.etag, crawl_ledger.etag),
             last_modified=COALESCE(excluded.last_modified, crawl_ledger.last_modified),
             lastmod=COALESCE(excluded.lastmod, crawl_ledger.lastmod),
             content_hash=COALESCE(excluded.content_hash, crawl_ledger.content_hash),
             last_status=COALESCE(excluded.last_status, crawl_ledger.last_status),
             last_seen_at=excluded.last_seen_at""",
        (source, url, etag, last_modified, lastmod, content_hash, status, now, now),
    )
    # self-committing: callers (sitemap adapters, image backfill) invoke this between
    # HTTP fetches — an open write tx spanning a fetch starves every other worker at
    # the WAL write lock ("database is locked" past busy_timeout)
    conn.commit()


# --- snapshots ---------------------------------------------------------------

def snapshot(conn, source: str, source_event_id: str, payload: dict) -> None:
    conn.execute(
        "INSERT INTO raw_snapshots(source_slug, source_event_id, payload_json, fetched_at) VALUES(?,?,?,?)",
        (source, source_event_id, json.dumps(payload, ensure_ascii=False, default=str), now_iso()),
    )


# --- events / occurrences ----------------------------------------------------

def upsert_event(conn, ev: Event) -> tuple[str, bool]:
    """Insert or update an event. Returns (event_id, changed) — changed gates downstream work."""
    now = now_iso()
    row = conn.execute(
        "SELECT id, content_hash, category, category_tier, category_confidence FROM events "
        "WHERE source_slug=? AND source_event_id=?",
        (ev.source, ev.source_event_id),
    ).fetchone()
    new_hash = ev.content_hash()
    if row is None:
        ev.id = ev.id or event_id()
        ev.canonical_id = ev.canonical_id or ev.id
        conn.execute(
            """INSERT INTO events(id,canonical_id,city,source_slug,source_event_id,source_url,title,
                 category,category_tier,category_confidence,tags_json,source_category_raw,
                 is_range,range_start,range_end,event_status,attendance_mode,
                 venue_id,venue_name,address_json,lat,lon,price_json,image_url,content_hash,
                 description,first_seen_at,last_seen_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (ev.id, ev.canonical_id, ev.city, ev.source, ev.source_event_id, ev.source_url,
             ev.title, ev.category, ev.category_tier, ev.category_confidence,
             json.dumps(ev.tags), ev.source_category_raw,
             int(ev.is_range), ev.range_start, ev.range_end, ev.event_status, ev.attendance_mode,
             ev.venue_id, ev.venue_name, json.dumps(ev.address), ev.lat, ev.lon,
             json.dumps(ev.price.to_json()), ev.image_url, new_hash,
             (ev.description if ev.description_public else None), now, now),
        )
        changed = True
    else:
        ev.id = row["id"]
        changed = row["content_hash"] != new_hash
        conn.execute("UPDATE events SET last_seen_at=? WHERE id=?", (now, ev.id))
        if changed:
            conn.execute(
                """UPDATE events SET title=?, source_url=?, is_range=?, range_start=?, range_end=?,
                     event_status=?, attendance_mode=?, venue_id=?, venue_name=?, address_json=?,
                     lat=?, lon=?, price_json=?, image_url=COALESCE(?, image_url), content_hash=?,
                     source_category_raw=?, description=?, last_seen_at=? WHERE id=?""",
                (ev.title, ev.source_url, int(ev.is_range), ev.range_start, ev.range_end,
                 ev.event_status, ev.attendance_mode, ev.venue_id, ev.venue_name,
                 json.dumps(ev.address), ev.lat, ev.lon, json.dumps(ev.price.to_json()),
                 ev.image_url, new_hash, ev.source_category_raw,
                 (ev.description if ev.description_public else None), now, ev.id),
            )
    if changed:
        for occ in ev.occurrences:
            conn.execute(
                """INSERT INTO occurrences(id,event_id,starts_at_utc,ends_at_utc,starts_at_local,
                     doors_at_local,nightlife_date,time_unknown,status)
                   VALUES(?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(event_id,starts_at_utc) DO UPDATE SET
                     ends_at_utc=excluded.ends_at_utc, starts_at_local=excluded.starts_at_local,
                     doors_at_local=excluded.doors_at_local, nightlife_date=excluded.nightlife_date,
                     time_unknown=excluded.time_unknown, status=excluded.status""",
                (event_id(), ev.id, occ.starts_at_utc, occ.ends_at_utc, occ.starts_at_local,
                 occ.doors_at_local, occ.nightlife_date, int(occ.time_unknown), occ.status),
            )
    return ev.id, changed


def reconcile_occurrences(conn, eid: str, starts_at_utc: set[str]) -> int:
    """Remove occurrences a successfully refreshed source no longer reports.

    Adapters may yield the same source event more than once (one row per slot),
    so callers collect the complete set and reconcile only after the adapter has
    finished without error. This keeps valid multi-slot events while removing
    stale times left behind by reschedules or changed opening-hour schedules.
    """
    if not starts_at_utc:
        return 0
    placeholders = ",".join("?" for _ in starts_at_utc)
    cur = conn.execute(
        f"DELETE FROM occurrences WHERE event_id=? AND starts_at_utc NOT IN ({placeholders})",
        (eid, *sorted(starts_at_utc)),
    )
    return cur.rowcount


def set_category(conn, eid: str, category: str, tier: str, confidence: float, tags: list[str]) -> None:
    conn.execute(
        "UPDATE events SET category=?, category_tier=?, category_confidence=?, tags_json=? WHERE id=?",
        (category, tier, confidence, json.dumps(tags), eid),
    )


def queue_for_llm(conn, eid: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO llm_queue(event_id, queued_at) VALUES(?,?)", (eid, now_iso())
    )


def mark_cancelled_missing(conn) -> None:
    """Events not seen for 7 days whose occurrences are in the future → leave as-is (sources
    differ in re-listing behaviour); cancellation comes from event_status only."""
    # Intentionally a no-op for the pilot; documented decision.


# --- feed query ---------------------------------------------------------------

def feed_rows(conn, city: str, date_from: str, date_to: str) -> list[sqlite3.Row]:
    """Occurrence-joined canonical events for export. Only cluster heads (canonical_id == id)."""
    return conn.execute(
        """SELECT e.*, o.starts_at_utc, o.ends_at_utc, o.starts_at_local, o.doors_at_local,
                  o.nightlife_date, o.time_unknown, o.status AS occ_status
           FROM occurrences o JOIN events e ON e.id = o.event_id
           WHERE e.city = ? AND e.canonical_id = e.id
             AND o.nightlife_date >= ? AND o.nightlife_date <= ?
             AND e.attendance_mode != 'online'
             AND e.link_dead_at IS NULL
           ORDER BY o.starts_at_utc""",
        (city, date_from, date_to),
    ).fetchall()
