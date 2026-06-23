"""Push the exported feed into Gobento's Cloudflare D1 (`city_events` table).

The scraper writes D1 directly via the D1 REST `/query` endpoint — the same
contract Gobento's backend uses (`backend/app/db/pool.js`). Rows mirror the
static-JSON export (`export._row_to_item`) so the two stay byte-for-byte
consistent.

Built to stay cheap at scale (every major city, multiple times a day). D1 bills
rows *written* ~1000x more than rows *read*, so each sync:

  1. reads back the city's existing (id, content_hash) — cheap — and writes ONLY
     rows that are new or actually changed; deletes only rows that disappeared.
     Steady-state writes fall to near zero.
  2. inlines escaped literals into multi-row INSERTs packed to an ~80 KB
     statement budget (~100-200 rows/request), sidestepping D1's 100-bound-param
     cap and collapsing ~100 tiny POSTs into a handful.
  3. fires those few batches concurrently over one shared HTTP pool, with
     retry/backoff for 429/5xx.

Env (skips with a log + exit 0 if unset, so local runs never fail):
  CF_ACCOUNT_ID / CLOUDFLARE_ACCOUNT_ID
  CF_D1_DATABASE_ID
  CF_API_TOKEN / CLOUDFLARE_API_TOKEN   (needs D1 edit scope)
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import httpx

from . import config as cfg
from .db import feed_rows, now_iso
from .export import _row_to_item

log = logging.getLogger(__name__)

# Data columns, in INSERT order. `content_hash` + `synced_at` are appended below.
# Keep in sync with city_events in Gobento/backend/schema.sql.
CONTENT_COLUMNS = [
    "id", "event_id", "city", "title", "category", "category_label", "tags_json",
    "starts_at_utc", "starts_at_local", "ends_at_utc", "doors_at_local", "nightlife_date",
    "time_unknown", "is_range", "range_start", "range_end", "event_status", "venue_name",
    "address_json", "lat", "lon", "price_json", "image_url", "placeholder_url",
    "description", "source", "source_url", "source_is_record",
]
COLUMNS = CONTENT_COLUMNS + ["content_hash", "synced_at"]

# Stay comfortably under D1's 100 KB max SQL statement length.
BUDGET_BYTES = 80_000
# Rows of (id, content_hash) to pull per read page when diffing.
READ_PAGE = 5_000
# Concurrent write requests — modest, to respect D1 rate limits.
MAX_WORKERS = 6
_RETRY_STATUS = {429, 500, 502, 503, 504}


# --- value mapping -----------------------------------------------------------

def _row_dict(item: dict, city: str) -> dict:
    """Map an export item dict → city_events content columns (no hash/synced)."""
    geo = item.get("geo") or {}
    return {
        "id": f"{item['id']}::{item['starts_at_utc']}",   # occurrence-unique
        "event_id": item["id"],
        "city": city,
        "title": item.get("title"),
        "category": item.get("category"),
        "category_label": item.get("category_label"),
        "tags_json": json.dumps(item.get("tags") or [], ensure_ascii=False),
        "starts_at_utc": item.get("starts_at_utc"),
        "starts_at_local": item.get("starts_at_local"),
        "ends_at_utc": item.get("ends_at_utc"),
        "doors_at_local": item.get("doors_at_local"),
        "nightlife_date": item.get("nightlife_date"),
        "time_unknown": int(bool(item.get("time_unknown"))),
        "is_range": int(bool(item.get("is_range"))),
        "range_start": item.get("range_start"),
        "range_end": item.get("range_end"),
        "event_status": item.get("event_status"),
        "venue_name": item.get("venue_name"),
        "address_json": json.dumps(item.get("address") or {}, ensure_ascii=False),
        "lat": geo.get("lat"),
        "lon": geo.get("lon"),
        "price_json": json.dumps(item.get("price") or {}, ensure_ascii=False),
        "image_url": item.get("image_url"),
        "placeholder_url": item.get("placeholder_url"),
        "description": item.get("description"),
        "source": item.get("source"),
        "source_url": item.get("source_url"),
        "source_is_record": int(bool(item.get("source_is_record"))),
    }


def content_hash(row: dict) -> str:
    """Stable hash of a row's content (excludes synced_at — that changes per run)."""
    payload = json.dumps([row[c] for c in CONTENT_COLUMNS], ensure_ascii=False, default=str)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _sql_literal(v) -> str:
    """Encode a Python value as a safe SQLite literal (no bound params)."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return repr(v) if math.isfinite(v) else "NULL"
    # SQLite string literals only need single-quote doubling; backslashes are
    # literal. Strip NULs which can't appear in a text literal.
    s = str(v).replace("\x00", "")
    return "'" + s.replace("'", "''") + "'"


# --- batch SQL builders ------------------------------------------------------

def _insert_prefix() -> str:
    return f"INSERT INTO city_events ({', '.join(COLUMNS)}) VALUES "


def _insert_suffix() -> str:
    updates = ", ".join(f"{c}=excluded.{c}" for c in COLUMNS if c not in ("id", "event_id"))
    return f" ON CONFLICT(id) DO UPDATE SET {updates}"


def iter_insert_batches(rows: list[list]):
    """Yield multi-row UPSERT statements packed under BUDGET_BYTES."""
    prefix, suffix = _insert_prefix(), _insert_suffix()
    base = len(prefix) + len(suffix)
    cur: list[str] = []
    size = base
    for r in rows:
        tup = "(" + ",".join(_sql_literal(v) for v in r) + ")"
        add = len(tup) + (1 if cur else 0)
        if cur and size + add > BUDGET_BYTES:
            yield prefix + ",".join(cur) + suffix
            cur, size = [], base
            add = len(tup)
        cur.append(tup)
        size += add
    if cur:
        yield prefix + ",".join(cur) + suffix


def iter_delete_batches(ids: list[str]):
    """Yield DELETE ... WHERE id IN (...) statements packed under BUDGET_BYTES."""
    prefix, suffix = "DELETE FROM city_events WHERE id IN (", ")"
    base = len(prefix) + len(suffix)
    cur: list[str] = []
    size = base
    for i in ids:
        lit = _sql_literal(i)
        add = len(lit) + (1 if cur else 0)
        if cur and size + add > BUDGET_BYTES:
            yield prefix + ",".join(cur) + suffix
            cur, size = [], base
            add = len(lit)
        cur.append(lit)
        size += add
    if cur:
        yield prefix + ",".join(cur) + suffix


# --- D1 client ---------------------------------------------------------------

class D1Client:
    """Minimal Cloudflare D1 REST client (statement + optional bound params)."""

    def __init__(self, account_id: str, database_id: str, token: str):
        self._url = (
            f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
            f"/d1/database/{database_id}/query"
        )
        self._client = httpx.Client(
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            # one pool, shared across worker threads
            limits=httpx.Limits(max_connections=MAX_WORKERS, max_keepalive_connections=MAX_WORKERS),
            timeout=60.0,
        )

    @classmethod
    def from_env(cls) -> "D1Client | None":
        account = os.environ.get("CF_ACCOUNT_ID") or os.environ.get("CLOUDFLARE_ACCOUNT_ID")
        database = os.environ.get("CF_D1_DATABASE_ID")
        token = os.environ.get("CF_API_TOKEN") or os.environ.get("CLOUDFLARE_API_TOKEN")
        if not (account and database and token):
            return None
        return cls(account, database, token)

    def execute(self, sql: str, params: list | None = None) -> dict:
        for attempt in range(3):
            resp = self._client.post(self._url, json={"sql": sql, "params": params or []})
            if resp.status_code in _RETRY_STATUS and attempt < 2:
                time.sleep(0.5 * (attempt + 1))
                continue
            resp.raise_for_status()
            body = resp.json()
            if not body.get("success"):
                errs = body.get("errors") or [{"message": "D1 query failed"}]
                raise RuntimeError(f"D1 error: {errs[0].get('message')} | sql: {sql[:80]}")
            return body
        raise RuntimeError("D1 request exhausted retries")

    def fetch_existing_hashes(self, city: str) -> dict[str, str]:
        """All (id → content_hash) currently stored for a city. Keyset-paginated."""
        out: dict[str, str] = {}
        cursor = ""
        while True:
            body = self.execute(
                "SELECT id, content_hash FROM city_events "
                "WHERE city = ? AND id > ? ORDER BY id LIMIT ?",
                [city, cursor, READ_PAGE],
            )
            rows = body["result"][0]["results"]
            for r in rows:
                out[r["id"]] = r["content_hash"]
            if len(rows) < READ_PAGE:
                break
            cursor = rows[-1]["id"]
        return out

    def run_batches(self, batches: list[str]) -> None:
        """Execute write batches concurrently; raises on the first failure."""
        if not batches:
            return
        with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(batches))) as ex:
            futures = [ex.submit(self.execute, b) for b in batches]
            for f in as_completed(futures):
                f.result()  # propagate exceptions

    def close(self) -> None:
        self._client.close()


# --- sync --------------------------------------------------------------------

def sync_city(conn, city: str, client: D1Client) -> dict:
    """Diff the city's export window against D1 and write only the delta."""
    today = date.today()
    date_from = today.isoformat()
    date_to = (today + timedelta(days=cfg.WINDOW_DAYS)).isoformat()
    items = [_row_to_item(r) for r in feed_rows(conn, city, date_from, date_to)]

    if not items:
        # Never prune on an empty result — a failed/zero scrape must not wipe the
        # city. (CI's freshness gate already aborts before this step on zero yield.)
        log.warning("sync-d1: 0 events for %s — skipping diff + write", city)
        return {"city": city, "upserted": 0, "deleted": 0, "unchanged": 0,
                "requests": 0, "skipped": True}

    desired = {}
    for item in items:
        row = _row_dict(item, city)
        desired[row["id"]] = (row, content_hash(row))

    existing = client.fetch_existing_hashes(city)

    synced_at = now_iso()
    to_upsert = [
        [row[c] for c in CONTENT_COLUMNS] + [h, synced_at]
        for rid, (row, h) in desired.items()
        if existing.get(rid) != h          # new or changed only
    ]
    to_delete = [rid for rid in existing if rid not in desired]

    batches = list(iter_insert_batches(to_upsert)) + list(iter_delete_batches(to_delete))
    client.run_batches(batches)

    unchanged = len(desired) - len(to_upsert)
    log.info(
        "sync-d1: %s — upserted %d, deleted %d, unchanged %d (%d requests)",
        city, len(to_upsert), len(to_delete), unchanged, len(batches),
    )
    return {"city": city, "upserted": len(to_upsert), "deleted": len(to_delete),
            "unchanged": unchanged, "requests": len(batches), "skipped": False}


def sync(city: str) -> dict:
    """Entry point for the `sync-d1` CLI command."""
    from .db import connect

    client = D1Client.from_env()
    if client is None:
        log.warning(
            "sync-d1: CF_ACCOUNT_ID / CF_D1_DATABASE_ID / CF_API_TOKEN not set — skipping"
        )
        return {"city": city, "skipped": True, "reason": "no-credentials"}

    conn = connect(cfg.DB_PATH)
    try:
        return sync_city(conn, city, client)
    finally:
        client.close()
        conn.close()
