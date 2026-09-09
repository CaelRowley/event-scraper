"""Static JSON export — the published feed Gobento reads, plus the demo slices.

Two shapes come out of here, and the distinction matters:

* **The demo export** (`index.json` + per-day files) is unchanged: whole rows,
  human-readable, for the public demo page.

* **The published feed** (`manifest.json`, `feed.<hash>.json.gz`,
  `geo.<hash>.json.gz`, `events/<id>.<hash>.json.gz`) is what the Gobento app syncs.
  It exists because serving a browsable feed out of a SQL database meant every
  visitor scanning the whole table several times over — the feed is the same
  document for everybody, so it is published once here and read straight from
  object storage, and the database is left to hold only per-user state.

The published shape is content-addressed: the hash in a filename is of the bytes
inside it, so a URL's content can never change. Clients keep whatever they have
and re-fetch only when the manifest names a hash they don't hold. `manifest.json`
is the one mutable object, written last, so a reader either sees the whole
previous version or the whole new one.

The list objects carry `LIST_FIELDS` only. `description` is ~43% of the payload
and is wanted on one screen, so it lives in the per-event objects; the list rows
carry `h`, the hash of their event object, which is how a client builds that URL
without another round-trip.

That `h` is the only published route to a detail object, and it appears nowhere
but on a feed row. Two things follow, and the shape here depends on both:

* A reader that can address a detail object necessarily holds its list row, so
  the object carries only what the row lacks — `description`, the occurrence
  list, and the alias trail. Repeating title, venue, price and the rest would be
  about half the payload spent on bytes the reader already has.
* An object for an event that never reaches a listing is unreachable by
  construction, so none is written. That was ~6.5% of the bucket.

Detail objects ship gzipped (`events/<id>.<hash>.json.gz`), like the feed and geo
lists. R2 serves bytes verbatim and will not compress on the way out, so anything
published uncompressed stays uncompressed on every read.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
from datetime import date, timedelta
from pathlib import Path

from . import config as cfg
from .categorise.taxonomy import LABELS
from .db import now_iso
from .placeholders import placeholder_url
from .redact import contains_contact, redact_contacts

# none: the client draws its own panel for an event with no photo (EventImage in
# Gobento). LoremFlickr was the default and was worse than nothing: `art,gallery`
# — 471 events, a third of all placeholders — answered HTTP 500 every time, and a
# nonsense keyword still returned a photo, so the keyword never constrained the
# result. Broken URLs became the client's hardcoded fallback image; irrelevant
# ones became a random 640x360 photo on an event it had nothing to do with. It
# also hotlinked CC images without per-image attribution, which placeholders.py
# has flagged as unresolved since it was written.
PLACEHOLDER_PROVIDER = "none"  # loremflickr | picsum | none

# Bumped when the published shape changes incompatibly. The client compares it
# against its own compiled constant and resets its cache on a mismatch, so an
# old tab can't misread a new feed. Keep in step with SCHEMA_VERSION in
# Gobento's frontend/src/services/discoverStore.ts.
#
# 2: detail objects are gzipped, named `.json.gz`, and carry only the fields a
#    list row lacks. A v1 client reading a v2 object finds no title/venue/price.
SCHEMA_VERSION = 2

# Fields a browsing client needs for a card. Everything else — description above
# all — is in the per-event object, fetched only when a detail view opens.
LIST_FIELDS = (
    "id", "event_id", "city", "title", "category", "category_label", "tags",
    "starts_at_utc", "starts_at_local", "ends_at_utc", "doors_at_local",
    "nightlife_date", "time_unknown", "is_range", "range_start", "range_end",
    "event_status", "venue_name", "address", "geo", "price",
    # `image_attribution` rides the list row rather than only the detail object:
    # CC-BY wants credit wherever the photo is shown, and the card shows it.
    "image_url", "image_attribution", "placeholder_url",
    "source", "source_url", "source_is_record",
    "series_key", "first_seen_at", "h",
)

# Internal scoring/bookkeeping that must never reach a published object.
PRIVATE_FIELDS = frozenset({"category_tier", "category_confidence", "source_category_raw"})

# Fields that belong to one occurrence rather than to the event. They move into
# the detail object's `occurrences` list instead of sitting at its top level.
OCCURRENCE_FIELDS = frozenset({
    "id", "starts_at_utc", "ends_at_utc", "starts_at_local", "doors_at_local",
    "nightlife_date", "time_unknown", "event_status",
})

log = logging.getLogger(__name__)


def _canonical_json(payload) -> bytes:
    """Byte-stable JSON — the input to every content hash, so key order can't churn."""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _hash(payload) -> str:
    return hashlib.sha1(_canonical_json(payload)).hexdigest()[:8]


def _stock_url(row, keys) -> str | None:
    """The Commons photo resolved for this event, if stock_photos.py found one."""
    return row["stock_image_url"] if "stock_image_url" in keys else None


def _stock_attribution(row, keys) -> dict | None:
    """Author/licence for the Commons photo — CC-BY obliges us to carry it."""
    if "stock_attribution" not in keys or not row["stock_attribution"]:
        return None
    try:
        return json.loads(row["stock_attribution"])
    except (TypeError, ValueError):
        return None


def _series_key(row) -> str:
    """Groups the many dated records a recurring listing arrives as.

    Sources like kulturdaten emit one record per date for a run of the same
    exhibition, each with its own id, so a feed keyed on `event_id` shows the
    same show 150 times. The one signal that survives into the export is the
    shared `source_url`, so hash that. Consumers may collapse a series to one
    card; nothing here depends on it.
    """
    return hashlib.sha1((row["source_url"] or row["id"]).encode()).hexdigest()[:12]


def _row_to_item(row) -> dict:
    keys = row.keys()
    # The one gate between a stored row and anything published. Source prose carries
    # organisers' phone numbers and addresses; the licence covers the text, not
    # re-publishing someone's contact details at a new address. Applied here so the
    # demo export and the published feed cannot drift apart on it — see redact.py.
    title, _ = redact_contacts(row["title"])
    description, _ = redact_contacts(row["description"])
    return {
        # Occurrence-unique. One event with several start times is several cards,
        # and this is the id a client addresses a single one of them by.
        "id": f"{row['id']}::{row['starts_at_utc']}",
        # The event itself — stable across its occurrences, and the id downstream
        # persists on a bookmark. `event_aliases` resolves superseded values of it.
        "event_id": row["id"],
        "city": row["city"],
        "series_key": _series_key(row),
        # When this event first entered the feed — lets a client mark "new".
        # Absent on databases predating the column, hence the guard.
        "first_seen_at": row["first_seen_at"] if "first_seen_at" in keys else None,
        "title": title,
        "category": row["category"],
        "category_label": LABELS.get(row["category"], "Other"),
        "category_tier": row["category_tier"],
        "tags": json.loads(row["tags_json"] or "[]"),
        "starts_at_utc": row["starts_at_utc"],
        "starts_at_local": row["starts_at_local"],
        "ends_at_utc": row["ends_at_utc"],
        "doors_at_local": row["doors_at_local"],
        "nightlife_date": row["nightlife_date"],
        "time_unknown": bool(row["time_unknown"]),
        "is_range": bool(row["is_range"]),
        "range_start": row["range_start"],
        "range_end": row["range_end"],
        "event_status": row["event_status"] if row["occ_status"] == "scheduled" else row["occ_status"],
        "venue_name": row["venue_name"],
        "address": json.loads(row["address_json"] or "{}"),
        "geo": {"lat": row["lat"], "lon": row["lon"]} if row["lat"] is not None else None,
        "price": json.loads(row["price_json"] or "{}"),
        # Three tiers, best first: the source's own image; a Commons photo of the
        # venue (resolved in stock_photos.py, licensed and attributed); and only then a
        # decorative stand-in. `image_url` is what a card shows, so the Commons
        # result is promoted into it rather than kept in a field every consumer
        # would have to know to fall back to.
        "image_url": row["image_url"] or _stock_url(row, keys),
        "image_attribution": _stock_attribution(row, keys) if not row["image_url"] else None,
        # Decorative stand-in when we found nothing real — stable per event, varied
        # across events; UIs should treat it as decoration and may label it "stock"
        "placeholder_url": None if (row["image_url"] or _stock_url(row, keys)) else placeholder_url(
            row["id"], row["category"], PLACEHOLDER_PROVIDER),
        "description": description,  # open-licensed sources only (CC-BY), contacts stripped
        "source": row["source_slug"],
        "source_url": row["source_url"],
        # true when the link is a raw data record, not a human page — UIs should label it
        "source_is_record": row["source_url"].startswith("https://api-v2.kulturdaten.berlin"),
    }


def _is_presentable(item: dict) -> bool:
    """An event with neither a description nor a picture has nothing to show.

    Such rows are hidden from every *listing* surface, but they still get a
    per-event object: a deep link or an already-saved bookmark has to open.
    """
    return bool(item.get("description")) or bool(item.get("image_url"))


def _list_row(item: dict, event_hash: str) -> dict:
    row = {k: item.get(k) for k in LIST_FIELDS}
    row["h"] = event_hash
    return row


def _detail_payload(event_id: str, occurrences: list[dict], aliases: list[str]) -> dict:
    """What a reader can't already have: the heavy fields, the dates, the alias trail.

    `event_id` is the one list field kept — it is how a fetched object ties back
    to the row that named it, and to a bookmark holding a superseded id.
    """
    first = occurrences[0]
    body = {
        k: v for k, v in first.items()
        if k not in LIST_FIELDS and k not in PRIVATE_FIELDS and k not in OCCURRENCE_FIELDS
    }
    return {
        "event_id": event_id,
        **body,
        "occurrences": [
            {k: o[k] for k in sorted(OCCURRENCE_FIELDS)}
            for o in sorted(occurrences, key=lambda o: o["starts_at_utc"])
        ],
        "aliases": sorted(aliases),
    }


def build_feed(conn, city: str, items: list[dict], meta: dict) -> dict:
    """Assemble the published objects. Pure — writing/uploading is the caller's job.

    Returns `{"manifest": ..., "feed": (name, payload), "geo": (name, payload),
    "events": {name: payload}}`, where each name already carries its content hash.
    """
    # Two views of the same ledger: per event (inside its object) and the flat
    # alias → canonical map in the manifest. The map is what a client actually
    # needs — it holds a superseded id from a bookmark and has nothing else to
    # look it up by, so an index keyed by the old id is the only usable shape.
    aliases: dict[str, list[str]] = {}
    alias_map: dict[str, str] = {}
    for r in conn.execute(
        "SELECT a.alias_id, a.canonical_id FROM event_aliases a "
        "JOIN events e ON e.id = a.canonical_id WHERE e.city = ?",
        (city,),
    ):
        aliases.setdefault(r["canonical_id"], []).append(r["alias_id"])
        alias_map[r["alias_id"]] = r["canonical_id"]

    # Listing first: an event that never reaches a feed row has no published hash,
    # so nothing could address its detail object. Building one would be dead weight.
    listed = [i for i in items if _is_presentable(i)]
    listed.sort(key=lambda i: (i["starts_at_utc"] or "", i["id"]))

    by_event: dict[str, list[dict]] = {}
    for item in listed:
        by_event.setdefault(item["event_id"], []).append(item)

    events: dict[str, dict] = {}
    event_hashes: dict[str, str] = {}
    for event_id, occurrences in by_event.items():
        payload = _detail_payload(event_id, occurrences, aliases.get(event_id, []))
        h = _hash(payload)
        event_hashes[event_id] = h
        events[f"events/{event_id}.{h}.json.gz"] = payload

    feed_rows = [_list_row(i, event_hashes[i["event_id"]]) for i in listed]
    geo_rows = [r for r in feed_rows if r.get("geo")]

    feed_hash = _hash(feed_rows)
    geo_hash = _hash(geo_rows)

    # Per-day counts drive the calendar, and a day's badge should count events,
    # not occurrences — several slots on one night are one card.
    days: dict[str, set] = {}
    category_events: dict[str, set] = {}
    category_labels: dict[str, str] = {}
    for row in feed_rows:
        days.setdefault(row["nightlife_date"], set()).add(row["event_id"])
        # Distinct events, not rows, for the same reason as the day counts: the
        # client collapses an event's occurrences into one card, so counting rows
        # would show a chip promising more than the grid delivers.
        category_events.setdefault(row["category"], set()).add(row["event_id"])
        category_labels.setdefault(row["category"], row.get("category_label") or "Other")
    categories = {
        key: {"label": category_labels[key], "count": len(ids)}
        for key, ids in category_events.items()
    }

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "city": city,
        "generated_at": meta["generated_at"],
        "window": meta["window"],
        "attribution": meta["attribution"],
        "feed": {"url": f"feed.{feed_hash}.json.gz", "hash": feed_hash, "count": len(feed_rows)},
        "geo": {"url": f"geo.{geo_hash}.json.gz", "hash": geo_hash, "count": len(geo_rows)},
        "days": [{"date": d, "count": len(ids)} for d, ids in sorted(days.items())],
        "categories": dict(sorted(categories.items(), key=lambda kv: -kv[1]["count"])),
        # Superseded event id → the id that replaced it. Optional for readers;
        # additive to the schema, so no version bump.
        "aliases": dict(sorted(alias_map.items())),
    }
    return {
        "manifest": manifest,
        "feed": (manifest["feed"]["url"], feed_rows),
        "geo": (manifest["geo"]["url"], geo_rows),
        "events": events,
    }


def _write_json(path: Path, payload, *, gzipped: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = _canonical_json(payload)
    # mtime=0 so identical content compresses to identical bytes — otherwise the
    # gzip header timestamp would make every publish look like a change.
    path.write_bytes(gzip.compress(raw, mtime=0) if gzipped else raw)


def _remove_superseded(city_dir: Path, built: dict) -> None:
    """Drop hashed objects from earlier exports that this manifest no longer names.

    Content-addressed names never collide, so an old `feed.<hash>` or
    `events/<id>.<hash>` left on disk is harmless to readers — but `publish`
    uploads the whole directory, so on a persistent checkout every superseded
    object would be re-uploaded, counted as current, and never pruned. The
    bucket would only ever grow. Deleting locally is what lets it shrink.
    """
    keep = {built["feed"][0], built["geo"][0], *built["events"]}
    candidates = [*city_dir.glob("feed.*.json.gz"), *city_dir.glob("geo.*.json.gz"),
                  *(city_dir / "events").glob("*.json.gz")]
    for path in candidates:
        if path.relative_to(city_dir).as_posix() not in keep:
            path.unlink()


def export_city(conn, city: str, out_dir: str | Path = "public") -> dict:
    today = date.today()
    date_from = today.isoformat()
    date_to = (today + timedelta(days=cfg.WINDOW_DAYS)).isoformat()
    rows = conn.execute(
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

    # Counted off the raw rows rather than tracked through _row_to_item, so the
    # number means "rows that arrived carrying contact details" — a source that
    # starts or stops publishing them is visible in run telemetry either way.
    redacted = sum(
        1 for r in rows if contains_contact(r["description"]) or contains_contact(r["title"])
    )

    items = [_row_to_item(r) for r in rows]
    by_day: dict[str, list[dict]] = {}
    counts: dict[str, int] = {}
    for item in items:
        by_day.setdefault(item["nightlife_date"], []).append(item)
        counts[item["category"]] = counts.get(item["category"], 0) + 1

    city_dir = Path(out_dir) / city
    city_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "city": city,
        "generated_at": now_iso(),
        "window": {"from": date_from, "to": date_to},
        "total": len(items),
        "by_category": dict(sorted(counts.items(), key=lambda kv: -kv[1])),
        "attribution": "Event data aggregated from public sources; every item links to its original listing.",
    }
    (city_dir / "index.json").write_text(
        json.dumps({"meta": meta, "events": items}, ensure_ascii=False, indent=1)
    )
    for day, day_items in by_day.items():
        (city_dir / f"{day}.json").write_text(
            json.dumps({"meta": {**meta, "day": day, "total": len(day_items)}, "events": day_items},
                       ensure_ascii=False, indent=1)
        )

    # The published feed. Written alongside the demo export so `publish` is a pure
    # upload of a directory and can be re-run without re-querying.
    built = build_feed(conn, city, items, meta)
    _remove_superseded(city_dir, built)
    _write_json(city_dir / built["feed"][0], built["feed"][1], gzipped=True)
    _write_json(city_dir / built["geo"][0], built["geo"][1], gzipped=True)
    for name, payload in built["events"].items():
        _write_json(city_dir / name, payload, gzipped=name.endswith(".gz"))
    # Last, so a reader never sees a manifest naming an object that isn't there yet.
    _write_json(city_dir / "manifest.json", built["manifest"], gzipped=False)

    log.info(
        "exported %d occurrences (%d days) to %s — feed=%d listed, %d events, %d geo, "
        "%d rows had contact details stripped",
        len(items), len(by_day), city_dir,
        built["manifest"]["feed"]["count"], len(built["events"]), built["manifest"]["geo"]["count"],
        redacted,
    )
    return {
        "events": len(items),
        "days": len(by_day),
        "listed": built["manifest"]["feed"]["count"],
        "event_objects": len(built["events"]),
        "feed_hash": built["manifest"]["feed"]["hash"],
        "redacted": redacted,
    }
