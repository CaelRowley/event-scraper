"""Static JSON export: public/<city>/index.json (next 14 days) + per-day slices."""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path

from .categorise.taxonomy import LABELS
from .db import now_iso

log = logging.getLogger(__name__)

WINDOW_DAYS = 14


def _row_to_item(row) -> dict:
    return {
        "id": row["id"],
        "title": row["title"],
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
        "image_url": row["image_url"],
        "description": row["description"],  # open-licensed sources only (CC-BY)
        "source": row["source_slug"],
        "source_url": row["source_url"],
        # true when the link is a raw data record, not a human page — UIs should label it
        "source_is_record": row["source_url"].startswith("https://api-v2.kulturdaten.berlin"),
    }


def export_city(conn, city: str, out_dir: str | Path = "public") -> dict:
    today = date.today()
    date_from = today.isoformat()
    date_to = (today + timedelta(days=WINDOW_DAYS)).isoformat()
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
    log.info("exported %d events (%d days) to %s", len(items), len(by_day), city_dir)
    return {"events": len(items), "days": len(by_day)}
