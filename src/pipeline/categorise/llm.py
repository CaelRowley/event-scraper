"""Tier 3: nightly Claude Haiku classification via the Message Batches API (50% off).

~400 input + 30 output tokens per event ⇒ ~$0.28 per 1,000 events batched.
Strict JSON-schema structured output — no response parsing heuristics.
Degrades gracefully: anything unresolved stays queued or falls back to "other".
"""

from __future__ import annotations

import json
import logging
import time

from .taxonomy import CATEGORIES, TAGS

log = logging.getLogger(__name__)

MODEL = "claude-haiku-4-5"
CONFIDENCE_FLOOR = 0.6

SYSTEM = (
    "You classify Berlin event listings into exactly one category. Categories: "
    "live_music (concerts, gigs), club_nightlife (parties, raves, DJ sets), "
    "theatre_performance (theatre, dance, opera, performance art), comedy (stand-up, Kabarett, impro), "
    "art_exhibitions (Ausstellungen, Vernissagen, galleries), film_cinema (Kino, screenings), "
    "talks_literature (Lesungen, Vorträge, panels, poetry slams), workshops_classes (Workshops, Kurse), "
    "markets_fairs (Flohmärkte, Messen, Designmärkte), food_drink (tastings, Street Food, dinners), "
    "festivals (multi-act festivals, Festspiele), sports_fitness, family_kids (Kinder/Familien), "
    "community_causes (Meetups, Demos, Kiezfeste, networking, religious), other (none fits).\n"
    "Examples: 'Lange Nacht der Bilder – Atelierrundgang Lichtenberg' → art_exhibitions. "
    "'Kiezkneipenquiz im Familiengarten' → community_causes. "
    "'Soli-Tresen für die Seenotrettung' → community_causes.\n"
    "Tags (zero or more, only when clearly supported): free-entry, open-air, family-friendly, queer."
)

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {"type": "string", "enum": CATEGORIES},
        "confidence": {"type": "number"},
        "tags": {"type": "array", "items": {"type": "string", "enum": TAGS}},
    },
    "required": ["category", "confidence", "tags"],
    "additionalProperties": False,
}


def _kv_get(conn, key: str) -> str | None:
    conn.execute("CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, value TEXT)")
    row = conn.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def _kv_set(conn, key: str, value: str | None) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, value TEXT)")
    if value is None:
        conn.execute("DELETE FROM kv WHERE key=?", (key,))
    else:
        conn.execute("INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))


def _pending(conn, descriptions: dict[str, str]) -> list[dict]:
    rows = conn.execute(
        """SELECT e.id, e.title, e.venue_name, e.source_slug, e.source_category_raw,
                  (SELECT payload_json FROM raw_snapshots s
                   WHERE s.source_slug=e.source_slug AND s.source_event_id=e.source_event_id
                   ORDER BY s.id DESC LIMIT 1) AS payload
           FROM llm_queue q JOIN events e ON e.id = q.event_id"""
    ).fetchall()
    items = []
    for r in rows:
        desc = descriptions.get(r["id"])
        if not desc and r["payload"]:
            try:
                payload = json.loads(r["payload"])
                desc = str(payload.get("description") or "")[:1500]
            except json.JSONDecodeError:
                desc = ""
        items.append({
            "id": r["id"], "title": r["title"], "venue": r["venue_name"] or "?",
            "source": r["source_slug"], "raw_category": r["source_category_raw"] or "",
            "description": (desc or "")[:1500],
        })
    return items


def _apply_results(conn, client, batch_id: str) -> int:
    from . import rules  # noqa: F401  (kept for symmetry; results are authoritative here)
    from ..db import set_category

    applied = 0
    for result in client.messages.batches.results(batch_id):
        if result.result.type != "succeeded":
            log.warning("batch item %s: %s", result.custom_id, result.result.type)
            continue
        msg = result.result.message
        text = next((b.text for b in msg.content if b.type == "text"), "")
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            log.warning("batch item %s: unparseable output", result.custom_id)
            continue
        category = data.get("category", "other")
        confidence = float(data.get("confidence", 0))
        if category not in CATEGORIES or confidence < CONFIDENCE_FLOOR:
            category, confidence = "other", confidence
        tags = [t for t in data.get("tags", []) if t in TAGS]
        set_category(conn, result.custom_id, category, "llm", confidence, tags)
        conn.execute("DELETE FROM llm_queue WHERE event_id=?", (result.custom_id,))
        applied += 1
    return applied


def run_llm_tier(conn, descriptions: dict[str, str], *, poll_seconds: int = 1200) -> dict:
    """Collect any previous batch, then submit + poll a new one for the current queue.

    Returns telemetry. No ANTHROPIC_API_KEY or empty queue → no-op (pipeline degrades
    to category=other via the queue's natural fallback at export time).
    """
    import os

    stats = {"collected": 0, "submitted": 0, "applied": 0}
    if not os.environ.get("ANTHROPIC_API_KEY"):
        log.info("LLM tier skipped: no ANTHROPIC_API_KEY")
        return stats

    import anthropic

    client = anthropic.Anthropic()

    # 1. Collect a batch left over from a previous run, if any.
    prev = _kv_get(conn, "pending_batch_id")
    if prev:
        try:
            batch = client.messages.batches.retrieve(prev)
            if batch.processing_status == "ended":
                stats["collected"] = _apply_results(conn, client, prev)
                _kv_set(conn, "pending_batch_id", None)
            else:
                log.info("previous batch %s still %s — leaving queued", prev, batch.processing_status)
                return stats
        except anthropic.NotFoundError:
            _kv_set(conn, "pending_batch_id", None)

    items = _pending(conn, descriptions)
    if not items:
        return stats

    requests = [
        {
            "custom_id": item["id"],
            "params": {
                "model": MODEL,
                "max_tokens": 200,
                "system": SYSTEM,
                "messages": [{
                    "role": "user",
                    "content": (
                        f"Title: {item['title']}\nVenue: {item['venue']}\n"
                        f"Source: {item['source']} (raw category: {item['raw_category']})\n"
                        f"Description: {item['description']}"
                    ),
                }],
                "output_config": {"format": {"type": "json_schema", "schema": OUTPUT_SCHEMA}},
            },
        }
        for item in items
    ]
    batch = client.messages.batches.create(requests=requests)
    _kv_set(conn, "pending_batch_id", batch.id)
    stats["submitted"] = len(requests)
    log.info("submitted batch %s with %d events", batch.id, len(requests))

    deadline = time.time() + poll_seconds
    while time.time() < deadline:
        time.sleep(20)
        batch = client.messages.batches.retrieve(batch.id)
        if batch.processing_status == "ended":
            stats["applied"] = _apply_results(conn, client, batch.id)
            _kv_set(conn, "pending_batch_id", None)
            return stats
    log.info("batch %s not finished within %ds — will collect next run", batch.id, poll_seconds)
    return stats
