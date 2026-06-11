"""Pipeline orchestration: adapters → normalise → store → dedup → categorise → LLM → export."""

from __future__ import annotations

import logging
from collections import Counter

import os

from . import config as cfg
from .categorise.llm import run_llm_tier
from .categorise.rules import classify, classify_best_effort
from .db import connect, queue_for_llm, set_category, snapshot, upsert_event
from .dedup import run_dedup
from .export import export_city
from .fetch import Fetcher
from .images import backfill_images
from .linkcheck import check_images, check_links
from .verify import run_sanity_checks
from .normalise.convert import to_event
from .venues import VenueIndex, seed_venues

log = logging.getLogger(__name__)


def run(city_slug: str = "berlin", *, mode: str = "full", only: list[str] | None = None,
        limit: int | None = None, no_llm: bool = False, llm_poll: int = 1200,
        db_path: str | None = None) -> dict:
    # Two categorisation modes: "ai" when a key is present (and not opted out),
    # "rules" otherwise. Rules mode classifies as best it can (keyword_multi tier)
    # and still queues those events, so adding ANTHROPIC_API_KEY later upgrades them.
    ai_enabled = not no_llm and bool(os.environ.get("ANTHROPIC_API_KEY"))
    city = cfg.CITIES[city_slug]
    conn = connect(db_path or cfg.DB_PATH)
    seed_venues(conn)
    fetcher = Fetcher(cache_path=cfg.HTTP_CACHE_PATH)
    venue_index = VenueIndex(conn, city.slug)
    adapters = cfg.build_adapters(city, fetcher, conn, mode=mode, only=only)

    tier_counts: Counter = Counter()
    source_counts: Counter = Counter()
    descriptions: dict[str, str] = {}
    errors: dict[str, str] = {}

    for adapter in adapters:
        log.info("=== source: %s", adapter.slug)
        try:
            for raw in adapter.fetch_events(window_days=cfg.WINDOW_DAYS, limit=limit):
                ev = to_event(raw, city.slug, city.tz)
                if ev is None:
                    continue
                ev.venue_id, venue_prior = venue_index.resolve(ev.venue_name)
                classifier = classify if ai_enabled else classify_best_effort
                cls = classifier(
                    ev,
                    source_prior=adapter.category_prior,
                    mapped_category=adapter.map_category(raw),
                    venue_prior=venue_prior,
                )
                eid, changed = upsert_event(conn, ev)
                source_counts[adapter.slug] += 1
                if changed:
                    snapshot(conn, ev.source, ev.source_event_id,
                             {**raw.payload, "description": (ev.description or "")[:1500]})
                current_tier = conn.execute(
                    "SELECT category_tier FROM events WHERE id=?", (eid,)
                ).fetchone()["category_tier"]
                if cls is not None:
                    # never clobber an LLM verdict on an unchanged event
                    if changed or current_tier != "llm":
                        set_category(conn, eid, cls.category, cls.tier, cls.confidence, cls.tags)
                        tier_counts[cls.tier] += 1
                    if cls.tier == "keyword_multi" and current_tier != "llm":
                        # best-effort guess — queue so the AI tier upgrades it later
                        queue_for_llm(conn, eid)
                        if ev.description:
                            descriptions[eid] = ev.description
                elif changed or current_tier in ("fallback_other", "keyword_multi"):
                    queue_for_llm(conn, eid)
                    if ev.description:
                        descriptions[eid] = ev.description
                    tier_counts["queued_llm"] += 1
            conn.commit()
        except Exception as exc:  # noqa: BLE001 — one dead source never blocks the run
            conn.commit()
            errors[adapter.slug] = f"{type(exc).__name__}: {exc}"
            log.error("source %s failed: %s", adapter.slug, exc, exc_info=True)

    dedup_stats = run_dedup(conn, city.slug)
    conn.commit()

    image_stats = backfill_images(conn, fetcher, city.slug)
    conn.commit()

    link_stats = check_links(conn, fetcher, city.slug)
    image_check_stats = check_images(conn, fetcher, city.slug)
    sanity_stats = run_sanity_checks(conn, city.slug)
    conn.commit()

    llm_stats = {}
    if ai_enabled:
        llm_stats = run_llm_tier(conn, descriptions, poll_seconds=llm_poll)
        conn.commit()

    export_stats = export_city(conn, city.slug, cfg.PUBLIC_DIR)

    queue_size = conn.execute("SELECT COUNT(*) AS n FROM llm_queue").fetchone()["n"]
    telemetry = {
        "categorisation_mode": "ai" if ai_enabled else "rules",
        "sources": dict(source_counts),
        "errors": errors,
        "category_tiers": dict(tier_counts),
        "dedup": dedup_stats,
        "images": image_stats,
        "links": link_stats,
        "image_checks": image_check_stats,
        "sanity": sanity_stats,
        "llm": llm_stats,
        "llm_queue_remaining": queue_size,
        "export": export_stats,
        "http_requests": fetcher.requests_made,
    }
    log.info("run telemetry: %s", telemetry)
    fetcher.close()
    conn.close()
    return telemetry
