"""Pipeline orchestration: adapters → normalise → store → dedup → categorise → LLM → export.

Adapters run concurrently, one worker thread per source, each on its own SQLite
connection (WAL). Politeness is unaffected: the shared Fetcher serializes same-domain
requests via per-domain locks, and every source hits a distinct domain anyway.
Invariant for all workers: no write transaction may span an HTTP request — commit
per event/iteration, or the WAL write lock starves every other thread.
"""

from __future__ import annotations

import logging
import os
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

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


@dataclass
class AdapterResult:
    slug: str
    count: int = 0
    tier_counts: Counter = field(default_factory=Counter)
    descriptions: dict[str, str] = field(default_factory=dict)
    error: str | None = None


def _run_adapter(slug: str, adapter_cls: type, city: cfg.CityConfig, db_path: str,
                 fetcher: Fetcher, venue_index: VenueIndex, ai_enabled: bool,
                 limit: int | None) -> AdapterResult:
    threading.current_thread().name = slug
    res = AdapterResult(slug)
    conn = connect(db_path)
    try:
        adapter = adapter_cls(fetcher, conn)  # adapter DDL lands on this thread's conn
        log.info("=== source: %s", slug)
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
            res.count += 1
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
                    res.tier_counts[cls.tier] += 1
                if cls.tier == "keyword_multi" and current_tier != "llm":
                    # best-effort guess — queue so the AI tier upgrades it later
                    queue_for_llm(conn, eid)
                    if ev.description:
                        res.descriptions[eid] = ev.description
            elif changed or current_tier in ("fallback_other", "keyword_multi"):
                queue_for_llm(conn, eid)
                if ev.description:
                    res.descriptions[eid] = ev.description
                res.tier_counts["queued_llm"] += 1
            conn.commit()  # per event — see module invariant
        conn.commit()  # sitemap adapters write their ledger after the final yield
    except Exception as exc:  # noqa: BLE001 — one dead source never blocks the run
        conn.rollback()  # release any held write lock before reporting
        res.error = f"{type(exc).__name__}: {exc}"
        log.error("source %s failed: %s", slug, exc, exc_info=True)
    finally:
        conn.close()
    return res


def run(city_slug: str = "berlin", *, mode: str = "full", only: list[str] | None = None,
        limit: int | None = None, no_llm: bool = False, llm_poll: int = 1200,
        db_path: str | None = None) -> dict:
    # Two categorisation modes: "ai" when a key is present (and not opted out),
    # "rules" otherwise. Rules mode classifies as best it can (keyword_multi tier)
    # and still queues those events, so adding ANTHROPIC_API_KEY later upgrades them.
    ai_enabled = not no_llm and bool(os.environ.get("ANTHROPIC_API_KEY"))
    city = cfg.CITIES[city_slug]
    main_db = str(db_path or cfg.DB_PATH)
    conn = connect(main_db)
    seed_venues(conn)
    conn.commit()  # release the seed write tx, or every worker's first write stalls
    fetcher = Fetcher(cache_path=cfg.HTTP_CACHE_PATH)
    venue_index = VenueIndex(conn, city.slug)  # read-only in-memory — shared across workers
    tasks = cfg.select_adapters(city, mode=mode, only=only)

    tier_counts: Counter = Counter()
    source_counts: Counter = Counter()
    descriptions: dict[str, str] = {}
    errors: dict[str, str] = {}

    max_workers = min(len(tasks) or 1, int(os.environ.get("PIPELINE_MAX_WORKERS", "12")))
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="adapter") as ex:
        futures = [
            ex.submit(_run_adapter, slug, acls, city, main_db,
                      fetcher, venue_index, ai_enabled, limit)
            for slug, acls in tasks
        ]
        for fut in as_completed(futures):
            r = fut.result()  # never raises — the worker contains all adapter errors
            if r.count:
                source_counts[r.slug] = r.count
            tier_counts.update(r.tier_counts)
            descriptions.update(r.descriptions)
            if r.error:
                errors[r.slug] = r.error

    dedup_stats = run_dedup(conn, city.slug)
    conn.commit()

    # The three check phases probe disjoint URL sets and touch disjoint rows (by
    # predicate), so they overlap safely — same-host collisions serialize in the
    # Fetcher's domain locks. Each runs on its own connection.
    def _phase(fn):
        pconn = connect(main_db)
        try:
            return fn(pconn, fetcher, city.slug)
        finally:
            pconn.commit()
            pconn.close()

    with ThreadPoolExecutor(max_workers=3, thread_name_prefix="post") as ex:
        f_img = ex.submit(_phase, backfill_images)
        f_link = ex.submit(_phase, check_links)
        f_ic = ex.submit(_phase, check_images)
        image_stats = f_img.result()
        link_stats = f_link.result()
        image_check_stats = f_ic.result()

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
