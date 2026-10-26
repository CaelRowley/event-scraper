"""Pipeline orchestration: adapters → normalise → categorise → store → dedup → enrich → export.

Adapters run concurrently, one worker thread per source, each on its own SQLite
connection (WAL). Politeness is unaffected: the shared Fetcher serializes same-domain
requests via per-domain locks, and every source hits a distinct domain anyway.
Invariant for all workers: no write transaction may span an HTTP request — commit
per event/iteration, or the WAL write lock starves every other thread.

After dedup, the enrichment phases (image scan, link and image checks, stock
photos, sanity) are best-effort and incremental: each picks up next run whatever
it skipped. So none of them may cost the run its export — a phase that raises is
logged and reported, and once the run passes ENRICH_DEADLINE the remaining phases
are skipped. A stale feed is the one outcome worse than a slightly less enriched one.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from . import config as cfg
from .categorise.rules import classify_best_effort
from .db import connect, reconcile_occurrences, set_category, snapshot, upsert_event
from .dedup import run_dedup
from .export import export_city
from .retention import prune_past
from .fetch import Fetcher, HostBlockedError
from .images import backfill_images
from .linkcheck import check_images, check_links
from .stock_photos import backfill_stock_photos
from .verify import run_sanity_checks
from .normalise.convert import to_event
from .venues import VenueIndex, seed_venues

log = logging.getLogger(__name__)

# Minutes after the run starts beyond which no further enrichment phase begins.
# The job's hard limit is 75; a full run's scrape takes ~15. Past this point the
# run goes straight to export, so a slow host can delay the feed but not kill it.
ENRICH_DEADLINE_MINUTES = float(os.environ.get("PIPELINE_ENRICH_DEADLINE_MIN", "45"))


@dataclass
class AdapterResult:
    slug: str
    count: int = 0
    tier_counts: Counter = field(default_factory=Counter)
    error: str | None = None
    # The source's host refused us until the circuit breaker gave up on it — a
    # block (needs a different egress IP), not a bug, so it is not an `error`.
    blocked: bool = False


def _run_adapter(slug: str, adapter_cls: type, city: cfg.CityConfig, db_path: str,
                 fetcher: Fetcher, venue_index: VenueIndex,
                 limit: int | None) -> AdapterResult:
    threading.current_thread().name = slug
    res = AdapterResult(slug)
    conn = connect(db_path)
    seen_occurrences: dict[str, set[str]] = {}
    try:
        adapter = adapter_cls(fetcher, conn)  # adapter DDL lands on this thread's conn
        log.info("=== source: %s", slug)
        for raw in adapter.fetch_events(window_days=cfg.SCRAPE_WINDOW_DAYS, limit=limit):
            ev = to_event(raw, city.slug, city.tz)
            if ev is None:
                continue
            ev.venue_id, venue_prior = venue_index.resolve(ev.venue_name)
            cls = classify_best_effort(
                ev,
                source_prior=adapter.category_prior,
                mapped_category=adapter.map_category(raw),
                venue_prior=venue_prior,
            )
            eid, changed = upsert_event(conn, ev)
            seen_occurrences.setdefault(eid, set()).update(
                occ.starts_at_utc for occ in ev.occurrences
            )
            res.count += 1
            if changed:
                snapshot(conn, ev.source, ev.source_event_id,
                         {**raw.payload, "description": (ev.description or "")[:1500]})
            if cls is None:
                res.tier_counts["unclassified"] += 1  # zero signal — stays "other"
            else:
                current_tier = conn.execute(
                    "SELECT category_tier FROM events WHERE id=?", (eid,)
                ).fetchone()["category_tier"]
                # Verdicts from the retired AI tier beat a rules guess; keep them
                # until the event itself changes (retention clears them within weeks).
                if changed or current_tier != "llm":
                    set_category(conn, eid, cls.category, cls.tier, cls.confidence, cls.tags)
                    res.tier_counts[cls.tier] += 1
            conn.commit()  # per event — see module invariant
        # A source refresh is authoritative for every event it returned. Reconcile
        # only after clean completion: a mid-stream adapter failure must never
        # delete slots that simply were not reached. Limited smoke runs are also
        # partial by definition, so they remain upsert-only.
        if limit is None:
            for eid, starts in seen_occurrences.items():
                reconcile_occurrences(conn, eid, starts)
                conn.commit()
        conn.commit()  # sitemap adapters write their ledger after the final yield
    except HostBlockedError as exc:
        # Before the generic handler: what it yielded before the trip is kept
        # (committed per event); reconcile is skipped, as for any partial run.
        conn.rollback()
        res.blocked = True
        log.warning("source %s blocked: %s", slug, exc)
    except Exception as exc:  # noqa: BLE001 — one dead source never blocks the run
        conn.rollback()  # release any held write lock before reporting
        res.error = f"{type(exc).__name__}: {exc}"
        log.error("source %s failed: %s", slug, exc, exc_info=True)
    finally:
        conn.close()
    return res


def _phase(name: str, fn, conn, *, deadline: float, phase_errors: dict) -> dict:
    """Run one enrichment phase without letting it take the export down with it."""
    if time.monotonic() > deadline:
        log.warning("phase %s skipped: past the %.0f-minute enrichment deadline",
                    name, ENRICH_DEADLINE_MINUTES)
        phase_errors[name] = "skipped: deadline"
        return {"skipped": "deadline"}
    started = time.monotonic()
    try:
        stats = fn()
        conn.commit()
    except Exception as exc:  # noqa: BLE001 — enrichment is best-effort, the export is not
        conn.rollback()
        log.error("phase %s failed: %s", name, exc, exc_info=True)
        phase_errors[name] = f"{type(exc).__name__}: {exc}"
        return {"error": phase_errors[name]}
    stats = dict(stats or {})
    stats["seconds"] = round(time.monotonic() - started, 1)
    return stats


def run(city_slug: str = "berlin", *, mode: str = "full", only: list[str] | None = None,
        limit: int | None = None, db_path: str | None = None) -> dict:
    started = time.monotonic()
    deadline = started + ENRICH_DEADLINE_MINUTES * 60
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
    errors: dict[str, str] = {}
    tripped_sources: set[str] = set()
    phase_errors: dict[str, str] = {}

    max_workers = min(len(tasks) or 1, int(os.environ.get("PIPELINE_MAX_WORKERS", "12")))
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="adapter") as ex:
        futures = [
            ex.submit(_run_adapter, slug, acls, city, main_db, fetcher, venue_index, limit)
            for slug, acls in tasks
        ]
        for fut in as_completed(futures):
            r = fut.result()  # never raises — the worker contains all adapter errors
            # Recorded even at zero. Leaving zero-yield sources out of the dict is
            # how four blocked sources stayed invisible in a green run.
            source_counts[r.slug] = r.count
            tier_counts.update(r.tier_counts)
            if r.error:
                errors[r.slug] = r.error
            if r.blocked:
                tripped_sources.add(r.slug)
    scrape_seconds = round(time.monotonic() - started, 1)

    dedup_stats = run_dedup(conn, city.slug)
    conn.commit()

    def phase(name, fn):
        return _phase(name, fn, conn, deadline=deadline, phase_errors=phase_errors)

    # Each phase writes to SQLite from this thread only. Their HTTP fans out across
    # hosts inside the phase (probe.py), which is where the time was; running the
    # phases themselves concurrently produced intermittent WAL lock failures in CI.
    image_stats = phase("images", lambda: backfill_images(conn, fetcher, city.slug))
    link_stats = phase("links", lambda: check_links(conn, fetcher, city.slug))
    image_check_stats = phase("image_checks", lambda: check_images(conn, fetcher, city.slug))
    # Last of the image phases, deliberately: it fills only what both the source
    # and the page scan failed to provide, and `check_images` above can null an
    # image_url it finds dead — which creates work for this.
    stock_stats = phase("stock_photos",
                        lambda: backfill_stock_photos(conn, city.slug, fetcher=fetcher))
    sanity_stats = phase("sanity", lambda: run_sanity_checks(conn, city.slug))

    export_stats = export_city(conn, city.slug, cfg.PUBLIC_DIR)

    # After the export, deliberately: the window is today-forward, so nothing
    # pruned here could have belonged in the feed we just wrote — and if this
    # ever gets its cutoff wrong, it cannot take the feed down with it. No
    # deadline: it is a few local DELETEs, and skipping it lets the DB grow.
    retention_stats = _phase("retention", lambda: prune_past(conn, city.slug), conn,
                             deadline=float("inf"), phase_errors=phase_errors)

    # A source can fail without raising: adapters log an HTTP block and yield
    # nothing, so `errors` stays empty and the run looks healthy. Zero yield is not
    # the signal either — sitemap sources legitimately return nothing on a day with
    # no new pages. What is unambiguous is a source whose every request was refused.
    source_http = {
        slug: counts for slug, counts in fetcher.source_http().items()
        if slug in {s for s, _ in tasks}
    }
    # Also a source whose host tripped the breaker mid-run: tip-berlin serves its
    # sitemaps fine and 403s every event page, so it never looks all-refused.
    blocked_sources = sorted(tripped_sources | {
        slug for slug, counts in source_http.items()
        if counts.get("blocked", 0) and not counts.get("ok", 0)
    })
    if blocked_sources:
        log.error("sources refused every request: %s", ", ".join(blocked_sources))

    telemetry = {
        "sources": dict(source_counts),
        "errors": errors,
        "phase_errors": phase_errors,
        "source_http": source_http,
        "blocked_sources": blocked_sources,
        "tripped_hosts": fetcher.tripped_hosts(),
        "category_tiers": dict(tier_counts),
        "dedup": dedup_stats,
        "images": image_stats,
        "stock_photos": stock_stats,
        "links": link_stats,
        "image_checks": image_check_stats,
        "sanity": sanity_stats,
        "export": export_stats,
        "retention": retention_stats,
        "http_requests": fetcher.requests_made,
        "seconds": {"scrape": scrape_seconds, "total": round(time.monotonic() - started, 1)},
    }
    log.info("run telemetry: %s", telemetry)
    fetcher.close()
    conn.close()
    return telemetry
