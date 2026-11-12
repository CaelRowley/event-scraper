"""Delete events that are over, and everything hanging off them.

Nothing used to remove anything. The export only ever looks at `today .. today +
EXPORT_WINDOW_DAYS`, so a concert from last March stopped being published the
morning after it happened — and then stayed in the database forever, with its
occurrences, its raw snapshot and its alias trail.
The published feed prunes itself; the database it comes from did not.

An event is over when its *last* occurrence is behind the cutoff. Last, not
first: a run of an exhibition is one event with many dates, and the first of
them says nothing about whether it is finished.

What goes with it:

  * `occurrences`, `merged_sources` — meaningless without the event
  * `event_aliases` on either side, since neither a dead alias nor a dead
    canonical resolves to anything
  * `raw_snapshots`, matched on `(source_slug, source_event_id)` because that is
    what it is keyed by rather than the event id
  * `link_checks` / `image_checks` rows for URLs no event references any more

What deliberately stays:

  * `crawl_ledger`. It is keyed by URL and it is what `_discover()` consults to
    decide a sitemap page is new or changed. Dropping a row there does not
    reclaim much and does make the next run re-fetch a page it already knows,
    which is the opposite of the point.
  * `venues`, which are reused across events and cheap to keep.

Safe to delete aggressively now that ids are derived (`ids.py`): if a source
re-reports something we pruned, it comes back under the id it had before.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

log = logging.getLogger(__name__)

# Days of finished events to keep. The feed never shows them; this is only a
# margin against clock skew and against a source that reports a date late.
# 0 prunes everything up to yesterday.
RETAIN_PAST_DAYS = 2


def _chunks(items: list[str], size: int = 400):
    """SQLite caps bound parameters; ids are deleted in batches under that."""
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _delete_events(conn, rows) -> tuple[int, int]:
    """Delete these events and everything hanging off them. Returns
    (occurrences removed, stale link/image check rows removed)."""
    ids = [r["id"] for r in rows]
    sources = [(r["source_slug"], r["source_event_id"]) for r in rows]

    occurrences = 0
    for batch in _chunks(ids):
        marks = ",".join("?" * len(batch))
        occurrences += conn.execute(
            f"DELETE FROM occurrences WHERE event_id IN ({marks})", batch
        ).rowcount
        conn.execute(f"DELETE FROM merged_sources WHERE canonical_id IN ({marks})", batch)
        # Both sides: a pruned event is neither a useful alias nor a useful target.
        conn.execute(f"DELETE FROM event_aliases WHERE canonical_id IN ({marks})", batch)
        conn.execute(f"DELETE FROM event_aliases WHERE alias_id IN ({marks})", batch)
        conn.execute(f"DELETE FROM events WHERE id IN ({marks})", batch)

    for batch in _chunks(sources):
        conn.executemany(
            "DELETE FROM raw_snapshots WHERE source_slug=? AND source_event_id=?", batch
        )

    # URL-keyed ledgers, once nothing points at the URL any more. Both tables are
    # created lazily by linkcheck.py, so a database that has never run a check
    # will not have them.
    checks = 0
    for table, column in (("link_checks", "source_url"), ("image_checks", "image_url")):
        try:
            checks += conn.execute(
                f"DELETE FROM {table} WHERE url NOT IN "
                f"(SELECT {column} FROM events WHERE {column} IS NOT NULL)"
            ).rowcount
        except Exception:  # noqa: BLE001 — table absent on a fresh database
            pass
    return occurrences, checks


def purge_unpermitted(conn, city: str) -> dict:
    """Delete every stored event of a source we are not permitted to use.

    Switching a source off stops new collection, but what it already yielded
    would otherwise sit in the database — and in the feed — until each event
    aged out, weeks later. A source is off because its site said no (or never
    said yes), so its data goes the same day: events, occurrences, raw
    snapshots, and the crawl ledger rows that only that source's adapter reads.

    Runs before dedup, so a cluster that was headed by a purged event re-forms
    around the members that remain.
    """
    from . import sources as policy

    slugs = [r["source_slug"] for r in conn.execute(
        "SELECT DISTINCT source_slug FROM events WHERE city = ?", (city,))]
    gone = [s for s in slugs if not policy.is_permitted(s)]
    removed: dict[str, int] = {}
    for slug in gone:
        rows = conn.execute(
            "SELECT id, source_slug, source_event_id FROM events WHERE city=? AND source_slug=?",
            (city, slug),
        ).fetchall()
        _delete_events(conn, rows)
        conn.execute("DELETE FROM raw_snapshots WHERE source_slug=?", (slug,))
        conn.execute("DELETE FROM crawl_ledger WHERE source_slug=?", (slug,))
        removed[slug] = len(rows)
    if removed:
        # Members whose head just went become their own head until dedup re-clusters.
        conn.execute(
            "UPDATE events SET canonical_id=id WHERE canonical_id NOT IN (SELECT id FROM events)")
        log.warning("policy: purged events of unpermitted sources: %s", removed)
    return {"sources": removed, "events": sum(removed.values())}


def prune_linkless(conn, city: str) -> dict:
    """Delete stored events that have no external link.

    Nothing without one is stored any more (`has_external_link`, applied before
    an event is converted); this removes what earlier runs kept — rows whose
    link was empty, not a URL, or a raw kulturdaten API record.
    """
    from .adapters.base import has_external_link

    rows = [
        r for r in conn.execute(
            "SELECT id, source_slug, source_event_id, source_url FROM events WHERE city=?", (city,))
        if not has_external_link(r["source_url"])
    ]
    if rows:
        _delete_events(conn, rows)
        conn.execute(
            "UPDATE events SET canonical_id=id WHERE canonical_id NOT IN (SELECT id FROM events)")
        log.warning("policy: deleted %d stored event(s) with no external link", len(rows))
    return {"events": len(rows)}


def prune_past(conn, city: str, *, keep_days: int = RETAIN_PAST_DAYS,
               today: date | None = None) -> dict:
    """Remove finished events for `city`. Returns what it deleted."""
    cutoff = ((today or date.today()) - timedelta(days=keep_days)).isoformat()

    # An event with no occurrences at all is already invisible — reconcile drops
    # the rows when a source stops reporting a slot — so it is judged on when it
    # was last seen instead.
    rows = conn.execute(
        """SELECT e.id, e.source_slug, e.source_event_id
             FROM events e
            WHERE e.city = ?
              AND COALESCE(
                    (SELECT MAX(o.nightlife_date) FROM occurrences o WHERE o.event_id = e.id),
                    date(e.last_seen_at)
                  ) < ?""",
        (city, cutoff),
    ).fetchall()
    if not rows:
        log.info("retention: nothing finished before %s", cutoff)
        return {"cutoff": cutoff, "events": 0, "occurrences": 0, "checks": 0}

    occurrences, checks = _delete_events(conn, rows)

    log.info("retention: removed %d events (%d occurrences, %d stale checks) finished before %s",
             len(rows), occurrences, checks, cutoff)
    return {"cutoff": cutoff, "events": len(rows), "occurrences": occurrences, "checks": checks}
