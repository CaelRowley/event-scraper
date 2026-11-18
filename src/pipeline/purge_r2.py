"""Manual purges of the feed bucket: the pre-v2 generation, and everything de-listed.

Two commands, both dry runs unless `--apply` is passed.

`purge-delisted` deletes, now, every object the published manifest does not
name. `publish` does the same on its own, 48 hours after an object is de-listed,
because a client holding the previous manifest may still ask for it. Use this
when that wait is the wrong trade: content that must not stay downloadable
(a source that was switched off, text or photos that are no longer published),
and no client population that the 404s would hurt.

    python -m pipeline purge-delisted --city berlin           # count what would go
    python -m pipeline purge-delisted --city berlin --apply   # delete it

`purge-legacy` is the older, narrower one:

One-off purge of the pre-v2 detail objects left in the feed bucket.

`publish_r2` normally retires an object on its own: anything the manifest stops
naming is stamped in a ledger and deleted once it has been de-listed for
GRACE_HOURS. That mechanism cannot retire the v1 generation, for a reason worth
spelling out.

The v2 cutover renames every detail object (`.json` → `.json.gz`), so the first
v2 publish *adds* ~19,400 objects beside the ~20,800 already there rather than
replacing them. On the next run `_unsafe_to_prune` compares the local export
against the bucket, sees it holding under half of what is there, reads that as a
broken scrape, and refuses. The grace clock never advances and the v1 objects
stay forever — while carrying the source prose from before `redact.py` existed,
in a bucket that is world-readable.

That guard is right to fire; a run that legitimately halved the feed would be a
disaster. So the v1 generation is retired here instead, once, by hand.

Safe by construction: the two generations are told apart by suffix, and this only
ever deletes keys ending `.json` under `<city>/events/`. A v2 object cannot match.
It also refuses to run unless a v2 generation is actually present, so it cannot
empty a bucket that has not been migrated yet.

Dry run unless `--apply` is passed:

    python -m pipeline purge-legacy --city berlin           # list what would go
    python -m pipeline purge-legacy --city berlin --apply   # delete it
"""

from __future__ import annotations

import gzip
import json
import logging
import os

from .publish_r2 import _ledger_key, _list_existing, _load_ledger, _save_ledger, client

log = logging.getLogger(__name__)

DELETE_BATCH = 1000


def _split_generations(keys) -> tuple[list[str], list[str]]:
    """(v1, v2) detail-object keys. Suffix is the only thing that separates them."""
    prefix_marker = "/events/"
    v1 = [k for k in keys if prefix_marker in k and k.endswith(".json")]
    v2 = [k for k in keys if prefix_marker in k and k.endswith(".json.gz")]
    return sorted(v1), sorted(v2)


def purge_legacy(city: str, *, apply: bool = False) -> dict:
    """Delete `<city>/events/*.json`, the uncompressed pre-v2 generation."""
    s3 = client()
    if s3 is None:
        log.warning("R2 credentials not set — nothing to do")
        return {"skipped": True, "reason": "no-credentials"}
    bucket = os.getenv("R2_BUCKET")
    if not bucket:
        log.warning("R2_BUCKET not set — nothing to do")
        return {"skipped": True, "reason": "no-bucket"}

    keys = _list_existing(s3, bucket, f"{city}/")
    v1, v2 = _split_generations(keys)

    if not v1:
        log.info("no legacy objects under %s/events/ — nothing to purge", city)
        return {"legacy": 0, "current": len(v2), "deleted": 0}

    # The whole point is to remove the *superseded* generation. If the new one
    # isn't there, this would just be deleting the live feed.
    if not v2:
        log.error("refusing to purge: %d legacy objects but no .json.gz generation — "
                  "publish the v2 feed first", len(v1))
        return {"legacy": len(v1), "current": 0, "deleted": 0,
                "refused": "no v2 generation present"}

    log.info("%s: %d legacy (.json) vs %d current (.json.gz)", city, len(v1), len(v2))
    if not apply:
        for key in v1[:10]:
            log.info("  would delete %s", key)
        if len(v1) > 10:
            log.info("  ... and %d more", len(v1) - 10)
        log.info("dry run — pass --apply to delete")
        return {"legacy": len(v1), "current": len(v2), "deleted": 0, "dry_run": True}

    deleted = 0
    for i in range(0, len(v1), DELETE_BATCH):
        chunk = v1[i:i + DELETE_BATCH]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]})
        deleted += len(chunk)
        log.info("  deleted %d/%d", deleted, len(v1))

    log.info("purged %d legacy objects from %s", deleted, bucket)
    return {"legacy": len(v1), "current": len(v2), "deleted": deleted}


def _named_by_manifest(s3, bucket: str, city: str) -> set[str]:
    """Every key the manifest currently in the bucket leads to. Raises if it cannot be read."""
    manifest = json.loads(s3.get_object(Bucket=bucket, Key=f"{city}/manifest.json")["Body"].read())
    feed_key = f"{city}/{manifest['feed']['url']}"
    rows = json.loads(gzip.decompress(s3.get_object(Bucket=bucket, Key=feed_key)["Body"].read()))
    if len(rows) != manifest["feed"]["count"]:
        raise ValueError(f"feed holds {len(rows)} rows but the manifest says "
                         f"{manifest['feed']['count']}")
    named = {f"{city}/manifest.json", feed_key}
    if manifest.get("geo", {}).get("url"):
        named.add(f"{city}/{manifest['geo']['url']}")
    named |= {f"{city}/events/{r['event_id']}.{r['h']}.json.gz" for r in rows}
    return named


def purge_delisted(city: str, *, apply: bool = False) -> dict:
    """Delete every object under `<city>/` that the published manifest does not name.

    The live feed is worked out from the bucket itself — manifest, then the feed
    object it names, then one detail object per feed row — never from a local
    export, so a stale or partial checkout cannot widen what is deleted. It
    refuses outright when that picture is not whole: an unreadable manifest or
    feed, a row count that disagrees with the manifest, an empty feed, or a
    named object that is missing from the bucket.
    """
    s3 = client()
    if s3 is None:
        log.warning("R2 credentials not set — nothing to do")
        return {"skipped": True, "reason": "no-credentials"}
    bucket = os.getenv("R2_BUCKET")
    if not bucket:
        log.warning("R2_BUCKET not set — nothing to do")
        return {"skipped": True, "reason": "no-bucket"}

    try:
        named = _named_by_manifest(s3, bucket, city)
    except Exception as exc:  # noqa: BLE001 — any doubt about the live set means no deletion
        log.error("refusing to purge: cannot establish what the manifest names (%s)", exc)
        return {"deleted": 0, "refused": f"manifest or feed unreadable: {exc}"}

    ledger_key = _ledger_key(city)
    existing = set(_list_existing(s3, bucket, f"{city}/")) - {ledger_key}
    missing = named - existing
    if missing:
        log.error("refusing to purge: %d object(s) the manifest names are not in the bucket",
                  len(missing))
        return {"deleted": 0, "live": len(named), "refused":
                f"{len(missing)} named object(s) missing from the bucket"}
    if len(named) <= 3:
        log.error("refusing to purge: the published feed is empty")
        return {"deleted": 0, "live": len(named), "refused": "the published feed is empty"}

    doomed = sorted(existing - named)
    kinds = {
        "event_objects": sum(1 for k in doomed if "/events/" in k and k.endswith(".json.gz")),
        "legacy_event_objects": sum(1 for k in doomed if "/events/" in k and k.endswith(".json")),
        "feeds": sum(1 for k in doomed if k.startswith(f"{city}/feed.")),
        "geo": sum(1 for k in doomed if k.startswith(f"{city}/geo.")),
    }
    kinds["other"] = len(doomed) - sum(kinds.values())
    log.info("%s: %d live objects, %d de-listed %s", city, len(named), len(doomed), kinds)

    if not apply:
        for key in doomed[:10]:
            log.info("  would delete %s", key)
        if len(doomed) > 10:
            log.info("  … and %d more", len(doomed) - 10)
        log.info("dry run — pass --apply to delete")
        return {"live": len(named), "delisted": len(doomed), "kinds": kinds,
                "deleted": 0, "dry_run": True}

    deleted = 0
    for start in range(0, len(doomed), DELETE_BATCH):
        batch = doomed[start:start + DELETE_BATCH]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})
        deleted += len(batch)
    # Nothing de-listed is left, so nothing is waiting out a grace window either.
    gone = set(doomed)
    _save_ledger(s3, bucket, city,
                 {k: v for k, v in _load_ledger(s3, bucket, city).items() if k not in gone})
    log.info("%s: deleted %d de-listed objects, %d live objects kept", city, deleted, len(named))
    return {"live": len(named), "delisted": len(doomed), "kinds": kinds, "deleted": deleted}
