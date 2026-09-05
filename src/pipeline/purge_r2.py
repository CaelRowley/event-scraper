"""One-off purge of the pre-v2 detail objects left in the feed bucket.

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

import logging
import os

from .publish_r2 import _list_existing, client

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
