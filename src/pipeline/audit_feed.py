"""Audit what is actually in the bucket, rather than what a run said it did.

A green workflow proves the scrape yielded events and the upload returned 200. It
does not prove the redaction fired, that the published shape is the one this
version writes, or that no contact detail survived into an object — the freshness
gate only counts events. Those are properties of the artefact, so they are
checked against the artefact.

Reads the manifest, the feed rows, and the detail objects, and reports:

  * the published `schema_version` and whether detail objects are gzipped,
  * how many objects of each kind are there,
  * every email address or phone number still reachable in a published object.

The last one is the point. It uses the same `contains_contact` the export gate
uses, so a disagreement between them is a bug in one or the other, not a matter
of opinion.

    python -m pipeline audit-feed --city berlin              # 500-object sample
    python -m pipeline audit-feed --city berlin --all        # every object

Needs the same R2 credentials as `publish`.
"""

from __future__ import annotations

import gzip
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from itertools import islice

from .publish_r2 import MAX_WORKERS, _list_existing, client
from .redact import contains_contact

log = logging.getLogger(__name__)

DEFAULT_SAMPLE = 500


def _get(s3, bucket: str, key: str) -> bytes:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    # R2 hands back exactly the bytes that were uploaded — Content-Encoding is a
    # promise to the browser, not something boto unwraps.
    return gzip.decompress(body) if key.endswith(".gz") else body


def _contacts_in(payload: dict) -> list[str]:
    """Fields of a published object that carry free text a person could be in."""
    found = []
    for field in ("title", "description"):
        value = payload.get(field)
        if isinstance(value, str) and contains_contact(value):
            found.append(field)
    return found


def audit(city: str, *, sample: int | None = DEFAULT_SAMPLE) -> dict:
    s3 = client()
    if s3 is None:
        log.error("R2 credentials not set")
        return {"skipped": True, "reason": "no-credentials"}
    import os

    bucket = os.getenv("R2_BUCKET")
    if not bucket:
        log.error("R2_BUCKET not set")
        return {"skipped": True, "reason": "no-bucket"}

    manifest = json.loads(_get(s3, bucket, f"{city}/manifest.json"))
    rows = json.loads(_get(s3, bucket, f"{city}/{manifest['feed']['url']}"))

    keys = _list_existing(s3, bucket, f"{city}/")
    detail = [k for k in keys if "/events/" in k]
    gzipped = [k for k in detail if k.endswith(".json.gz")]
    legacy = [k for k in detail if k.endswith(".json")]

    # Titles ride the list row, so they are checked from the feed in one read
    # rather than by opening every object.
    dirty_titles = [r["id"] for r in rows
                    if isinstance(r.get("title"), str) and contains_contact(r["title"])]

    targets = gzipped if sample is None else list(islice(iter(gzipped), sample))
    dirty_objects: list[dict] = []

    def check(key: str):
        try:
            payload = json.loads(_get(s3, bucket, key))
        except Exception as exc:                      # noqa: BLE001 — report, don't abort
            return {"key": key, "error": type(exc).__name__}
        hits = _contacts_in(payload)
        return {"key": key, "fields": hits} if hits else None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="audit") as pool:
        for result in pool.map(check, targets):
            if result:
                dirty_objects.append(result)

    report = {
        "schema_version": manifest.get("schema_version"),
        "generated_at": manifest.get("generated_at"),
        "feed_rows": len(rows),
        "feed_count_claimed": manifest["feed"]["count"],
        "detail_objects": len(gzipped),
        "legacy_uncompressed": len(legacy),
        "objects_checked": len(targets),
        "rows_with_contact_in_title": len(dirty_titles),
        "objects_with_contact": len(dirty_objects),
        "examples": dirty_objects[:5],
    }

    log.info("schema_version=%s  feed rows=%d  detail objects=%d  legacy=%d",
             report["schema_version"], report["feed_rows"],
             report["detail_objects"], report["legacy_uncompressed"])
    if legacy:
        log.warning("%d pre-v2 uncompressed objects still present — run purge-legacy",
                    len(legacy))
    if dirty_titles or dirty_objects:
        log.error("CONTACT DETAILS FOUND: %d titles, %d objects (of %d checked)",
                  len(dirty_titles), len(dirty_objects), len(targets))
    else:
        log.info("no contact details in %d checked objects or %d feed rows",
                 len(targets), len(rows))
    return report
