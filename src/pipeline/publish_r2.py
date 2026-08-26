"""Publish the exported feed to Cloudflare R2 — the objects Gobento serves from.

This replaces the old D1 sync. The feed is one document, identical for every
visitor, so it is published once here and read straight from object storage;
the database is left holding only per-user state. R2 egress is free, so the
read side costs nothing per user.

What makes it cheap and safe is content addressing. Every object except the
manifest carries a hash of its own bytes in its name, which means:

  * an object's content can never change, so it ships `immutable` and a client
    (or the CDN) that has it never asks again;
  * re-publishing is a diff — an object whose name is already in the bucket is
    skipped, so a steady-state run uploads the handful of events that actually
    changed plus the feed and manifest;
  * a reader can't observe a half-written feed, because the manifest — the only
    mutable object, and the only way in — is written last.

Deletion is deferred rather than immediate: an object dropped from the manifest
stays for GRACE_HOURS, because clients and CDN nodes still hold the previous
manifest and would otherwise 404 on objects it names.

Env (skips with a log + exit 0 if unset, so local runs never fail):
  R2_ACCOUNT_ID          Cloudflare account id (used to build the S3 endpoint)
  R2_BUCKET              bucket name
  R2_ACCESS_KEY_ID       R2 API token pair, "Object Read & Write"
  R2_SECRET_ACCESS_KEY
  R2_ENDPOINT            optional; overrides the derived endpoint (tests, dev)
"""

from __future__ import annotations

import logging
import mimetypes
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config as cfg

log = logging.getLogger(__name__)

MAX_WORKERS = 8

# How long a de-listed object survives. Must exceed the manifest's own cache
# lifetime plus any client's sync interval, or a client holding the previous
# manifest will ask for an object that no longer exists.
GRACE_HOURS = 48

# Cache lifetimes per object kind. The manifest is revalidated constantly and
# everything else is immutable — that split is the whole caching strategy.
IMMUTABLE = "public, max-age=31536000, immutable"
MANIFEST_CACHE = "public, max-age=0, s-maxage=300, stale-while-revalidate=3600"


def _client():
    """boto3 S3 client pointed at R2, or None when credentials are absent."""
    account = os.getenv("R2_ACCOUNT_ID")
    key = os.getenv("R2_ACCESS_KEY_ID")
    secret = os.getenv("R2_SECRET_ACCESS_KEY")
    endpoint = os.getenv("R2_ENDPOINT") or (
        f"https://{account}.r2.cloudflarestorage.com" if account else None
    )
    if not (endpoint and key and secret):
        return None
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        # R2 ignores the region but the SDK insists on one.
        region_name="auto",
        config=Config(retries={"max_attempts": 3, "mode": "standard"}, max_pool_connections=MAX_WORKERS),
    )


def _list_existing(s3, bucket: str, prefix: str) -> dict[str, datetime]:
    """Every key under `prefix`, mapped to its last-modified time."""
    existing: dict[str, datetime] = {}
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        page = s3.list_objects_v2(**kwargs)
        for obj in page.get("Contents", []):
            existing[obj["Key"]] = obj["LastModified"]
        if not page.get("IsTruncated"):
            return existing
        token = page.get("NextContinuationToken")


def _local_objects(city_dir: Path, city: str) -> dict[str, Path]:
    """Publishable files under the city directory, keyed by their bucket key.

    Only the feed artefacts ship. The demo export (`index.json`, the per-day
    slices) stays local — it is a different product with a different shape, and
    publishing it would double the bucket for no reader.
    """
    keys: dict[str, Path] = {}
    manifest = city_dir / "manifest.json"
    if manifest.exists():
        keys[f"{city}/manifest.json"] = manifest
    for path in city_dir.glob("feed.*.json.gz"):
        keys[f"{city}/{path.name}"] = path
    for path in city_dir.glob("geo.*.json.gz"):
        keys[f"{city}/{path.name}"] = path
    events_dir = city_dir / "events"
    if events_dir.is_dir():
        for path in events_dir.glob("*.json"):
            keys[f"{city}/events/{path.name}"] = path
    return keys


def _put(s3, bucket: str, key: str, path: Path) -> None:
    gzipped = key.endswith(".gz")
    # Strip the .gz so the browser sees the real type; the encoding header tells
    # it the transfer is compressed. R2 serves bytes verbatim — it will not
    # compress for us, which is why export.py writes them already compressed.
    content_type = mimetypes.guess_type(key[:-3] if gzipped else key)[0] or "application/json"
    extra = {
        "ContentType": content_type,
        "CacheControl": MANIFEST_CACHE if key.endswith("manifest.json") else IMMUTABLE,
    }
    if gzipped:
        extra["ContentEncoding"] = "gzip"
    s3.upload_file(str(path), bucket, key, ExtraArgs=extra)


def publish(city: str, out_dir: str | Path | None = None, *, prune: bool = True) -> dict:
    """Upload the city's feed objects, manifest last. Returns a telemetry dict."""
    city_dir = Path(out_dir or cfg.PUBLIC_DIR) / city
    manifest_path = city_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"no manifest at {manifest_path} — run `export` first")

    s3 = _client()
    if s3 is None:
        log.warning("R2 credentials not set — skipping publish")
        return {"skipped": True, "reason": "no-credentials"}

    bucket = os.getenv("R2_BUCKET")
    if not bucket:
        log.warning("R2_BUCKET not set — skipping publish")
        return {"skipped": True, "reason": "no-bucket"}

    local = _local_objects(city_dir, city)
    existing = _list_existing(s3, bucket, f"{city}/")

    # Content-addressed names mean "already present" implies "identical", so the
    # only things worth uploading are new names — plus the manifest, whose name
    # is fixed and whose content changes every run.
    manifest_key = f"{city}/manifest.json"
    todo = [(k, p) for k, p in local.items() if k != manifest_key and k not in existing]

    uploaded = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="r2") as pool:
        futures = {pool.submit(_put, s3, bucket, k, p): k for k, p in todo}
        for future in as_completed(futures):
            future.result()  # surface the first failure before the manifest flips
            uploaded += 1

    # Only now — every object the new manifest names is in place.
    _put(s3, bucket, manifest_key, manifest_path)

    deleted = 0
    if prune:
        deleted = _prune(s3, bucket, city, set(local), existing)

    log.info("published %s: %d uploaded, %d already current, %d pruned",
             city, uploaded, len(local) - len(todo) - 1, deleted)
    return {"uploaded": uploaded, "unchanged": len(local) - len(todo) - 1,
            "pruned": deleted, "objects": len(local)}


def _prune(s3, bucket: str, city: str, keep: set[str], existing: dict[str, datetime]) -> int:
    """Delete de-listed objects older than the grace window.

    The age check is what makes this safe: a client that fetched the previous
    manifest a minute ago is still entitled to the objects it named.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=GRACE_HOURS)
    stale = [k for k, modified in existing.items() if k not in keep and modified < cutoff]
    for chunk in (stale[i:i + 1000] for i in range(0, len(stale), 1000)):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]})
    return len(stale)
