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
manifest and would otherwise 404 on objects it names. The clock starts when the
object is *de-listed*, not when it was uploaded — a small ledger
(`<city>/.delisted.json`) records the first run that stopped naming each key,
and only keys that have been absent for the whole window are deleted. Measuring
from upload time instead would delete a months-old object the moment it left
the manifest, which is precisely the 404 the grace window exists to prevent.

A run that looks like a failed scrape — an empty feed, or far fewer objects than
the bucket holds — never prunes. The workflow's freshness gate is the first line
of defence; this is the second.

Env (skips with a log + exit 0 if unset, so local runs never fail):
  R2_ACCOUNT_ID          Cloudflare account id (used to build the S3 endpoint)
  R2_BUCKET              bucket name
  R2_ACCESS_KEY_ID       R2 API token pair, "Object Read & Write"
  R2_SECRET_ACCESS_KEY
  R2_ENDPOINT            optional; overrides the derived endpoint (tests, dev)
"""

from __future__ import annotations

import json
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

# Pruning is refused when the local export holds fewer than this fraction of the
# objects already in the bucket. A healthy day changes a few hundred of ~6,400
# objects; anything that halves the set is a broken scrape, not a quiet week.
MIN_KEEP_RATIO = 0.5

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


def _ledger_key(city: str) -> str:
    return f"{city}/.delisted.json"


def _load_ledger(s3, bucket: str, city: str) -> dict[str, str]:
    """`{key: iso-time first seen missing from the manifest}`; empty if absent."""
    try:
        body = s3.get_object(Bucket=bucket, Key=_ledger_key(city))["Body"].read()
    except s3.exceptions.NoSuchKey:
        return {}
    try:
        ledger = json.loads(body)
    except ValueError:
        log.warning("unreadable de-list ledger for %s — starting a fresh one", city)
        return {}
    return {k: v for k, v in ledger.items() if isinstance(v, str)}


def _save_ledger(s3, bucket: str, city: str, ledger: dict[str, str]) -> None:
    s3.put_object(
        Bucket=bucket, Key=_ledger_key(city),
        Body=json.dumps(dict(sorted(ledger.items())), separators=(",", ":")).encode(),
        ContentType="application/json", CacheControl="no-store",
    )


def _local_objects(city_dir: Path, city: str) -> dict[str, Path]:
    """Publishable files under the city directory, keyed by their bucket key.

    Only the feed artefacts ship. The demo export (`index.json`, the per-day
    slices) stays local — it is a different product with a different shape, and
    publishing it would double the bucket for no reader. The de-list ledger is
    bucket-side state, never a local file, so it is not among these either.
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
    ledger_key = _ledger_key(city)
    existing = _list_existing(s3, bucket, f"{city}/")
    existing.pop(ledger_key, None)

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
    prune_skipped = None
    if prune:
        prune_skipped = _unsafe_to_prune(manifest_path, len(local), len(existing))
        if prune_skipped:
            log.warning("refusing to prune %s: %s", city, prune_skipped)
        else:
            now = datetime.now(timezone.utc)
            ledger = _note_delisted(_load_ledger(s3, bucket, city), set(local), existing, now)
            deleted = _prune(s3, bucket, ledger, now)
            _save_ledger(s3, bucket, city, ledger)

    log.info("published %s: %d uploaded, %d already current, %d pruned",
             city, uploaded, len(local) - len(todo) - 1, deleted)
    return {"uploaded": uploaded, "unchanged": len(local) - len(todo) - 1,
            "pruned": deleted, "objects": len(local), "prune_skipped": prune_skipped}


def _unsafe_to_prune(manifest_path: Path, local_count: int, existing_count: int) -> str | None:
    """Why this run must not delete anything, or None when it is safe to."""
    try:
        count = json.loads(manifest_path.read_text())["feed"]["count"]
    except (ValueError, KeyError, TypeError):
        return "manifest has no feed count"
    if not count:
        return "the feed is empty"
    if existing_count and local_count < existing_count * MIN_KEEP_RATIO:
        return f"export holds {local_count} objects against {existing_count} in the bucket"
    return None


def _note_delisted(ledger: dict[str, str], keep: set[str], existing: dict[str, datetime],
                   now: datetime) -> dict[str, str]:
    """Advance the ledger: stamp newly de-listed keys, forget keys that are back."""
    stamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    out = {k: t for k, t in ledger.items() if k in existing and k not in keep}
    for key in existing:
        if key not in keep:
            out.setdefault(key, stamp)
    return out


def _prune(s3, bucket: str, ledger: dict[str, str], now: datetime) -> int:
    """Delete objects de-listed for longer than the grace window; drop them from the ledger.

    The age check is what makes this safe: a client that fetched the previous
    manifest a minute ago is still entitled to the objects it named, however old
    those objects are.
    """
    cutoff = now - timedelta(hours=GRACE_HOURS)
    stale = []
    for key, stamped in ledger.items():
        try:
            since = datetime.fromisoformat(stamped.replace("Z", "+00:00"))
        except ValueError:
            since = now  # unreadable stamp: restart its clock rather than delete
            ledger[key] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        if since < cutoff:
            stale.append(key)
    for chunk in (stale[i:i + 1000] for i in range(0, len(stale), 1000)):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]})
    for key in stale:
        ledger.pop(key, None)
    return len(stale)
