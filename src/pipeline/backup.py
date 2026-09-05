"""Snapshot `data/pipeline.db` to R2, and restore it when the CI cache has gone.

The pipeline DB is the only thing in this system that cannot be re-derived. It
holds the ULIDs every published event is keyed by (`ids.py` mints them at insert
time and `db.py` reuses one only when a row already exists for
`source_slug`+`source_event_id`), plus `first_seen_at`, the dedup clusters, the
alias trail, the delta and image-scan ledgers, and the id of a Haiku batch that
may still be in flight.

It lives in the Actions cache, which is evicted after 7 idle days or under the
repo's 10 GB ceiling. Losing it does not merely cost a slow cold run: every event
is re-inserted with a *new* ULID, so every saved event id in the app breaks, the
alias map cannot help (it records dedup supersessions, not a rebuild), everything
reads as newly seen, and the whole bucket churns in one go.

So: one rolling snapshot, written after the freshness gate has passed, restored
only when the cache came back empty.

It goes to a **separate bucket** from the feed. The DB carries the raw scraped
prose before `redact.py` touches it, along with internal scoring — none of which
belongs in a bucket the app serves to the public. Pointing both at one bucket is
refused rather than warned about.

Env (skips with a log if unset, so local runs never fail):
  R2_BACKUP_BUCKET       private bucket, must not be R2_BUCKET
  R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_ENDPOINT
                         as for publishing — see publish_r2.py
"""

from __future__ import annotations

import gzip
import logging
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

from . import config as cfg
from .publish_r2 import client

log = logging.getLogger(__name__)

KEY = "pipeline.db.gz"


class BucketConfusion(RuntimeError):
    """The backup bucket is the public feed bucket. Refuse rather than leak."""


def _bucket() -> str | None:
    bucket = os.getenv("R2_BACKUP_BUCKET")
    if not bucket:
        return None
    if bucket == os.getenv("R2_BUCKET"):
        raise BucketConfusion(
            "R2_BACKUP_BUCKET must not be the feed bucket — the DB holds unredacted "
            "source prose and internal scoring, and the feed bucket is world-readable"
        )
    return bucket


def _skip(reason: str) -> dict:
    log.warning("DB snapshot skipped: %s", reason)
    return {"skipped": True, "reason": reason}


def snapshot(db_path: str | Path | None = None) -> dict:
    """Upload a consistent, compacted copy of the DB. Overwrites the previous one."""
    path = Path(db_path or cfg.DB_PATH)
    if not path.exists():
        return _skip("no local database")

    bucket = _bucket()
    if not bucket:
        return _skip("R2_BACKUP_BUCKET not set")
    s3 = client()
    if s3 is None:
        return _skip("no credentials")

    with tempfile.TemporaryDirectory() as tmp:
        # VACUUM INTO takes a consistent copy without stopping writers and drops
        # free pages on the way out — a plain file copy of a live WAL database
        # can land mid-transaction.
        compact = Path(tmp) / "pipeline.db"
        conn = sqlite3.connect(path)
        try:
            conn.execute("VACUUM INTO ?", (str(compact),))
        finally:
            conn.close()

        # Streamed rather than compressed in memory — this file grows with the
        # corpus. mtime=0 keeps identical content compressing to identical bytes.
        blob = Path(tmp) / KEY
        with open(compact, "rb") as src, open(blob, "wb") as out:
            with gzip.GzipFile(fileobj=out, mode="wb", mtime=0) as dst:
                shutil.copyfileobj(src, dst)
        size = blob.stat().st_size
        s3.upload_file(str(blob), bucket, KEY, ExtraArgs={
            "ContentType": "application/x-sqlite3",
            "ContentEncoding": "gzip",
            "CacheControl": "no-store",
        })

    log.info("snapshotted %s to %s/%s (%.1f MB gzipped)", path, bucket, KEY, size / 1e6)
    return {"bucket": bucket, "key": KEY, "bytes": size}


def restore(db_path: str | Path | None = None, *, force: bool = False) -> dict:
    """Fetch the snapshot, but only when there is no local DB to lose.

    A cache hit must always win: it is newer than any snapshot by definition, and
    silently overwriting it would discard a day of ledger state.
    """
    path = Path(db_path or cfg.DB_PATH)
    if path.exists() and not force:
        return _skip("local database present")

    bucket = _bucket()
    if not bucket:
        return _skip("R2_BACKUP_BUCKET not set")
    s3 = client()
    if s3 is None:
        return _skip("no credentials")

    try:
        body = s3.get_object(Bucket=bucket, Key=KEY)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return _skip("no snapshot in the bucket yet")

    path.parent.mkdir(parents=True, exist_ok=True)
    raw = gzip.decompress(body)
    # Write beside the target and move into place, so an interrupted restore
    # cannot leave a half-written database that the next run would open.
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
        tmp.write(raw)
        staged = Path(tmp.name)
    staged.replace(path)

    log.info("restored %s from %s/%s (%.1f MB)", path, bucket, KEY, len(raw) / 1e6)
    return {"bucket": bucket, "key": KEY, "bytes": len(raw)}
