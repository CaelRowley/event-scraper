"""DB snapshot/restore: the one asset in the system that cannot be re-derived."""
import gzip
import sqlite3

import boto3
import pytest
from moto import mock_aws

from pipeline import backup
from pipeline.db import connect, upsert_event
from pipeline.models import Event, Occurrence, Price

FEED_BUCKET = "gobento-feed-test"
BACKUP_BUCKET = "gobento-backup-test"


@pytest.fixture
def r2(monkeypatch):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=FEED_BUCKET)
        s3.create_bucket(Bucket=BACKUP_BUCKET)
        monkeypatch.setenv("R2_BUCKET", FEED_BUCKET)
        monkeypatch.setenv("R2_BACKUP_BUCKET", BACKUP_BUCKET)
        monkeypatch.setenv("R2_ACCESS_KEY_ID", "k")
        monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "s")
        monkeypatch.setenv("R2_ENDPOINT", "https://s3.amazonaws.com")
        yield s3


def _db(path, title="Kept Event"):
    conn = connect(str(path))
    upsert_event(conn, Event(
        id="", source="ra", source_event_id="1", source_url="https://ra.example/1",
        title=title, city="berlin", venue_name="V", price=Price(),
        occurrences=[Occurrence(starts_at_utc="2026-09-01T20:00:00Z",
                                starts_at_local="2026-09-01T22:00:00+02:00",
                                nightlife_date="2026-09-01")],
    ))
    conn.commit()
    event_id = conn.execute("SELECT id FROM events").fetchone()["id"]
    conn.close()
    return event_id


def test_snapshot_then_restore_preserves_the_event_ids(r2, tmp_path):
    """The whole point: a restored DB must reuse the ULIDs the feed is keyed by."""
    db = tmp_path / "pipeline.db"
    original = _db(db)
    backup.snapshot(db)

    db.unlink()                       # the cache evicted
    assert backup.restore(db)["key"] == backup.KEY
    assert db.exists()
    restored = sqlite3.connect(db).execute("SELECT id FROM events").fetchone()[0]
    assert restored == original


def test_snapshot_is_stored_gzipped(r2, tmp_path):
    db = tmp_path / "pipeline.db"
    _db(db)
    backup.snapshot(db)
    obj = r2.get_object(Bucket=BACKUP_BUCKET, Key=backup.KEY)
    assert obj["ContentEncoding"] == "gzip"
    assert gzip.decompress(obj["Body"].read())[:15] == b"SQLite format 3"


def test_restore_keeps_a_local_database(r2, tmp_path):
    """A cache hit is newer than any snapshot — restoring over it loses a day."""
    db = tmp_path / "pipeline.db"
    _db(db, "Snapshotted")
    backup.snapshot(db)
    db.unlink()
    newer = _db(db, "From The Cache")

    assert backup.restore(db)["skipped"] is True
    assert sqlite3.connect(db).execute("SELECT id FROM events").fetchone()[0] == newer


def test_restore_force_overwrites(r2, tmp_path):
    db = tmp_path / "pipeline.db"
    original = _db(db)
    backup.snapshot(db)
    db.unlink()
    _db(db, "Replace me")

    backup.restore(db, force=True)
    assert sqlite3.connect(db).execute("SELECT id FROM events").fetchone()[0] == original


def test_backing_up_into_the_public_feed_bucket_is_refused(r2, tmp_path, monkeypatch):
    """The DB holds unredacted prose; the feed bucket is world-readable."""
    monkeypatch.setenv("R2_BACKUP_BUCKET", FEED_BUCKET)
    db = tmp_path / "pipeline.db"
    _db(db)
    with pytest.raises(backup.BucketConfusion, match="must not be the feed bucket"):
        backup.snapshot(db)


def test_restore_without_a_snapshot_skips_cleanly(r2, tmp_path):
    assert backup.restore(tmp_path / "pipeline.db")["skipped"] is True


def test_snapshot_without_a_bucket_skips_instead_of_failing(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_BACKUP_BUCKET", raising=False)
    db = tmp_path / "pipeline.db"
    _db(db)
    assert backup.snapshot(db)["skipped"] is True


def test_snapshot_without_a_database_skips(r2, tmp_path):
    assert backup.snapshot(tmp_path / "missing.db")["skipped"] is True
