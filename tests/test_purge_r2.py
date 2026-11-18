"""The one-off v1 purge: deletes the superseded generation and nothing else."""
import boto3
import pytest
from moto import mock_aws

from pipeline import purge_r2

BUCKET = "gobento-feed-test"


@pytest.fixture
def r2(monkeypatch):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        monkeypatch.setenv("R2_BUCKET", BUCKET)
        monkeypatch.setenv("R2_ACCESS_KEY_ID", "k")
        monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "s")
        monkeypatch.setenv("R2_ENDPOINT", "https://s3.amazonaws.com")
        yield s3


def _seed(s3, *, legacy=0, current=0, extras=()):
    for i in range(legacy):
        s3.put_object(Bucket=BUCKET, Key=f"berlin/events/evt_{i}.aaaaaaaa.json", Body=b"{}")
    for i in range(current):
        s3.put_object(Bucket=BUCKET, Key=f"berlin/events/evt_{i}.bbbbbbbb.json.gz", Body=b"{}")
    for key in extras:
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"{}")


def _keys(s3):
    return {o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])}


def test_dry_run_deletes_nothing(r2):
    _seed(r2, legacy=3, current=3)
    before = _keys(r2)
    stats = purge_r2.purge_legacy("berlin")
    assert stats == {"legacy": 3, "current": 3, "deleted": 0, "dry_run": True}
    assert _keys(r2) == before


def test_apply_deletes_only_the_uncompressed_generation(r2):
    _seed(r2, legacy=3, current=2)
    stats = purge_r2.purge_legacy("berlin", apply=True)
    assert stats["deleted"] == 3
    remaining = _keys(r2)
    assert all(k.endswith(".json.gz") for k in remaining)
    assert len(remaining) == 2


def test_the_feed_manifest_and_geo_are_never_touched(r2):
    """Only `<city>/events/*.json` goes — manifest.json shares that suffix."""
    extras = ("berlin/manifest.json", "berlin/feed.abc.json.gz",
              "berlin/geo.abc.json.gz", "berlin/.delisted.json")
    _seed(r2, legacy=2, current=2, extras=extras)
    purge_r2.purge_legacy("berlin", apply=True)
    for key in extras:
        assert key in _keys(r2), f"{key} must survive"


def test_refuses_when_no_v2_generation_exists(r2):
    """Without the new generation this would be deleting the live feed."""
    _seed(r2, legacy=5)
    stats = purge_r2.purge_legacy("berlin", apply=True)
    assert stats["deleted"] == 0
    assert stats["refused"]
    assert len(_keys(r2)) == 5


def test_a_migrated_bucket_is_a_no_op(r2):
    _seed(r2, current=4)
    stats = purge_r2.purge_legacy("berlin", apply=True)
    assert stats == {"legacy": 0, "current": 4, "deleted": 0}
    assert len(_keys(r2)) == 4


def test_other_cities_are_untouched(r2):
    _seed(r2, legacy=2, current=2, extras=("munich/events/evt_9.cccccccc.json",))
    purge_r2.purge_legacy("berlin", apply=True)
    assert "munich/events/evt_9.cccccccc.json" in _keys(r2)


def test_batches_past_the_delete_limit(r2, monkeypatch):
    """delete_objects caps at 1000 keys per call."""
    monkeypatch.setattr(purge_r2, "DELETE_BATCH", 2)
    _seed(r2, legacy=5, current=1)
    assert purge_r2.purge_legacy("berlin", apply=True)["deleted"] == 5
    assert len(_keys(r2)) == 1


def test_without_credentials_it_skips(tmp_path, monkeypatch):
    for var in ("R2_ACCOUNT_ID", "R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    assert purge_r2.purge_legacy("berlin")["skipped"] is True


# --- purge-delisted: everything the published manifest does not name -------------

def _published(tmp_path, titles=("A", "B")):
    from pipeline import publish_r2
    from pipeline.db import connect, upsert_event
    from pipeline.export import export_city
    from pipeline.models import Event, Occurrence, Price
    from datetime import date
    day = date.today().isoformat()
    conn = connect(":memory:")
    for i, title in enumerate(titles):
        upsert_event(conn, Event(
            id="", source="vhs", source_event_id=str(i), source_url=f"https://vhs.example/{i}",
            title=title, city="berlin", venue_name="V", price=Price(),
            occurrences=[Occurrence(starts_at_utc=f"{day}T20:00:00Z",
                                    starts_at_local=f"{day}T21:00:00+01:00", nightlife_date=day)]))
    conn.commit()
    export_city(conn, "berlin", tmp_path)
    publish_r2.publish("berlin", tmp_path)


def _all_keys(s3):
    return {o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])}


LEFTOVERS = {
    "berlin/feed.0ld0ld00.json.gz",            # a feed from before a source was switched off
    "berlin/geo.0ld0ld00.json.gz",
    "berlin/events/evt_RA1.deadbeef.json.gz",  # its detail objects
    "berlin/events/evt_RA2.deadbeef.json.gz",
    "berlin/events/evt_V1.json",               # and a pre-v2 one
}


def test_purge_delisted_is_a_dry_run_unless_applied(r2, tmp_path):
    from pipeline.purge_r2 import purge_delisted
    _published(tmp_path)
    for key in LEFTOVERS:
        r2.put_object(Bucket=BUCKET, Key=key, Body=b"{}")
    before = _all_keys(r2)
    stats = purge_delisted("berlin")
    assert stats["dry_run"] and stats["deleted"] == 0 and stats["delisted"] == len(LEFTOVERS)
    assert stats["kinds"] == {"event_objects": 2, "legacy_event_objects": 1, "feeds": 1,
                              "geo": 1, "other": 0}
    assert _all_keys(r2) == before


def test_purge_delisted_removes_exactly_what_the_manifest_does_not_name(r2, tmp_path):
    from pipeline import publish_r2
    from pipeline.purge_r2 import purge_delisted
    _published(tmp_path)
    live = _all_keys(r2) - {"berlin/.delisted.json"}
    for key in LEFTOVERS:
        r2.put_object(Bucket=BUCKET, Key=key, Body=b"{}")
    publish_r2._save_ledger(r2, BUCKET, "berlin", {k: "2026-01-01T00:00:00Z" for k in LEFTOVERS})

    stats = purge_delisted("berlin", apply=True)
    assert stats["deleted"] == len(LEFTOVERS) and stats["live"] == len(live)
    assert _all_keys(r2) == live | {"berlin/.delisted.json"}
    assert publish_r2._load_ledger(r2, BUCKET, "berlin") == {}
    # and the feed still resolves end to end afterwards
    from pipeline.purge_r2 import _named_by_manifest
    assert _named_by_manifest(r2, BUCKET, "berlin") <= _all_keys(r2)


def test_purge_delisted_refuses_when_the_live_feed_is_not_whole(r2, tmp_path):
    from pipeline.purge_r2 import purge_delisted
    _published(tmp_path)
    r2.put_object(Bucket=BUCKET, Key="berlin/events/evt_OLD.deadbeef.json.gz", Body=b"{}")
    victim = next(k for k in _all_keys(r2) if "/events/" in k and "evt_OLD" not in k)
    r2.delete_object(Bucket=BUCKET, Key=victim)   # a named object has gone missing
    before = _all_keys(r2)
    stats = purge_delisted("berlin", apply=True)
    assert stats["deleted"] == 0 and "missing" in stats["refused"]
    assert _all_keys(r2) == before


def test_purge_delisted_refuses_without_a_readable_manifest(r2):
    from pipeline.purge_r2 import purge_delisted
    r2.put_object(Bucket=BUCKET, Key="berlin/events/evt_OLD.deadbeef.json.gz", Body=b"{}")
    stats = purge_delisted("berlin", apply=True)
    assert stats["deleted"] == 0 and stats["refused"]
    assert _all_keys(r2) == {"berlin/events/evt_OLD.deadbeef.json.gz"}
