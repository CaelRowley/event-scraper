"""Publish semantics: diff-upload, manifest-last, deferred deletion."""
import json
from datetime import date, timedelta

import boto3
import pytest
from moto import mock_aws

from pipeline.db import connect, upsert_event
from pipeline.export import export_city
from pipeline.models import Event, Occurrence, Price
from pipeline import publish_r2

BUCKET = "gobento-feed-test"
DAY = date.today().isoformat()


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


def _ev(sid, title, img="http://i/x"):
    return Event(
        id="", source="ra", source_event_id=sid, source_url=f"https://ra.example/{sid}",
        title=title, city="berlin", venue_name="V", price=Price(), image_url=img,
        occurrences=[Occurrence(starts_at_utc=f"{DAY}T20:00:00Z",
                                starts_at_local=f"{DAY}T20:00:00+02:00", nightlife_date=DAY)],
    )


def _seed_and_export(tmp_path, titles):
    conn = connect(":memory:")
    for i, t in enumerate(titles):
        upsert_event(conn, _ev(str(i), t))
    conn.commit()
    export_city(conn, "berlin", tmp_path)
    return conn


def _keys(s3):
    return {o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])}


def test_publish_uploads_feed_objects_and_manifest(r2, tmp_path):
    _seed_and_export(tmp_path, ["A", "B"])
    stats = publish_r2.publish("berlin", tmp_path)
    keys = _keys(r2)
    assert "berlin/manifest.json" in keys
    assert any(k.startswith("berlin/feed.") for k in keys)
    assert sum(k.startswith("berlin/events/") for k in keys) == 2
    assert stats["uploaded"] > 0


def test_demo_export_is_not_published(r2, tmp_path):
    """index.json and the per-day slices are a different product; keep them local."""
    _seed_and_export(tmp_path, ["A"])
    publish_r2.publish("berlin", tmp_path)
    keys = _keys(r2)
    assert "berlin/index.json" not in keys
    assert not any(k == f"berlin/{DAY}.json" for k in keys)


def test_republishing_unchanged_data_uploads_nothing_but_the_manifest(r2, tmp_path):
    conn = _seed_and_export(tmp_path, ["A", "B"])
    publish_r2.publish("berlin", tmp_path)
    export_city(conn, "berlin", tmp_path)          # same data, exported again
    stats = publish_r2.publish("berlin", tmp_path)
    assert stats["uploaded"] == 0, "content-addressed names should already be present"


def test_changing_one_event_uploads_only_that_object_and_the_feed(r2, tmp_path):
    conn = _seed_and_export(tmp_path, ["A", "B"])
    publish_r2.publish("berlin", tmp_path)
    before = _keys(r2)

    conn.execute("UPDATE events SET title='A2' WHERE title='A'")
    export_city(conn, "berlin", tmp_path)
    stats = publish_r2.publish("berlin", tmp_path)

    # one new event object + one new feed object (manifest is overwritten in place)
    assert stats["uploaded"] == 2, f"expected 2 new objects, got {stats['uploaded']}"
    assert stats["unchanged"] >= 1, "the untouched event must not re-upload"
    # the superseded objects are still served — clients hold the old manifest
    assert before <= _keys(r2)


def test_manifest_only_names_objects_that_exist(r2, tmp_path):
    _seed_and_export(tmp_path, ["A", "B"])
    publish_r2.publish("berlin", tmp_path)
    manifest = json.loads(
        r2.get_object(Bucket=BUCKET, Key="berlin/manifest.json")["Body"].read()
    )
    keys = _keys(r2)
    assert f"berlin/{manifest['feed']['url']}" in keys
    assert f"berlin/{manifest['geo']['url']}" in keys


def test_gzipped_objects_carry_content_encoding(r2, tmp_path):
    _seed_and_export(tmp_path, ["A"])
    publish_r2.publish("berlin", tmp_path)
    feed_key = next(k for k in _keys(r2) if k.startswith("berlin/feed."))
    head = r2.head_object(Bucket=BUCKET, Key=feed_key)
    assert head["ContentEncoding"] == "gzip"
    assert head["ContentType"] == "application/json"
    assert "immutable" in head["CacheControl"]
    manifest = r2.head_object(Bucket=BUCKET, Key="berlin/manifest.json")
    assert "immutable" not in manifest["CacheControl"]
    assert "s-maxage=300" in manifest["CacheControl"]


def test_delisted_objects_survive_the_grace_window(r2, tmp_path):
    """A client holding the previous manifest must not 404 on what it names."""
    conn = _seed_and_export(tmp_path, ["A", "B"])
    publish_r2.publish("berlin", tmp_path)
    conn.execute("DELETE FROM occurrences WHERE event_id IN (SELECT id FROM events WHERE title='B')")
    conn.execute("DELETE FROM events WHERE title='B'")
    export_city(conn, "berlin", tmp_path)
    stats = publish_r2.publish("berlin", tmp_path)
    assert stats["pruned"] == 0, "freshly de-listed objects must not be deleted immediately"
    assert sum(k.startswith("berlin/events/") for k in _keys(r2)) == 2


def test_publish_without_credentials_skips_instead_of_failing(tmp_path, monkeypatch):
    for var in ("R2_ACCOUNT_ID", "R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    _seed_and_export(tmp_path, ["A"])
    assert publish_r2.publish("berlin", tmp_path)["skipped"] is True


def test_publish_before_export_is_a_clear_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="run `export` first"):
        publish_r2.publish("berlin", tmp_path)


def test_prune_deletes_delisted_objects_once_they_age_out(r2, tmp_path):
    from datetime import datetime, timedelta, timezone
    _seed_and_export(tmp_path, ["A", "B"])
    publish_r2.publish("berlin", tmp_path)

    keep = {"berlin/manifest.json"}
    stale_key = next(k for k in _keys(r2) if k.startswith("berlin/events/"))
    old = datetime.now(timezone.utc) - timedelta(hours=publish_r2.GRACE_HOURS + 1)
    recent = datetime.now(timezone.utc)

    deleted = publish_r2._prune(
        r2, BUCKET, "berlin", keep,
        {stale_key: old, "berlin/events/fresh.json": recent},
    )
    assert deleted == 1, "only the aged-out object should go"
    assert stale_key not in _keys(r2)
