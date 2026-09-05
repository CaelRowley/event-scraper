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

    # Title is a list field, so only the feed object is new — the detail objects
    # hold description/occurrences/aliases and none of those moved.
    assert stats["uploaded"] == 1, f"expected 1 new object, got {stats['uploaded']}"
    assert stats["unchanged"] >= 2, "both event objects must not re-upload"
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


def test_detail_objects_are_published_gzipped(r2, tmp_path):
    """R2 serves bytes verbatim, so an object published raw is read raw forever."""
    _seed_and_export(tmp_path, ["A"])
    publish_r2.publish("berlin", tmp_path)
    key = next(k for k in _keys(r2) if k.startswith("berlin/events/"))
    assert key.endswith(".json.gz")
    head = r2.head_object(Bucket=BUCKET, Key=key)
    assert head["ContentEncoding"] == "gzip"
    assert head["ContentType"] == "application/json"
    assert "immutable" in head["CacheControl"]


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


def _delist_b(conn, tmp_path):
    conn.execute("DELETE FROM occurrences WHERE event_id IN (SELECT id FROM events WHERE title='B')")
    conn.execute("DELETE FROM events WHERE title='B'")
    export_city(conn, "berlin", tmp_path)


def _ledger(s3):
    body = s3.get_object(Bucket=BUCKET, Key="berlin/.delisted.json")["Body"].read()
    return json.loads(body)


def test_grace_is_measured_from_delisting_not_upload(r2, tmp_path, monkeypatch):
    """An object uploaded long ago but de-listed today is still in use — keep it."""
    from datetime import datetime, timedelta, timezone
    conn = _seed_and_export(tmp_path, ["A", "B"])
    publish_r2.publish("berlin", tmp_path)
    event_keys_before = {k for k in _keys(r2) if k.startswith("berlin/events/")}

    # Pretend the upload happened well before the grace window …
    long_ago = datetime.now(timezone.utc) - timedelta(hours=publish_r2.GRACE_HOURS * 3)
    real_list = publish_r2._list_existing
    monkeypatch.setattr(publish_r2, "_list_existing",
                        lambda *a: {k: long_ago for k in real_list(*a)})

    # … then de-list B today.
    _delist_b(conn, tmp_path)
    stats = publish_r2.publish("berlin", tmp_path)
    assert stats["pruned"] == 0
    assert event_keys_before <= _keys(r2), "B's object must survive its grace window"
    ledger = _ledger(r2)
    assert len(ledger) == 2, f"B's event object and the old feed should be stamped: {ledger}"
    assert all(k in ledger for k in event_keys_before - _keys_named_by_manifest(r2))


def _keys_named_by_manifest(s3):
    manifest = json.loads(s3.get_object(Bucket=BUCKET, Key="berlin/manifest.json")["Body"].read())
    named = {f"berlin/{manifest['feed']['url']}", f"berlin/{manifest['geo']['url']}"}
    feed_key = f"berlin/{manifest['feed']['url']}"
    import gzip
    rows = json.loads(gzip.decompress(s3.get_object(Bucket=BUCKET, Key=feed_key)["Body"].read()))
    named |= {f"berlin/events/{r['event_id']}.{r['h']}.json.gz" for r in rows}
    return named


def test_prune_deletes_objects_delisted_for_the_whole_grace_window(r2, tmp_path):
    from datetime import datetime, timedelta, timezone
    conn = _seed_and_export(tmp_path, ["A", "B"])
    publish_r2.publish("berlin", tmp_path)
    _delist_b(conn, tmp_path)
    publish_r2.publish("berlin", tmp_path)          # stamps B as de-listed now

    # Age the ledger past the window and publish again.
    old = (datetime.now(timezone.utc) - timedelta(hours=publish_r2.GRACE_HOURS + 1)
           ).strftime("%Y-%m-%dT%H:%M:%SZ")
    aged = {k: old for k in _ledger(r2)}
    publish_r2._save_ledger(r2, BUCKET, "berlin", aged)

    stats = publish_r2.publish("berlin", tmp_path)
    assert stats["pruned"] == len(aged)
    assert sum(k.startswith("berlin/events/") for k in _keys(r2)) == 1
    assert _ledger(r2) == {}, "pruned keys leave the ledger"
    assert "berlin/.delisted.json" in _keys(r2), "the ledger itself is never pruned"


def test_relisted_object_is_forgotten_by_the_ledger(r2, tmp_path):
    conn = _seed_and_export(tmp_path, ["A", "B"])
    publish_r2.publish("berlin", tmp_path)
    conn.execute("UPDATE events SET title='B2' WHERE title='B'")
    export_city(conn, "berlin", tmp_path)
    publish_r2.publish("berlin", tmp_path)
    assert _ledger(r2), "the old B object and feed are de-listed"
    conn.execute("UPDATE events SET title='B' WHERE title='B2'")
    export_city(conn, "berlin", tmp_path)
    publish_r2.publish("berlin", tmp_path)
    assert not any(k.endswith(".json") and "events/" in k for k in _ledger(r2)
                   if k in _keys_named_by_manifest(r2)), "objects named again drop off the ledger"


def test_ledger_is_never_uploaded_from_disk(r2, tmp_path):
    (tmp_path / "berlin").mkdir(parents=True, exist_ok=True)
    _seed_and_export(tmp_path, ["A"])
    (tmp_path / "berlin" / ".delisted.json").write_text("{}")
    assert "berlin/.delisted.json" not in publish_r2._local_objects(tmp_path / "berlin", "berlin")


def test_empty_feed_never_prunes(r2, tmp_path):
    """A zero-event export is a failed scrape until proven otherwise."""
    conn = _seed_and_export(tmp_path, ["A", "B"])
    publish_r2.publish("berlin", tmp_path)
    conn.execute("DELETE FROM occurrences")
    conn.execute("DELETE FROM events")
    export_city(conn, "berlin", tmp_path)
    stats = publish_r2.publish("berlin", tmp_path)
    assert stats["pruned"] == 0
    assert stats["prune_skipped"]
    assert _ledger(r2) == {}, "a refused prune must not start anyone's grace clock"


def test_drastically_smaller_export_never_prunes(r2, tmp_path):
    conn = _seed_and_export(tmp_path, [f"E{i}" for i in range(6)])
    publish_r2.publish("berlin", tmp_path)
    conn.execute("DELETE FROM occurrences WHERE event_id IN "
                 "(SELECT id FROM events WHERE title != 'E0')")
    conn.execute("DELETE FROM events WHERE title != 'E0'")
    export_city(conn, "berlin", tmp_path)
    stats = publish_r2.publish("berlin", tmp_path)
    assert stats["prune_skipped"] and "objects against" in stats["prune_skipped"]


def test_no_prune_leaves_the_ledger_untouched(r2, tmp_path):
    conn = _seed_and_export(tmp_path, ["A", "B"])
    publish_r2.publish("berlin", tmp_path)
    before = _ledger(r2)
    _delist_b(conn, tmp_path)
    stats = publish_r2.publish("berlin", tmp_path, prune=False)
    assert stats["prune_skipped"] is None and stats["pruned"] == 0
    assert _ledger(r2) == before, "--no-prune must not touch the ledger"
