"""The audit reads the bucket, so it catches what a green run cannot."""
import json
from datetime import date

import boto3
import pytest
from moto import mock_aws

from pipeline import audit_feed, publish_r2
from pipeline.db import connect, upsert_event
from pipeline.export import export_city
from pipeline.models import Event, Occurrence, Price

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


def _publish(tmp_path, description, title="Lesung im Garten"):
    conn = connect(":memory:")
    upsert_event(conn, Event(
        id="", source="kulturdaten", source_event_id="E_1",
        source_url="https://api-v2.kulturdaten.berlin/api/events/E_1",
        title=title, city="berlin", venue_name="Bibliothek", price=Price(),
        description=description, description_public=True, image_url="http://i/1",
        occurrences=[Occurrence(starts_at_utc=f"{DAY}T18:00:00Z",
                                starts_at_local=f"{DAY}T20:00:00+02:00",
                                nightlife_date=DAY)],
    ))
    conn.commit()
    export_city(conn, "berlin", tmp_path)
    publish_r2.publish("berlin", tmp_path)


def test_a_clean_feed_audits_clean(r2, tmp_path):
    _publish(tmp_path, "Eine Lesung am 20.03.2026 um 18.00 Uhr. Eintritt frei.")
    report = audit_feed.audit("berlin")
    assert report["schema_version"] == 2
    assert report["objects_with_contact"] == 0
    assert report["rows_with_contact_in_title"] == 0
    assert report["detail_objects"] == 1
    assert report["legacy_uncompressed"] == 0


def test_it_finds_a_contact_that_reached_a_published_object(r2, tmp_path, monkeypatch):
    """If the export gate ever regresses, this is what catches it."""
    monkeypatch.setattr("pipeline.export.redact_contacts", lambda t: (t, 0))
    _publish(tmp_path, "Eine Lesung.\nAnmeldung: rudow@example.de")
    report = audit_feed.audit("berlin")
    assert report["objects_with_contact"] == 1
    assert report["examples"][0]["fields"] == ["description"]


def test_it_flags_leftover_legacy_objects(r2, tmp_path):
    _publish(tmp_path, "Eine Lesung.")
    r2.put_object(Bucket=BUCKET, Key="berlin/events/evt_old.aaaaaaaa.json", Body=b"{}")
    assert audit_feed.audit("berlin")["legacy_uncompressed"] == 1


def test_sampling_bounds_how_many_objects_are_opened(r2, tmp_path):
    _publish(tmp_path, "Eine Lesung.")
    assert audit_feed.audit("berlin", sample=0)["objects_checked"] == 0


def test_without_credentials_it_skips(monkeypatch):
    for var in ("R2_ACCOUNT_ID", "R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    assert audit_feed.audit("berlin")["skipped"] is True
