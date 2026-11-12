"""The takedown list: one request removes an organiser's events from everything published."""
import json
from datetime import date, timedelta

import boto3
import pytest
from moto import mock_aws

from pipeline import publish_r2
from pipeline.db import connect, upsert_event
from pipeline.export import export_city
from pipeline.models import Event, Occurrence, Price
from pipeline.suppress import Suppressions, load

DAY = (date.today() + timedelta(days=1)).isoformat()
BUCKET = "gobento-feed-test"


def _ev(sid, title, *, venue="V", url=None):
    return Event(
        id="", source="kulturdaten", source_event_id=sid,
        source_url=url or f"https://organiser-{sid}.example/event",
        title=title, city="berlin", venue_name=venue, price=Price(),
        image_url="https://img.example/x.jpg", description="Text.", description_public=True,
        occurrences=[Occurrence(starts_at_utc=f"{DAY}T20:00:00Z",
                                starts_at_local=f"{DAY}T21:00:00+01:00", nightlife_date=DAY)],
    )


@pytest.fixture
def listfile(tmp_path, monkeypatch):
    path = tmp_path / "suppressions.yaml"
    monkeypatch.setenv("PIPELINE_SUPPRESSIONS_FILE", str(path))
    monkeypatch.delenv("PIPELINE_SUPPRESSIONS", raising=False)
    return path


def _titles(out):
    return {e["title"] for e in json.loads((out / "berlin" / "index.json").read_text())["events"]}


def _seed():
    conn = connect(":memory:")
    ids = {
        "a": upsert_event(conn, _ev("a", "Alpha Night"))[0],
        "b": upsert_event(conn, _ev("b", "Beta Markt", venue="Café Größenwahn"))[0],
        "c": upsert_event(conn, _ev("c", "Gamma Promo presents: Live",
                                    url="https://tickets.promo.example/e/1"))[0],
    }
    conn.commit()
    return conn, ids


def test_an_empty_or_absent_list_suppresses_nothing(listfile, tmp_path):
    conn, _ = _seed()
    assert export_city(conn, "berlin", tmp_path / "out")["suppressed"] == 0
    listfile.write_text("events: []\nurls: []\n")
    assert export_city(conn, "berlin", tmp_path / "out")["suppressed"] == 0
    assert len(_titles(tmp_path / "out")) == 3


def test_one_event_is_removed_by_id_in_the_format_the_admin_page_gives(listfile, tmp_path):
    conn, ids = _seed()
    listfile.write_text(f"events:\n  - value: {ids['a']}\n    reason: organiser request\n")
    stats = export_city(conn, "berlin", tmp_path / "out")
    assert stats["suppressed"] == 1
    assert _titles(tmp_path / "out") == {"Beta Markt", "Gamma Promo presents: Live"}
    manifest = json.loads((tmp_path / "out" / "berlin" / "manifest.json").read_text())
    assert manifest["feed"]["count"] == 2
    assert not list((tmp_path / "out" / "berlin" / "events").glob(f"{ids['a']}.*"))


def test_an_occurrence_id_or_a_superseded_id_removes_the_event_too(listfile, tmp_path):
    conn, ids = _seed()
    conn.execute("INSERT INTO event_aliases(alias_id, canonical_id, noted_at) VALUES(?,?,?)",
                 ("OLD-ID", ids["b"], "2026-01-01T00:00:00Z"))
    listfile.write_text(f"events:\n  - value: {ids['a']}::{DAY}T20:00:00Z\n  - OLD-ID\n")
    export_city(conn, "berlin", tmp_path / "out")
    assert _titles(tmp_path / "out") == {"Gamma Promo presents: Live"}


def test_one_request_can_remove_everything_from_an_organiser(listfile, tmp_path):
    conn, _ = _seed()
    listfile.write_text("domains:\n  - value: promo.example\n")
    export_city(conn, "berlin", tmp_path / "out")
    assert "Gamma Promo presents: Live" not in _titles(tmp_path / "out")

    listfile.write_text("venues:\n  - value: cafe grossenwahn\n")
    export_city(conn, "berlin", tmp_path / "out")
    assert _titles(tmp_path / "out") == {"Alpha Night", "Gamma Promo presents: Live"}

    listfile.write_text("titles:\n  - value: gamma promo presents\n")
    export_city(conn, "berlin", tmp_path / "out")
    assert _titles(tmp_path / "out") == {"Alpha Night", "Beta Markt"}

    listfile.write_text("urls:\n  - value: https://organiser-a.example/*\n")
    export_city(conn, "berlin", tmp_path / "out")
    assert _titles(tmp_path / "out") == {"Beta Markt", "Gamma Promo presents: Live"}


def test_matching_rules():
    s = Suppressions({"urls": ["https://x.example/e/1/"], "domains": ["org.example"]})
    assert s.match(url="https://X.example/e/1") == "urls"
    assert s.match(url="https://x.example/e/12") is None
    assert s.match(url="https://www.org.example/a") == "domains"
    assert s.match(url="https://notorg.example/a") is None
    assert s.match(event_ids=["nope"], url=None, venue="V", title="T") is None


def test_entries_from_the_secret_are_merged_with_the_file(listfile, monkeypatch, tmp_path):
    conn, ids = _seed()
    listfile.write_text(f"events:\n  - value: {ids['a']}\n")
    monkeypatch.setenv("PIPELINE_SUPPRESSIONS", f"events:\n  - value: {ids['b']}\n")
    assert len(load()) == 2
    export_city(conn, "berlin", tmp_path / "out")
    assert _titles(tmp_path / "out") == {"Gamma Promo presents: Live"}


@pytest.mark.parametrize("text", [
    "events: [unclosed",                 # not YAML
    "event:\n  - value: X\n",            # misspelt section
    "events:\n  - reason: no value\n",   # entry without a value
    "events: just-a-string\n",           # not a list
])
def test_a_broken_list_stops_the_export_instead_of_publishing(listfile, tmp_path, text):
    conn, _ = _seed()
    listfile.write_text(text)
    with pytest.raises(ValueError):
        export_city(conn, "berlin", tmp_path / "out")


def test_a_takedown_is_deleted_from_the_bucket_at_once_not_after_the_grace_window(
        listfile, tmp_path, monkeypatch):
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)
        monkeypatch.setenv("R2_BUCKET", BUCKET)
        monkeypatch.setenv("R2_ACCESS_KEY_ID", "k")
        monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "s")
        monkeypatch.setenv("R2_ENDPOINT", "https://s3.amazonaws.com")

        def keys():
            return {o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])}

        conn, ids = _seed()
        out = tmp_path / "out"
        export_city(conn, "berlin", out)
        assert publish_r2.publish("berlin", out)["taken_down"] == 0
        assert any(k.startswith(f"berlin/events/{ids['a']}.") for k in keys())

        listfile.write_text(f"events:\n  - value: {ids['a']}\n")
        export_city(conn, "berlin", out)
        stats = publish_r2.publish("berlin", out)

        assert stats["taken_down"] == 1
        assert not any(k.startswith(f"berlin/events/{ids['a']}.") for k in keys())
        # the other events' objects are untouched, and nothing local-only was uploaded
        assert sum(k.startswith("berlin/events/") for k in keys()) == 2
        assert not any(k.endswith(".takedown.json") for k in keys())
