"""An event with nowhere to send a reader is not collected, stored or kept."""
import httpx
import pytest

import pipeline.config as cfg
import pipeline.runner as runner_mod
from pipeline.adapters.base import SourceAdapter, has_external_link
from pipeline.db import connect, upsert_event
from pipeline.models import Event, Occurrence, Price, RawEvent
from pipeline.normalise.convert import to_event
from pipeline.retention import prune_linkless
from pipeline.runner import run


@pytest.mark.parametrize("url,expected", [
    ("https://venue.example/event/1", True),
    ("http://www.vhsit.berlin.de/VHSKURSE/BusinessPages/CourseDetail.aspx?id=1", True),
    ("https://www.berlin.de/land/kalender/index.php?detail=293140", True),
    ("https://api-v2.kulturdaten.berlin/api/events/E_123", False),   # a data record, not a page
    ("", False), (None, False), ("   ", False),
    ("Genaue Adresse folgt", False),
    ("mailto:info@venue.example", False),
    ("/events/1", False),
    ("https://localhost/x", False),
    ("ftp://venue.example/x", False),
])
def test_what_counts_as_an_external_link(url, expected):
    assert has_external_link(url) is expected


def test_a_linkless_event_is_not_converted():
    raw = RawEvent(source="kulturdaten", source_event_id="1", source_url="", title="X",
                   start="2026-12-01T19:00:00")
    assert to_event(raw, "berlin") is None
    raw.source_url = "https://api-v2.kulturdaten.berlin/api/events/E_1"
    assert to_event(raw, "berlin") is None
    raw.source_url = "https://venue.example/x"
    assert to_event(raw, "berlin") is not None


class MixedAdapter(SourceAdapter):
    slug = "fake_mixed"

    def fetch_events(self, window_days=14, limit=None):
        for sid, url in (("a", "https://venue.example/a"), ("b", ""),
                         ("c", "https://api-v2.kulturdaten.berlin/api/events/c"),
                         ("d", "kein Link")):
            yield RawEvent(source=self.slug, source_event_id=sid, source_url=url,
                           title=f"Konzert {sid}", start="2199-06-13T20:00:00",
                           description="Anmeldung: Tel. 030 1234567", payload={"k": sid})


def test_the_run_stores_nothing_for_linkless_events_and_says_how_many(tmp_path, monkeypatch):
    db = str(tmp_path / "run.db")
    monkeypatch.setenv("PIPELINE_PERMITTED_SOURCES", "fake_mixed")
    monkeypatch.setattr(cfg, "HTTP_CACHE_PATH", None)
    monkeypatch.setattr(cfg, "PUBLIC_DIR", str(tmp_path / "public"))
    monkeypatch.setattr(cfg, "select_adapters", lambda city, **kw: [("fake_mixed", MixedAdapter)])
    for name in ("backfill_images", "check_links", "check_images"):
        monkeypatch.setattr(runner_mod, name, lambda conn, fetcher, city: {})

    telemetry = run("berlin", db_path=db)

    assert telemetry["sources"]["fake_mixed"] == 1
    assert telemetry["policy"]["no_link_skipped"] == {"fake_mixed": 3}
    conn = connect(db)
    assert [r["source_event_id"] for r in conn.execute("SELECT source_event_id FROM events")] == ["a"]
    snaps = conn.execute("SELECT source_event_id, payload_json FROM raw_snapshots").fetchall()
    assert [s["source_event_id"] for s in snaps] == ["a"], "no snapshot of a linkless event either"
    assert "1234567" not in snaps[0]["payload_json"], "contact details are stripped before storing"


def _stored(conn, sid, url):
    ev = Event(id="", source="kulturdaten", source_event_id=sid, source_url=url, title=sid,
               city="berlin", price=Price(),
               occurrences=[Occurrence(starts_at_utc="2199-06-13T20:00:00Z",
                                       starts_at_local="2199-06-13T22:00:00+02:00",
                                       nightlife_date="2199-06-13")])
    return upsert_event(conn, ev)[0]


def test_linkless_events_stored_by_earlier_runs_are_deleted():
    conn = connect(":memory:")
    keep = _stored(conn, "page", "https://www.berlin.de/land/kalender/index.php?detail=1")
    rec = _stored(conn, "record", "https://api-v2.kulturdaten.berlin/api/events/E_2")
    bare = _stored(conn, "bare", "www")
    conn.execute("UPDATE events SET canonical_id=? WHERE id=?", (rec, keep))  # headed by a linkless one
    assert prune_linkless(conn, "berlin") == {"events": 2}
    rows = conn.execute("SELECT id, canonical_id FROM events").fetchall()
    assert [(r["id"], r["canonical_id"]) for r in rows] == [(keep, keep)]
    assert conn.execute("SELECT COUNT(*) FROM occurrences").fetchone()[0] == 1
    assert bare not in {r["id"] for r in rows}


def test_kulturdaten_skips_an_event_without_a_page_before_reading_its_location():
    from pipeline.adapters.kulturdaten import KulturdatenAdapter

    asked = []

    class Stub(KulturdatenAdapter):
        def _cached(self, table, ref_id, endpoint, key):
            asked.append(endpoint)
            if endpoint == "attractions":
                return {"description": {"de": "Text"}, "metadata": {}}  # no calendar id, no website
            return {"address": {}}

    conn = connect(":memory:")
    entry = {"identifier": "E_1", "schedule": {"startDate": "2026-12-01", "startTime": "19:00:00"},
             "attractions": [{"referenceId": "A_1", "referenceLabel": {"de": "Lesung"}}],
             "locations": [{"referenceId": "L_1", "referenceLabel": {"de": "Bibliothek"}}]}
    assert Stub(None, conn)._to_raw(entry) is None
    assert asked == ["attractions"], "the location is not looked up for an event we will not keep"
