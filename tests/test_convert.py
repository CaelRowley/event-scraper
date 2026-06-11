from pipeline.adapters.jsonld_common import jsonld_to_raw
from pipeline.models import RawEvent
from pipeline.normalise.convert import to_event


def test_full_jsonld_round_trip():
    node = {
        "@type": "MusicEvent",
        "name": "Test Gig",
        "startDate": "2026-06-14T20:00:00+02:00",
        "endDate": "2026-06-14T23:00:00+02:00",
        "eventStatus": "https://schema.org/EventCancelled",
        "eventAttendanceMode": "https://schema.org/OfflineEventAttendanceMode",
        "location": {
            "@type": "Place", "name": "SO36",
            "address": {"streetAddress": "Oranienstr. 190", "postalCode": "10999",
                        "addressLocality": "Berlin"},
            "geo": {"latitude": "52.499", "longitude": "13.428"},
        },
        "offers": {"price": "15", "priceCurrency": "EUR"},
        "image": [{"@type": "ImageObject", "url": "https://img.example/x.jpg"}],
    }
    raw = jsonld_to_raw(node, source="test", page_url="https://example.com/e/1")
    ev = to_event(raw, "berlin")
    assert ev.title == "Test Gig"
    assert ev.event_status == "cancelled"
    assert ev.attendance_mode == "offline"
    assert ev.venue_name == "SO36"
    assert ev.lat == 52.499
    assert ev.price.min == 15 and ev.price.type == "fixed"
    assert ev.image_url == "https://img.example/x.jpg"
    assert ev.occurrences[0].starts_at_utc == "2026-06-14T18:00:00Z"


def test_unparseable_start_drops_event():
    raw = RawEvent(source="t", source_event_id="1", source_url="u", title="X", start="???")
    assert to_event(raw, "berlin") is None


def test_online_mode_mapped():
    raw = RawEvent(source="t", source_event_id="1", source_url="u", title="X",
                   start="2026-06-14", attendance_mode="https://schema.org/OnlineEventAttendanceMode")
    ev = to_event(raw, "berlin")
    assert ev.attendance_mode == "online"


def test_free_hint_sets_tag():
    raw = RawEvent(source="t", source_event_id="1", source_url="u", title="X",
                   start="2026-06-14", is_free=True)
    ev = to_event(raw, "berlin")
    assert ev.price.is_free and "free-entry" in ev.tags


def test_description_only_stored_for_open_licensed_sources():
    from pipeline.db import connect, upsert_event

    conn = connect(":memory:")
    open_raw = RawEvent(source="kd", source_event_id="1", source_url="u1", title="A",
                        start="2026-06-14", description="CC-BY text", description_public=True)
    closed_raw = RawEvent(source="tip", source_event_id="2", source_url="u2", title="B",
                          start="2026-06-14", description="editorial text")
    for raw in (open_raw, closed_raw):
        upsert_event(conn, to_event(raw, "berlin"))
    rows = {r["source_slug"]: r["description"]
            for r in conn.execute("SELECT source_slug, description FROM events")}
    assert rows == {"kd": "CC-BY text", "tip": None}


def test_source_url_change_marks_event_changed():
    from pipeline.db import connect, upsert_event

    conn = connect(":memory:")
    raw = RawEvent(source="kd", source_event_id="1", source_url="https://api.example/rec/1",
                   title="A", start="2026-06-14")
    upsert_event(conn, to_event(raw, "berlin"))
    raw.source_url = "https://venue.example/event"  # website discovered on a later run
    _, changed = upsert_event(conn, to_event(raw, "berlin"))
    assert changed
    row = conn.execute("SELECT source_url FROM events").fetchone()
    assert row["source_url"] == "https://venue.example/event"


def test_content_hash_stable_and_sensitive():
    raw = RawEvent(source="t", source_event_id="1", source_url="u", title="X", start="2026-06-14")
    a, b = to_event(raw, "berlin"), to_event(raw, "berlin")
    assert a.content_hash() == b.content_hash()
    b.title = "Y"
    assert a.content_hash() != b.content_hash()
