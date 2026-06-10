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


def test_content_hash_stable_and_sensitive():
    raw = RawEvent(source="t", source_event_id="1", source_url="u", title="X", start="2026-06-14")
    a, b = to_event(raw, "berlin"), to_event(raw, "berlin")
    assert a.content_hash() == b.content_hash()
    b.title = "Y"
    assert a.content_hash() != b.content_hash()
