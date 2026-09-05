"""Derived ids: the property that lets the database be thrown away."""
from pipeline.db import connect, upsert_event
from pipeline.ids import event_id, occurrence_id, venue_id
from pipeline.models import Event, Occurrence, Price


def test_the_same_listing_always_derives_the_same_id():
    assert event_id("ra", "12345") == event_id("ra", "12345")


def test_different_sources_do_not_collide():
    assert event_id("ra", "1") != event_id("livegigs", "1")


def test_the_separator_stops_boundary_collisions():
    # Without a delimiter, "ab"+"c" and "a"+"bc" would hash identically.
    assert event_id("ab", "c") != event_id("a", "bc")


def test_ids_keep_the_published_shape():
    eid = event_id("ra", "1")
    assert eid.startswith("evt_") and len(eid) == len("evt_") + 26
    assert venue_id().startswith("ven_")


def test_occurrence_ids_are_derived_too():
    assert occurrence_id("evt_A", "2026-09-01T20:00:00Z") == occurrence_id(
        "evt_A", "2026-09-01T20:00:00Z")
    assert occurrence_id("evt_A", "2026-09-01T20:00:00Z") != occurrence_id(
        "evt_A", "2026-09-02T20:00:00Z")


def _seed(conn):
    upsert_event(conn, Event(
        id="", source="ra", source_event_id="9", source_url="https://ra.example/9",
        title="A Night", city="berlin", venue_name="V", price=Price(),
        occurrences=[Occurrence(starts_at_utc="2026-09-01T20:00:00Z",
                                starts_at_local="2026-09-01T22:00:00+02:00",
                                nightlife_date="2026-09-01")],
    ))
    conn.commit()


def test_a_wiped_database_reproduces_the_same_event_id():
    """The whole point: losing data/pipeline.db must not re-key the catalogue."""
    first = connect(":memory:")
    _seed(first)
    original = first.execute("SELECT id FROM events").fetchone()["id"]

    rebuilt = connect(":memory:")          # a cold start, as after a cache eviction
    _seed(rebuilt)
    assert rebuilt.execute("SELECT id FROM events").fetchone()["id"] == original


def test_a_wiped_database_reproduces_occurrence_rows_too():
    first = connect(":memory:"); _seed(first)
    rebuilt = connect(":memory:"); _seed(rebuilt)
    q = "SELECT id, event_id, starts_at_utc FROM occurrences ORDER BY starts_at_utc"
    assert [tuple(r) for r in first.execute(q)] == [tuple(r) for r in rebuilt.execute(q)]
