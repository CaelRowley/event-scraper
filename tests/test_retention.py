"""Finished events leave, and take their baggage with them."""
from datetime import date, timedelta

from pipeline.db import connect, upsert_event, queue_for_llm, snapshot
from pipeline.models import Event, Occurrence, Price
from pipeline.retention import prune_past

TODAY = date.today()
def day(offset): return (TODAY + timedelta(days=offset)).isoformat()


def _ev(conn, sid, *, days, title="X", url=None, image=None):
    """An event whose occurrences sit on the given day offsets."""
    ev = Event(
        id="", source="ra", source_event_id=sid, source_url=url or f"https://ra.example/{sid}",
        title=title, city="berlin", venue_name="V", price=Price(), image_url=image,
        occurrences=[Occurrence(starts_at_utc=f"{day(d)}T20:00:00Z",
                                starts_at_local=f"{day(d)}T22:00:00+02:00",
                                nightlife_date=day(d)) for d in days],
    )
    eid, _ = upsert_event(conn, ev)
    conn.commit()
    return eid


def _count(conn, table, where="", args=()):
    return conn.execute(f"SELECT COUNT(*) c FROM {table} {where}", args).fetchone()["c"]


def test_a_finished_event_is_removed():
    conn = connect(":memory:")
    _ev(conn, "old", days=[-40])
    assert prune_past(conn, "berlin")["events"] == 1
    assert _count(conn, "events") == 0
    assert _count(conn, "occurrences") == 0


def test_an_upcoming_event_survives():
    conn = connect(":memory:")
    _ev(conn, "future", days=[5])
    assert prune_past(conn, "berlin")["events"] == 0
    assert _count(conn, "events") == 1


def test_judged_on_the_last_occurrence_not_the_first():
    """A long-running exhibition started months ago and is still on."""
    conn = connect(":memory:")
    _ev(conn, "expo", days=[-60, -30, 3])
    assert prune_past(conn, "berlin")["events"] == 0
    assert _count(conn, "events") == 1


def test_the_retention_margin_is_respected():
    conn = connect(":memory:")
    _ev(conn, "yesterday", days=[-1])
    assert prune_past(conn, "berlin", keep_days=7)["events"] == 0
    assert prune_past(conn, "berlin", keep_days=0)["events"] == 1


def test_the_llm_queue_does_not_outlive_its_event():
    """The queue only grows while the AI tier is off; pruning has to reach it."""
    conn = connect(":memory:")
    eid = _ev(conn, "queued", days=[-40])
    queue_for_llm(conn, eid)
    conn.commit()
    assert _count(conn, "llm_queue") == 1
    prune_past(conn, "berlin")
    assert _count(conn, "llm_queue") == 0


def test_the_raw_snapshot_goes_too():
    conn = connect(":memory:")
    _ev(conn, "snapped", days=[-40])
    snapshot(conn, "ra", "snapped", {"payload": "x"})
    conn.commit()
    assert _count(conn, "raw_snapshots") == 1
    prune_past(conn, "berlin")
    assert _count(conn, "raw_snapshots") == 0


def test_aliases_go_from_both_sides():
    conn = connect(":memory:")
    dead = _ev(conn, "dead", days=[-40])
    conn.execute("INSERT INTO event_aliases(alias_id, canonical_id, noted_at) VALUES(?,?,?)",
                 ("some-old-id", dead, "2026-01-01T00:00:00Z"))
    conn.execute("INSERT INTO event_aliases(alias_id, canonical_id, noted_at) VALUES(?,?,?)",
                 (dead, "some-live-id", "2026-01-01T00:00:00Z"))
    conn.commit()
    prune_past(conn, "berlin")
    assert _count(conn, "event_aliases") == 0


def test_another_city_is_untouched():
    conn = connect(":memory:")
    ev = Event(id="", source="ra", source_event_id="m", source_url="https://ra.example/m",
               title="Munich", city="munich", venue_name="V", price=Price(),
               occurrences=[Occurrence(starts_at_utc=f"{day(-40)}T20:00:00Z",
                                       starts_at_local=f"{day(-40)}T22:00:00+02:00",
                                       nightlife_date=day(-40))])
    upsert_event(conn, ev); conn.commit()
    assert prune_past(conn, "berlin")["events"] == 0
    assert _count(conn, "events") == 1


def test_an_event_with_no_occurrences_left_is_judged_on_last_seen():
    """reconcile_occurrences can empty an event; it must not become immortal."""
    conn = connect(":memory:")
    eid = _ev(conn, "empty", days=[-40])
    conn.execute("DELETE FROM occurrences WHERE event_id=?", (eid,))
    conn.execute("UPDATE events SET last_seen_at=? WHERE id=?", (f"{day(-40)}T00:00:00Z", eid))
    conn.commit()
    assert prune_past(conn, "berlin")["events"] == 1


def test_pruning_is_safe_to_run_on_an_empty_database():
    conn = connect(":memory:")
    assert prune_past(conn, "berlin")["events"] == 0
