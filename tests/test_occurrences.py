from pipeline.db import connect, reconcile_occurrences, upsert_event
from pipeline.models import Event, Occurrence, Price


def _event(*starts: str) -> Event:
    return Event(
        id="",
        source="test",
        source_event_id="series-1",
        source_url="https://example.test/events/series-1",
        title="Hourly open day",
        city="berlin",
        price=Price(),
        occurrences=[
            Occurrence(
                starts_at_utc=start,
                starts_at_local=start.replace("Z", "+00:00"),
                nightlife_date=start[:10],
            )
            for start in starts
        ],
    )


def test_reconcile_keeps_current_slots_and_removes_stale_ones():
    conn = connect(":memory:")
    first, _ = upsert_event(
        conn,
        _event("2026-09-01T10:00:00Z", "2026-09-01T11:00:00Z"),
    )
    conn.commit()

    refreshed, _ = upsert_event(conn, _event("2026-09-01T11:00:00Z"))
    removed = reconcile_occurrences(conn, refreshed, {"2026-09-01T11:00:00Z"})

    assert refreshed == first
    assert removed == 1
    assert [r["starts_at_utc"] for r in conn.execute(
        "SELECT starts_at_utc FROM occurrences ORDER BY starts_at_utc"
    )] == ["2026-09-01T11:00:00Z"]


def test_reconcile_preserves_every_reported_slot():
    conn = connect(":memory:")
    eid, _ = upsert_event(conn, _event("2026-09-01T10:00:00Z"))
    upsert_event(conn, _event("2026-09-01T11:00:00Z"))

    removed = reconcile_occurrences(
        conn,
        eid,
        {"2026-09-01T10:00:00Z", "2026-09-01T11:00:00Z"},
    )

    assert removed == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM occurrences WHERE event_id=?", (eid,)
    ).fetchone()[0] == 2
