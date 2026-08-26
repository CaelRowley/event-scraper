from pipeline.db import connect, upsert_event
from pipeline.dedup import norm_title, run_dedup
from pipeline.models import Event, Occurrence, Price


def _event(source, sid, title, venue, start_utc, nightlife_date, lat=None, lon=None, venue_id=None):
    return Event(
        id="", source=source, source_event_id=sid, source_url=f"https://{source}.example/{sid}",
        title=title, city="berlin", venue_name=venue, lat=lat, lon=lon, venue_id=venue_id,
        price=Price(),
        occurrences=[Occurrence(
            starts_at_utc=start_utc, starts_at_local=start_utc, nightlife_date=nightlife_date,
        )],
    )


def _future(day_offset_hint: str = "2199-06-13"):
    return day_offset_hint


def test_norm_title_strips_noise_and_diacritics():
    assert norm_title("SOLD OUT: Späti Räve!") == "spati rave"
    assert norm_title("Berghain presents: NIGHT") == "night"


def test_same_event_two_sources_merges():
    conn = connect(":memory:")
    a = _event("ra", "1", "Klubnacht", "Berghain", "2199-06-13T21:00:00Z", "2199-06-13")
    b = _event("rausgegangen", "x", "Klubnacht!", "Berghain / Panorama Bar", "2199-06-13T21:00:00Z", "2199-06-13")
    upsert_event(conn, a)
    upsert_event(conn, b)
    stats = run_dedup(conn, "berlin")
    assert stats["merged"] == 1
    heads = {r["canonical_id"] for r in conn.execute("SELECT canonical_id FROM events")}
    assert len(heads) == 1
    # head must be the richer source (ra over rausgegangen)
    head = conn.execute("SELECT source_slug FROM events WHERE id=?", (heads.pop(),)).fetchone()
    assert head["source_slug"] == "ra"


def test_title_alone_never_matches():
    conn = connect(":memory:")
    upsert_event(conn, _event("ra", "1", "Open Mic Night", "Madame Claude", "2199-06-13T19:00:00Z", "2199-06-13"))
    upsert_event(conn, _event("eventbrite", "2", "Open Mic Night", "Comedy Café", "2199-06-13T19:00:00Z", "2199-06-13"))
    stats = run_dedup(conn, "berlin")
    assert stats["merged"] == 0


def test_different_dates_never_match():
    conn = connect(":memory:")
    upsert_event(conn, _event("ra", "1", "Klubnacht", "Berghain", "2199-06-13T21:00:00Z", "2199-06-13"))
    upsert_event(conn, _event("rausgegangen", "2", "Klubnacht", "Berghain", "2199-06-20T21:00:00Z", "2199-06-20"))
    stats = run_dedup(conn, "berlin")
    assert stats["merged"] == 0


def test_cross_language_same_venue_same_time():
    conn = connect(":memory:")
    upsert_event(conn, _event("ra", "1", "Lange Nacht der Clubs", "X", "2199-06-13T21:00:00Z", "2199-06-13",
                              lat=52.5111, lon=13.4499))
    upsert_event(conn, _event("eventbrite", "2", "Long Night of Clubs", "X-Club", "2199-06-13T21:05:00Z", "2199-06-13",
                              lat=52.5112, lon=13.4500))
    stats = run_dedup(conn, "berlin")
    assert stats["merged"] == 1


def test_incumbent_head_survives_a_richer_source_arriving_later():
    """A stored event id must not change just because a better source shows up.

    Gobento persists our event id on bookmarks and saved plans, so re-picking the
    head by source priority every run silently orphans those rows.
    """
    conn = connect(":memory:")
    # Day 1: only rausgegangen carries it — it becomes the head of a 2-member cluster.
    upsert_event(conn, _event("rausgegangen", "x", "Klubnacht", "Berghain", "2199-06-13T21:00:00Z", "2199-06-13"))
    upsert_event(conn, _event("livegigs", "y", "Klubnacht!", "Berghain / Panorama Bar", "2199-06-13T21:00:00Z", "2199-06-13"))
    run_dedup(conn, "berlin")
    original_head = conn.execute(
        "SELECT canonical_id FROM events WHERE source_slug='rausgegangen'"
    ).fetchone()["canonical_id"]

    # Day 2: `ra` (higher priority) starts carrying the same event.
    upsert_event(conn, _event("ra", "1", "Klubnacht", "Berghain", "2199-06-13T21:00:00Z", "2199-06-13"))
    run_dedup(conn, "berlin")

    heads = {r["canonical_id"] for r in conn.execute("SELECT canonical_id FROM events")}
    assert len(heads) == 1
    assert heads.pop() == original_head, "incumbent head was replaced; stored ids would dangle"


def test_demoted_ids_are_recorded_as_aliases():
    conn = connect(":memory:")
    upsert_event(conn, _event("ra", "1", "Klubnacht", "Berghain", "2199-06-13T21:00:00Z", "2199-06-13"))
    upsert_event(conn, _event("rausgegangen", "x", "Klubnacht!", "Berghain / Panorama Bar", "2199-06-13T21:00:00Z", "2199-06-13"))
    run_dedup(conn, "berlin")

    head = conn.execute("SELECT canonical_id FROM events LIMIT 1").fetchone()["canonical_id"]
    loser = conn.execute("SELECT id FROM events WHERE id != ?", (head,)).fetchone()["id"]
    alias = conn.execute("SELECT canonical_id FROM event_aliases WHERE alias_id=?", (loser,)).fetchone()
    assert alias is not None and alias["canonical_id"] == head
