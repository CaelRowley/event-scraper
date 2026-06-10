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
