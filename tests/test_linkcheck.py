from pipeline.db import connect, upsert_event
from pipeline.linkcheck import check_links
from pipeline.models import Event, Occurrence, Price


class FakeResponse:
    def __init__(self, status):
        self.status_code = status


class FakeFetcher:
    """Maps url → status int, or an Exception to raise."""

    def __init__(self, statuses):
        self.statuses = statuses

    def _resolve(self, url):
        result = self.statuses.get(url, 200)
        if isinstance(result, Exception):
            raise result
        return FakeResponse(result)

    def head(self, url, **kw):
        return self._resolve(url)

    def get(self, url, **kw):
        return self._resolve(url)


def _event(source, sid, url):
    return Event(
        id="", source=source, source_event_id=sid, source_url=url,
        title=f"Event {sid}", city="berlin", price=Price(),
        occurrences=[Occurrence(starts_at_utc="2199-06-13T20:00:00Z",
                                starts_at_local="2199-06-13T22:00:00+02:00",
                                nightlife_date="2199-06-13")],
    )


def _backdate_strike(conn, url, hours=25):
    conn.execute(
        "UPDATE link_checks SET last_strike_at=datetime('now', ?) || 'Z', "
        "last_checked_at='2000-01-01T00:00:00Z' WHERE url=?",
        (f"-{hours} hours", url),
    )


def test_single_404_is_one_strike_not_dead():
    conn = connect(":memory:")
    upsert_event(conn, _event("kulturdaten", "1", "https://x.example/gone"))
    stats = check_links(conn, FakeFetcher({"https://x.example/gone": 404}), "berlin")
    assert stats["strikes"] == 1 and stats["killed"] == 0
    assert conn.execute("SELECT link_dead_at FROM events").fetchone()[0] is None


def test_second_404_after_gap_kills():
    conn = connect(":memory:")
    upsert_event(conn, _event("kulturdaten", "1", "https://x.example/gone"))
    fetcher = FakeFetcher({"https://x.example/gone": 404})
    check_links(conn, fetcher, "berlin")
    _backdate_strike(conn, "https://x.example/gone")
    stats = check_links(conn, fetcher, "berlin")
    assert stats["killed"] == 1
    assert conn.execute("SELECT link_dead_at FROM events").fetchone()[0] is not None


def test_second_404_too_soon_does_not_kill():
    conn = connect(":memory:")
    upsert_event(conn, _event("kulturdaten", "1", "https://x.example/gone"))
    fetcher = FakeFetcher({"https://x.example/gone": 404})
    check_links(conn, fetcher, "berlin")
    # force a recheck without backdating the strike (gap < 20h)
    conn.execute("UPDATE link_checks SET last_checked_at='2000-01-01T00:00:00Z'")
    stats = check_links(conn, fetcher, "berlin")
    assert stats["killed"] == 0


def test_403_and_errors_are_inconclusive():
    conn = connect(":memory:")
    upsert_event(conn, _event("eventbrite", "1", "https://x.example/blocked"))
    upsert_event(conn, _event("eventbrite", "2", "https://x.example/flaky"))
    fetcher = FakeFetcher({
        "https://x.example/blocked": 403,
        "https://x.example/flaky": ConnectionError("boom"),
    })
    stats = check_links(conn, fetcher, "berlin")
    assert stats["strikes"] == 0 and stats["killed"] == 0


def test_alive_resets_strikes():
    conn = connect(":memory:")
    upsert_event(conn, _event("kulturdaten", "1", "https://x.example/back"))
    check_links(conn, FakeFetcher({"https://x.example/back": 404}), "berlin")
    conn.execute("UPDATE link_checks SET last_checked_at='2000-01-01T00:00:00Z'")
    check_links(conn, FakeFetcher({"https://x.example/back": 200}), "berlin")
    assert conn.execute("SELECT strikes FROM link_checks").fetchone()[0] == 0


def test_ra_links_never_checked():
    conn = connect(":memory:")
    upsert_event(conn, _event("ra", "1", "https://ra.co/events/123"))
    stats = check_links(conn, FakeFetcher({"https://ra.co/events/123": 404}), "berlin")
    assert stats["checked"] == 0


def test_dead_head_promotes_live_member():
    conn = connect(":memory:")
    head = _event("kulturdaten", "1", "https://x.example/gone")
    member = _event("rausgegangen", "2", "https://alive.example/event")
    upsert_event(conn, head)
    upsert_event(conn, member)
    # form a cluster with the kulturdaten event as head
    conn.execute("UPDATE events SET canonical_id=? WHERE id IN (?,?)",
                 (head.id, head.id, member.id))
    fetcher = FakeFetcher({"https://x.example/gone": 404})
    check_links(conn, fetcher, "berlin")
    _backdate_strike(conn, "https://x.example/gone")
    stats = check_links(conn, fetcher, "berlin")
    assert stats["killed"] == 1 and stats["promoted"] == 1
    row = conn.execute("SELECT canonical_id FROM events WHERE id=?", (member.id,)).fetchone()
    assert row["canonical_id"] == member.id  # member now heads the cluster


def test_dead_events_excluded_from_export(tmp_path):
    from pipeline.export import export_city
    import json

    conn = connect(":memory:")
    upsert_event(conn, _event("kulturdaten", "1", "https://x.example/gone"))
    upsert_event(conn, _event("kulturdaten", "2", "https://x.example/alive"))
    conn.execute("UPDATE events SET link_dead_at='2026-01-01T00:00:00Z' "
                 "WHERE source_url LIKE '%gone'")
    # export window is relative to today; use far-future occurrence via direct query check
    rows = conn.execute(
        "SELECT COUNT(*) n FROM events e JOIN occurrences o ON o.event_id=e.id "
        "WHERE e.link_dead_at IS NULL").fetchone()
    assert rows["n"] == 1
    export_city(conn, "berlin", tmp_path)
    data = json.loads((tmp_path / "berlin" / "index.json").read_text())
    assert all("gone" not in e["source_url"] for e in data["events"])