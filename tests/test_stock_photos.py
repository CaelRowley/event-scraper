"""Commons stand-in photos: no collisions, and never freeze a miss."""
import json
from datetime import date

import httpx
import pytest
import respx

from pipeline.categorise.taxonomy import LABELS
from pipeline.db import connect, upsert_event
from pipeline.models import Event, Occurrence, Price
from pipeline.stock_photos import (
    COMMONS_API, FALLBACK_PHOTO, backfill_stock_photos, query_candidates,
    resolve, search_many, _strip_tracking,
)

DAY = date.today().isoformat()


def _commons_page(index, title, mime="image/jpeg", artist="Ada", licence="CC BY-SA 4.0"):
    return {
        "index": index,
        "imageinfo": [{
            "mime": mime,
            "thumburl": f"https://upload.wikimedia.org/{title}?utm_source=commons",
            "url": f"https://upload.wikimedia.org/{title}",
            "descriptionurl": f"https://commons.wikimedia.org/wiki/File:{title}",
            "extmetadata": {
                "Artist": {"value": artist},
                "LicenseShortName": {"value": licence},
                "LicenseUrl": {"value": "https://creativecommons.org/licenses/by-sa/4.0/"},
            },
        }],
    }


def _reply(pages):
    return httpx.Response(200, json={"query": {"pages": {str(i): p for i, p in enumerate(pages)}}})


def _ev(sid, title, *, img=None, category="live_music"):
    e = Event(
        id="", source="ra", source_event_id=sid, source_url=f"https://ra.example/{sid}",
        title=title, city="berlin", venue_name="Venue", price=Price(), image_url=img,
        occurrences=[Occurrence(starts_at_utc=f"{DAY}T20:00:00Z",
                                starts_at_local=f"{DAY}T20:00:00+02:00", nightlife_date=DAY)],
    )
    e.category = category
    return e


def test_strip_tracking_removes_the_analytics_querystring():
    assert _strip_tracking("https://u.w.org/800px-A.jpg?utm_source=commons") == \
        "https://u.w.org/800px-A.jpg"


def test_query_candidates_go_specific_to_broad_without_duplicates():
    row = {"title": "Klubnacht", "category": "club_nightlife", "venue_name": "Berghain"}
    qs = query_candidates(_FakeRow(row))
    assert qs[0] == f"{LABELS['club_nightlife']} Klubnacht"
    assert "Berghain" in qs
    assert qs[-1] == "party celebration event"
    assert len(qs) == len(set(qs))


class _FakeRow(dict):
    def keys(self):
        return super().keys()
    def __getitem__(self, k):
        return super().get(k)


@respx.mock
def test_search_many_ranks_by_relevance_not_dict_order():
    respx.get(COMMONS_API).mock(return_value=_reply([
        _commons_page(2, "C.jpg"), _commons_page(0, "A.jpg"), _commons_page(1, "B.jpg"),
    ]))
    with httpx.Client() as c:
        urls = [u for u, _ in search_many(c, "q")]
    assert urls == [
        "https://upload.wikimedia.org/A.jpg",
        "https://upload.wikimedia.org/B.jpg",
        "https://upload.wikimedia.org/C.jpg",
    ]


@respx.mock
def test_search_many_rejects_non_photo_mimes():
    respx.get(COMMONS_API).mock(return_value=_reply([
        _commons_page(0, "diagram.svg", mime="image/svg+xml"),
        _commons_page(1, "ok.jpg"),
    ]))
    with httpx.Client() as c:
        urls = [u for u, _ in search_many(c, "q")]
    assert urls == ["https://upload.wikimedia.org/ok.jpg"]


@respx.mock
def test_attribution_is_captured_for_cc_by():
    respx.get(COMMONS_API).mock(return_value=_reply([_commons_page(0, "A.jpg")]))
    with httpx.Client() as c:
        _, attribution = search_many(c, "q")[0]
    assert attribution["author"] == "Ada"
    assert attribution["license"] == "CC BY-SA 4.0"
    assert attribution["source"].startswith("https://commons.wikimedia.org/wiki/File:")


@respx.mock
def test_resolve_skips_photos_already_claimed_by_another_event():
    respx.get(COMMONS_API).mock(return_value=_reply([
        _commons_page(0, "taken.jpg"), _commons_page(1, "free.jpg"),
    ]))
    row = _FakeRow({"title": "T", "category": "live_music", "venue_name": "V"})
    with httpx.Client() as c:
        url, _ = resolve(c, row, claimed={"https://upload.wikimedia.org/taken.jpg"})
    assert url == "https://upload.wikimedia.org/free.jpg"


@respx.mock
def test_resolve_returns_none_when_commons_has_nothing():
    """None, not the fallback — so the caller can decline to store a miss."""
    respx.get(COMMONS_API).mock(return_value=httpx.Response(200, json={"query": {}}))
    row = _FakeRow({"title": "T", "category": "other", "venue_name": None})
    with httpx.Client() as c:
        assert resolve(c, row, claimed=set()) is None


@respx.mock
def test_a_miss_is_not_persisted_so_it_can_be_retried():
    """Storing a throttled miss once froze whole batches onto one disco-ball photo."""
    respx.get(COMMONS_API).mock(return_value=httpx.Response(429))
    conn = connect(":memory:")
    upsert_event(conn, _ev("1", "No Image Event"))
    conn.commit()

    stats = backfill_stock_photos(conn, "berlin")
    row = conn.execute("SELECT stock_image_url, stock_attempted_at FROM events").fetchone()
    assert stats["resolved"] == 0
    assert row["stock_image_url"] is None, "a miss must not be stored"
    assert row["stock_attempted_at"] is not None, "but the attempt must be recorded"
    assert FALLBACK_PHOTO not in (row["stock_image_url"] or "")


@respx.mock
def test_backfill_assigns_distinct_photos_across_events():
    respx.get(COMMONS_API).mock(return_value=_reply([
        _commons_page(0, "one.jpg"), _commons_page(1, "two.jpg"), _commons_page(2, "three.jpg"),
    ]))
    conn = connect(":memory:")
    for i in range(3):
        upsert_event(conn, _ev(str(i), f"Event {i}"))
    conn.commit()

    stats = backfill_stock_photos(conn, "berlin")
    urls = [r["stock_image_url"] for r in
            conn.execute("SELECT stock_image_url FROM events ORDER BY id")]
    assert stats["resolved"] == 3
    assert len(set(urls)) == 3, f"events collided on the same photo: {urls}"


@respx.mock
def test_events_that_already_have_an_image_are_left_alone():
    route = respx.get(COMMONS_API).mock(return_value=_reply([_commons_page(0, "x.jpg")]))
    conn = connect(":memory:")
    upsert_event(conn, _ev("1", "Has Image", img="https://source/real.jpg"))
    conn.commit()
    stats = backfill_stock_photos(conn, "berlin")
    assert stats["considered"] == 0
    assert not route.called


@respx.mock
def test_budget_caps_the_number_of_lookups_per_run():
    respx.get(COMMONS_API).mock(return_value=_reply([_commons_page(0, "a.jpg")]))
    conn = connect(":memory:")
    for i in range(5):
        upsert_event(conn, _ev(str(i), f"E{i}"))
    conn.commit()
    stats = backfill_stock_photos(conn, "berlin", budget=2)
    assert stats["attempted"] == 2


@respx.mock
def test_backfill_goes_through_the_pipeline_fetcher_when_given_one():
    """Sharing the run's Fetcher means Commons answers are cached like every other fetch."""
    from pipeline.fetch import Fetcher
    route = respx.get(COMMONS_API).mock(return_value=_reply([_commons_page(0, "f.jpg")]))
    conn = connect(":memory:")
    upsert_event(conn, _ev("1", "Fetched Event"))
    conn.commit()
    fetcher = Fetcher(cache_path=None)
    try:
        stats = backfill_stock_photos(conn, "berlin", fetcher=fetcher)
    finally:
        fetcher.close()
    assert stats["resolved"] == 1
    assert route.called
    assert fetcher.requests_made >= 1, "the lookup must be counted by the shared fetcher"
    assert route.calls[0].request.headers["User-Agent"].startswith("events-pipeline")
