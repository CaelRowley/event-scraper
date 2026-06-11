import json

from pipeline.categorise.taxonomy import CATEGORIES
from pipeline.db import connect, upsert_event
from pipeline.models import Event, Occurrence, Price
from pipeline.placeholders import KEYWORDS, placeholder_url
from pipeline.verify import run_sanity_checks


# --- placeholders ----------------------------------------------------------------

def test_placeholder_stable_per_event():
    assert placeholder_url("evt_A", "live_music") == placeholder_url("evt_A", "live_music")


def test_placeholder_differs_across_events():
    urls = {placeholder_url(f"evt_{i}", "live_music") for i in range(50)}
    assert len(urls) >= 48  # hash-distributed locks — duplicates vanishingly rare


def test_placeholder_keyword_matches_category():
    assert "concert" in placeholder_url("evt_A", "live_music")
    assert "nightclub" in placeholder_url("evt_A", "club_nightlife")


def test_placeholder_unknown_category_falls_back():
    assert "berlin" in placeholder_url("evt_A", "definitely_not_a_category")


def test_placeholder_providers():
    assert placeholder_url("evt_A", "comedy", "none") is None
    assert "picsum.photos/seed/evt_A" in placeholder_url("evt_A", "comedy", "picsum")


def test_every_category_has_a_keyword():
    assert set(KEYWORDS) == set(CATEGORIES)


def test_export_emits_placeholder_only_when_imageless(tmp_path):
    from datetime import date, timedelta

    from pipeline.export import export_city

    conn = connect(":memory:")
    tomorrow = (date.today() + timedelta(days=1)).isoformat()  # inside the export window
    occ = Occurrence(starts_at_utc=f"{tomorrow}T20:00:00Z",
                     starts_at_local=f"{tomorrow}T22:00:00+02:00", nightlife_date=tomorrow)
    with_img = Event(id="", source="ra", source_event_id="1", source_url="https://x/1",
                     title="A", city="berlin", price=Price(), image_url="https://img/a.jpg",
                     occurrences=[occ])
    without = Event(id="", source="ra", source_event_id="2", source_url="https://x/2",
                    title="B", city="berlin", price=Price(), occurrences=[occ])
    upsert_event(conn, with_img)
    upsert_event(conn, without)
    export_city(conn, "berlin", tmp_path)
    events = {e["title"]: e for e in
              json.loads((tmp_path / "berlin" / "index.json").read_text())["events"]}
    assert events["A"]["placeholder_url"] is None
    assert events["B"]["placeholder_url"] and "loremflickr" in events["B"]["placeholder_url"]


# --- sanity checks -----------------------------------------------------------------

def _stored_event(conn, *, lat=None, lon=None, price=None, title="X"):
    ev = Event(id="", source="t", source_event_id=title, source_url="https://x",
               title=title, city="berlin", lat=lat, lon=lon, price=price or Price(),
               occurrences=[Occurrence(starts_at_utc="2199-06-13T20:00:00Z",
                                       starts_at_local="2199-06-13T22:00:00+02:00",
                                       nightlife_date="2199-06-13")])
    upsert_event(conn, ev)
    return ev


def test_out_of_city_geo_nulled():
    conn = connect(":memory:")
    _stored_event(conn, lat=48.13, lon=11.58, title="munich-geo")   # Munich
    _stored_event(conn, lat=52.50, lon=13.40, title="berlin-geo")   # Berlin
    stats = run_sanity_checks(conn, "berlin")
    assert stats["geo_nulled"] == 1
    rows = {r["title"]: r["lat"] for r in conn.execute("SELECT title, lat FROM events")}
    assert rows["munich-geo"] is None and rows["berlin-geo"] == 52.50


def test_absurd_price_reverted_to_raw_text():
    conn = connect(":memory:")
    bad = Price(min=2026.0, max=2026.0, type="fixed", text="Jubiläum 2026")  # year mis-parse
    _stored_event(conn, price=bad, title="misparse")
    stats = run_sanity_checks(conn, "berlin")
    assert stats["price_reverted"] == 1
    price = json.loads(conn.execute("SELECT price_json FROM events").fetchone()[0])
    assert price["min"] is None and price["type"] == "unknown" and price["text"] == "Jubiläum 2026"


def test_reasonable_price_untouched():
    conn = connect(":memory:")
    _stored_event(conn, price=Price(min=120.0, max=120.0, type="fixed"), title="arena")
    stats = run_sanity_checks(conn, "berlin")
    assert stats["price_reverted"] == 0


def test_dead_image_nulled_only_for_hard_404():
    from pipeline.linkcheck import check_images

    class FakeResp:
        def __init__(self, status, ctype="image/jpeg"):
            self.status_code = status
            self.headers = {"content-type": ctype}

    class FakeFetcher:
        def __init__(self, mapping):
            self.mapping = mapping

        def head(self, url, **kw):
            result = self.mapping[url]
            if isinstance(result, Exception):
                raise result
            return result

    conn = connect(":memory:")
    for sid, img in (("1", "https://img/dead.jpg"), ("2", "https://img/blocked.jpg"),
                     ("3", "https://img/html-page")):
        ev = _stored_event(conn, title=f"e{sid}")
        conn.execute("UPDATE events SET image_url=? WHERE id=?", (img, ev.id))
    fetcher = FakeFetcher({
        "https://img/dead.jpg": FakeResp(404),
        "https://img/blocked.jpg": FakeResp(403),
        "https://img/html-page": FakeResp(200, "text/html"),
    })
    stats = check_images(conn, fetcher, "berlin")
    imgs = {r["title"]: r["image_url"] for r in conn.execute("SELECT title, image_url FROM events")}
    assert imgs["e1"] is None          # hard 404 → nulled
    assert imgs["e2"] is not None      # blocked → kept (frontend onerror covers it)
    assert imgs["e3"] is None          # serves HTML, not an image → nulled
    assert stats["nulled"] == 2