"""What we may collect and what we may publish — sources.py and everything it gates."""
import gzip
import json
from datetime import date, timedelta

import httpx
import pytest

import pipeline.config as cfg
from pipeline import sources
from pipeline.db import connect, upsert_event
from pipeline.export import export_city
from pipeline.fetch import Fetcher, HostBlockedError, RateSpec, is_denied
from pipeline.models import Event, Occurrence, Price
from pipeline.retention import purge_unpermitted

DAY = (date.today() + timedelta(days=1)).isoformat()
FAST = RateSpec(min_interval=0.0, jitter=(0.0, 0.0))


def _ev(source, sid, title="T", *, desc=None, img="https://img.example/x.jpg", credit=None,
        venue="V", url=None):
    return Event(
        id="", source=source, source_event_id=sid,
        source_url=url or f"https://{source}.example/{sid}",
        title=title, city="berlin", venue_name=venue, price=Price(),
        description=desc, description_public=bool(desc), image_url=img, image_credit=credit,
        occurrences=[Occurrence(starts_at_utc=f"{DAY}T20:00:00Z",
                                starts_at_local=f"{DAY}T21:00:00+01:00", nightlife_date=DAY)],
    )


def _export(conn, tmp_path):
    stats = export_city(conn, "berlin", tmp_path)
    index = json.loads((tmp_path / "berlin" / "index.json").read_text())
    manifest = json.loads((tmp_path / "berlin" / "manifest.json").read_text())
    return stats, {e["title"]: e for e in index["events"]}, manifest


@pytest.fixture(autouse=True)
def _no_ambient_flags(monkeypatch, tmp_path):
    monkeypatch.delenv(sources.PERMIT_ENV, raising=False)
    monkeypatch.delenv("PIPELINE_STOCK_PHOTOS", raising=False)
    monkeypatch.delenv("PIPELINE_SUPPRESSIONS", raising=False)
    # never read the repository's real takedown list from a test
    monkeypatch.setenv("PIPELINE_SUPPRESSIONS_FILE", str(tmp_path / "no-suppressions.yaml"))


# --- which sources run ---------------------------------------------------------

def test_only_sources_with_a_clear_basis_are_on_by_default():
    assert sources.permitted_slugs() == ["berlin_de", "kulturdaten", "prinzipal", "ticketmaster", "vhs"]
    for slug in ("ra", "rausgegangen", "tip_berlin", "livegigs", "eventbrite",
                 "comedy_in_english", "comedy_cafe"):
        assert not sources.is_permitted(slug), slug
        assert sources.policy(slug).reason, f"{slug} is off without a stated reason"


def test_every_adapter_in_the_registry_has_a_policy():
    for slug in cfg.CITIES["berlin"].sources:
        assert sources.policy(slug) is not None, f"{slug} has an adapter but no policy"


def test_an_unregistered_source_is_not_permitted():
    assert not sources.is_permitted("some_new_scraper")


def test_unpermitted_sources_are_never_selected_even_when_named():
    slugs = [s for s, _ in cfg.select_adapters(cfg.CITIES["berlin"])]
    assert slugs == ["kulturdaten", "berlin_de", "vhs", "prinzipal", "ticketmaster"]
    assert cfg.select_adapters(cfg.CITIES["berlin"], only=["ra"]) == []


def test_the_permission_flag_switches_a_source_back_on(monkeypatch):
    monkeypatch.setenv(sources.PERMIT_ENV, "ra, comedy_cafe")
    assert sources.is_permitted("ra")
    slugs = [s for s, _ in cfg.select_adapters(cfg.CITIES["berlin"])]
    assert "ra" in slugs and "comedy_cafe" in slugs and "eventbrite" not in slugs
    assert not is_denied("https://ra.co/graphql")


def test_a_switched_off_sources_domains_are_not_contacted_at_all():
    def handler(request):
        raise AssertionError(f"request sent to {request.url}")

    f = Fetcher(cache_path=None)
    f.client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        for url in ("https://ra.co/graphql", "https://images.ra.co/flyer.jpg",
                    "https://www.comedycafeberlin.com/wp-json/tribe/events/v1/events"):
            assert is_denied(url)
            with pytest.raises(HostBlockedError):
                f.post(url) if "graphql" in url else f.head(url)
    finally:
        f.close()


# --- robots.txt, on every request ------------------------------------------------

def _fetcher(robots_status=200, robots_text="", calls=None):
    def handler(request):
        if calls is not None:
            calls.append(str(request.url))
        if request.url.path == "/robots.txt":
            return httpx.Response(robots_status, text=robots_text)
        return httpx.Response(200, text="ok")

    f = Fetcher(cache_path=None)
    f.client = httpx.Client(transport=httpx.MockTransport(handler))
    return f


def test_a_disallowed_path_is_not_fetched_by_any_method():
    calls = []
    f = _fetcher(robots_text="User-agent: *\nDisallow: /private/\n", calls=calls)
    for method in (f.get, f.head, f.post):
        with pytest.raises(PermissionError):
            method("https://site.example/private/x", rate=FAST)
    assert f.get("https://site.example/public", rate=FAST).status_code == 200
    assert not any("/private/" in c for c in calls)


def test_rules_addressed_to_this_collector_by_name_are_obeyed():
    f = _fetcher(robots_text="User-agent: events-pipeline\nDisallow: /\n\nUser-agent: *\nAllow: /\n")
    with pytest.raises(PermissionError):
        f.get("https://site.example/anything", rate=FAST)


@pytest.mark.parametrize("status", [403, 429, 500, 503])
def test_an_unreadable_robots_txt_means_disallowed_and_counts_as_a_refusal(status):
    calls = []
    f = _fetcher(robots_status=status, calls=calls)
    with pytest.raises(HostBlockedError):
        f.get("https://site.example/page", rate=FAST)
    assert calls == ["https://site.example/robots.txt"], "the page itself must not be requested"
    assert sum(c.get("blocked", 0) for c in f.source_http().values()) == 1


def test_a_missing_robots_txt_means_no_rules():
    f = _fetcher(robots_status=404)
    assert f.get("https://site.example/page", rate=FAST).status_code == 200


def test_a_requested_crawl_delay_overrides_a_shorter_interval(monkeypatch):
    import pipeline.fetch as fetch_mod

    class Clock:
        now, sleeps = 0.0, []

        def monotonic(self):
            return self.now

        def sleep(self, s):
            self.sleeps.append(s)
            self.now += s

    clock = Clock()
    monkeypatch.setattr(fetch_mod, "time", clock)
    f = _fetcher(robots_text="User-agent: *\nCrawl-delay: 7\n")
    for _ in range(3):
        f.get("https://site.example/x", rate=FAST)
    assert clock.sleeps == [7.0, 7.0]


def test_the_api_exemption_covers_only_the_documented_api_path():
    f = _fetcher(robots_text="User-agent: *\nDisallow: /w/\n")
    assert f.allowed("https://commons.wikimedia.org/w/api.php?action=query")
    assert not f.allowed("https://commons.wikimedia.org/w/index.php?title=File:X")
    assert not f.allowed("https://evil.example/w/api.php")  # exemption is per host


def test_the_link_check_does_not_probe_a_page_robots_disallows():
    from pipeline.linkcheck import _probe
    calls = []
    f = _fetcher(robots_text="User-agent: *\nDisallow: /\n", calls=calls)
    assert _probe(f, "https://site.example/event/1") is None  # no verdict, not a strike
    assert calls == ["https://site.example/robots.txt"]


# --- what is published -------------------------------------------------------------

def test_events_of_an_unpermitted_source_are_not_exported(tmp_path):
    conn = connect(":memory:")
    upsert_event(conn, _ev("kulturdaten", "1", "Kept"))
    upsert_event(conn, _ev("ra", "2", "Club Night"))
    upsert_event(conn, _ev("eventbrite", "3", "Workshop"))
    upsert_event(conn, _ev("never_registered", "4", "Mystery"))
    stats, events, manifest = _export(conn, tmp_path)
    assert set(events) == {"Kept"}
    assert stats["withheld_sources"] == {"ra": 1, "eventbrite": 1, "never_registered": 1}
    assert set(manifest["sources"]) == {"kulturdaten"}


def test_a_permitted_source_comes_back_into_the_export(tmp_path, monkeypatch):
    conn = connect(":memory:")
    upsert_event(conn, _ev("ra", "2", "Club Night"))
    monkeypatch.setenv(sources.PERMIT_ENV, "ra")
    _, events, manifest = _export(conn, tmp_path)
    assert set(events) == {"Club Night"}
    assert manifest["sources"]["ra"]["attribution"] == "Resident Advisor"


def test_prose_is_published_only_for_sources_whose_licence_covers_it(tmp_path):
    conn = connect(":memory:")
    upsert_event(conn, _ev("kulturdaten", "1", "Open", desc="Lesung im Kiez."))
    upsert_event(conn, _ev("berlin_de", "2", "Markt", desc="Sa 10-18 Uhr"))
    # stored with a description on purpose: the export must still hold it back
    upsert_event(conn, _ev("ticketmaster", "3", "Arena Show", desc="Promoter's blurb."))
    upsert_event(conn, _ev("prinzipal", "4", "Revue", desc="Venue's own text."))
    _, events, _ = _export(conn, tmp_path)
    assert events["Open"]["description"] == "Lesung im Kiez."
    assert events["Markt"]["description"] == "Sa 10-18 Uhr"
    assert events["Arena Show"]["description"] is None
    assert events["Revue"]["description"] is None


def test_the_manifest_carries_each_sources_licence_and_credit(tmp_path):
    conn = connect(":memory:")
    upsert_event(conn, _ev("kulturdaten", "1", "A"))
    upsert_event(conn, _ev("berlin_de", "2", "B"))
    upsert_event(conn, _ev("ticketmaster", "3", "C"))
    _, _, manifest = _export(conn, tmp_path)
    src = manifest["sources"]
    assert set(src) == {"kulturdaten", "berlin_de", "ticketmaster"}
    assert src["berlin_de"]["licence"] == "CC BY"
    assert src["berlin_de"]["licence_url"].startswith("https://")
    assert "Senatsverwaltung" in src["berlin_de"]["attribution"]
    assert src["kulturdaten"]["licence"] is None, "no data licence is published — do not claim one"
    assert "kulturdaten.berlin" in src["kulturdaten"]["attribution"]
    assert src["ticketmaster"]["terms_url"].startswith("https://developer.ticketmaster.com/")
    for credit in src.values():
        assert credit["name"] and credit["url"] and credit["attribution"]
        assert credit["attribution"] in manifest["attribution"]
    assert "links to its original listing" in manifest["attribution"]


def test_every_feed_row_names_a_source_the_manifest_credits(tmp_path):
    conn = connect(":memory:")
    upsert_event(conn, _ev("kulturdaten", "1", "A"))
    upsert_event(conn, _ev("ticketmaster", "3", "C"))
    _, _, manifest = _export(conn, tmp_path)
    rows = json.loads(gzip.decompress((tmp_path / "berlin" / manifest["feed"]["url"]).read_bytes()))
    assert rows and all(r["source"] in manifest["sources"] and r["source_url"] for r in rows)


def test_a_sources_own_photo_credit_is_published_with_the_photo(tmp_path):
    conn = connect(":memory:")
    upsert_event(conn, _ev("ticketmaster", "1", "Credited", credit="Jane Roe / Live Nation"))
    upsert_event(conn, _ev("ticketmaster", "2", "Uncredited"))
    _, events, _ = _export(conn, tmp_path)
    assert events["Credited"]["image_attribution"] == {"author": "Jane Roe / Live Nation"}
    assert events["Uncredited"]["image_attribution"] is None


def test_commons_stand_ins_are_not_published_unless_switched_on(tmp_path, monkeypatch):
    conn = connect(":memory:")
    upsert_event(conn, _ev("kulturdaten", "1", "Imageless", desc="Text.", img=None))
    conn.execute(
        "UPDATE events SET stock_image_url='https://upload.wikimedia.org/a.jpg', stock_attribution=?",
        (json.dumps({"author": "Ada", "license": "CC BY-SA 4.0",
                     "license_url": "https://creativecommons.org/licenses/by-sa/4.0/",
                     "source": "https://commons.wikimedia.org/wiki/File:A.jpg"}),))
    _, events, _ = _export(conn, tmp_path)
    assert events["Imageless"]["image_url"] is None
    assert events["Imageless"]["image_attribution"] is None

    monkeypatch.setenv("PIPELINE_STOCK_PHOTOS", "1")
    _, events, _ = _export(conn, tmp_path)
    assert events["Imageless"]["image_url"] == "https://upload.wikimedia.org/a.jpg"
    assert events["Imageless"]["image_attribution"]["license"] == "CC BY-SA 4.0"


def test_commons_credit_fields_are_plain_text_not_markup():
    from pipeline.stock_photos import _attribution
    info = {"descriptionurl": "https://commons.wikimedia.org/wiki/File:A.jpg", "extmetadata": {
        "Artist": {"value": '<a href="//commons.wikimedia.org/wiki/User:Ada">Ada&nbsp;L.</a>'},
        "LicenseShortName": {"value": "CC BY 4.0"},
    }}
    assert _attribution(info)["author"] == "Ada L."


# --- what a switched-off source leaves behind --------------------------------------

def test_purge_removes_everything_an_unpermitted_source_stored():
    conn = connect(":memory:")
    upsert_event(conn, _ev("kulturdaten", "1", "Kept"))
    ra_id, _ = upsert_event(conn, _ev("ra", "2", "Club Night"))
    conn.execute("INSERT INTO raw_snapshots(source_slug, source_event_id, payload_json, fetched_at) "
                 "VALUES('ra','2','{}','2026-01-01T00:00:00Z')")
    conn.execute("INSERT INTO crawl_ledger(source_slug,url,first_seen_at,last_seen_at) "
                 "VALUES('ra','https://ra.co/x','2026-01-01','2026-01-01')")
    stats = purge_unpermitted(conn, "berlin")
    assert stats == {"sources": {"ra": 1}, "events": 1}
    for table in ("events", "raw_snapshots", "crawl_ledger"):
        assert conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE source_slug='ra'").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM occurrences WHERE event_id=?", (ra_id,)).fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1


def test_an_event_that_was_merged_under_a_purged_head_stays_published(tmp_path):
    conn = connect(":memory:")
    kd_id, _ = upsert_event(conn, _ev("kulturdaten", "1", "Same Night"))
    ra_id, _ = upsert_event(conn, _ev("ra", "2", "Same Night"))
    conn.execute("UPDATE events SET canonical_id=? WHERE id=?", (ra_id, kd_id))  # ra heads the cluster
    purge_unpermitted(conn, "berlin")
    _, events, _ = _export(conn, tmp_path)
    assert events["Same Night"]["event_id"] == kd_id


# --- adapters -------------------------------------------------------------------------

def test_ticketmaster_carries_the_image_credit_the_api_supplies():
    from pipeline.adapters.ticketmaster import TicketmasterAdapter
    raw = TicketmasterAdapter(None, None)._to_raw({
        "id": "Z1", "name": "Show", "url": "https://www.ticketmaster.de/event/1",
        "dates": {"start": {"dateTime": "2026-12-01T19:00:00Z"}},
        "images": [{"url": "https://s1.ticketm.net/small.jpg", "width": 100},
                   {"url": "https://s1.ticketm.net/big.jpg", "width": 2048,
                    "attribution": "Photo: Jane Roe"}],
    })
    assert raw.image_url == "https://s1.ticketm.net/big.jpg"
    assert raw.image_credit == "Photo: Jane Roe"
    assert raw.source_url == "https://www.ticketmaster.de/event/1"
    assert not raw.description_public


def test_berlin_de_publishes_the_open_data_text_and_keeps_no_contact_fields():
    from pipeline.adapters.berlin_de import ENDPOINTS, BerlinDeAdapter
    item = {"id": 7, "bezeichnung": "Kiezfest", "von": "01.12.2026", "bis": "02.12.2026",
            "zeit": "10-18 Uhr", "bemerkungen": "Mit Bühne", "www": "kiezfest.example",
            "strasse": "Hauptstr. 1", "plz": "10827", "veranstalter": "Erika Musterfrau",
            "mail": "erika@kiezfest.example"}
    raw = BerlinDeAdapter(None, None)._to_raw(item, "strassen-volksfeste",
                                              ENDPOINTS["strassen-volksfeste"])
    assert raw.description == "10-18 Uhr Mit Bühne" and raw.description_public
    assert "mail" not in raw.payload and "veranstalter" not in raw.payload
    assert raw.payload["bezeichnung"] == "Kiezfest"
