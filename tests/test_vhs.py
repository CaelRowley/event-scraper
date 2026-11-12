"""Volkshochschule open course data: what becomes an event, and what never leaves the file."""
import json
from datetime import date, timedelta

import httpx

from pipeline.adapters.vhs import FEED, VhsAdapter, to_raw
from pipeline.db import connect
from pipeline.fetch import Fetcher

SOON = (date.today() + timedelta(days=3)).isoformat()
LATER = (date.today() + timedelta(days=10)).isoformat()
FAR = (date.today() + timedelta(days=90)).isoformat()


def _course(guid="1", **over):
    course = {
        "guid": guid, "nummer": "Mi-1", "name": "Deutsch A1.1", "untertitel": "Abendkurs",
        "bezirk": "Mitte", "veranstaltungsart": "Kurs",
        "dvv_kategorie": {"@version": "2.0", "#text": "4.04"},
        "aktuelle_teilnehmerzahl": "4", "maximale_teilnehmerzahl": "16",
        "beginn_datum": SOON, "ende_datum": LATER, "schlagwort": ["A1", "Deutsch"],
        "text": [{"eigenschaft": "Beschreibung",
                  "text": "Für <b>Anfänger:innen</b>. Mail: <a href=\"mailto:x@vhs.example\">x@vhs.example</a>"},
                 {"eigenschaft": "Zusatzinformation", "text": "Bitte Stift mitbringen."}],
        "anmeldung": {"telefon": "030 902390", "mail": "anmeldung@vhs.example"},
        "ansprechperson": {"name": "Musterfrau", "vorname": "Erika", "mail": "erika@vhs.example"},
        "dozent": [{"name": "Beispiel", "vorname": "Max"}],
        "ortetermine": {
            "adresse": [{"lehrstaette": "VHS Berlin Mitte", "plz": "13347", "ort": "Berlin",
                         "strasse": "Antonstr. 37, 13347 Berlin", "raum": "2.05",
                         "laengengrad": "13.3611", "breitengrad": "52.5468"}] * 2,
            "termin": [{"beginn_datum": LATER, "beginn_uhrzeit": "18:00", "ende_uhrzeit": "20:15"},
                       {"beginn_datum": SOON, "beginn_uhrzeit": "18:00", "ende_uhrzeit": "20:15"}],
        },
        "preis": {"betrag": "62.50", "rabatt_moeglich": "true", "zusatz": "erm. Preis: 31,25 EUR"},
        "webadresse": {"typ": "website", "name": "zur Kursbuchung",
                       "uri": f"http://www.vhsit.berlin.de/VHSKURSE/BusinessPages/CourseDetail.aspx?id={guid}"},
    }
    course.update(over)
    return course


def test_a_course_is_listed_once_on_its_first_session_with_a_link_to_book():
    raw = to_raw(_course())
    assert raw.title == "Deutsch A1.1"
    assert raw.start == f"{SOON}T18:00:00" and raw.end == f"{SOON}T20:15:00"
    assert raw.source_url == "https://www.vhsit.berlin.de/VHSKURSE/BusinessPages/CourseDetail.aspx?id=1"
    assert (raw.venue_name, raw.street, raw.postal_code) == ("VHS Berlin Mitte", "Antonstr. 37", "13347")
    assert (raw.lat, raw.lon) == (52.5468, 13.3611)
    assert raw.price_value == 62.5 and "erm. Preis" in raw.price_text
    assert raw.description.startswith("Kurs mit 2 Terminen")
    assert "Abendkurs." in raw.description and "Anfänger:innen" in raw.description
    assert "<" not in raw.description and "href" not in raw.description
    assert raw.description_public, "cc-by open data"


def test_nothing_about_the_people_named_in_the_record_is_kept():
    raw = to_raw(_course())
    blob = json.dumps(raw.__dict__, default=str, ensure_ascii=False)
    for private in ("Musterfrau", "Erika", "erika@vhs.example", "Beispiel", "anmeldung@vhs.example",
                    "030 902390"):
        assert private not in blob, private
    assert set(raw.payload["vhs"]) == {"guid", "nummer", "bezirk", "sessions"}


def test_courses_that_are_not_somewhere_to_go_are_left_out():
    assert to_raw(_course(webadresse={"uri": ""})) is None                       # no page to link to
    assert to_raw(_course(webadresse=None)) is None
    assert to_raw(_course(veranstaltungsart="Beratung")) is None
    assert to_raw(_course(schlagwort=["Prüfungstermin", "B1"])) is None
    assert to_raw(_course(aktuelle_teilnehmerzahl="16")) is None                 # full
    online = _course()
    online["ortetermine"]["adresse"] = {"lehrstaette": "Online", "raum": "vhs.cloud"}
    assert to_raw(online) is None
    assert to_raw(_course(ortetermine=None)) is None


def test_single_objects_where_the_file_usually_has_lists_are_handled():
    course = _course(schlagwort="Kochen", untertitel=None)
    course["ortetermine"] = {
        "adresse": {"lehrstaette": "Lehrküche", "plz": "10829", "strasse": "Hauptstr. 1, 10829 Berlin"},
        "termin": {"beginn_datum": SOON, "beginn_uhrzeit": "17:00", "ende_uhrzeit": "21:00"},
    }
    course["preis"] = {"betrag": "0.00"}
    raw = to_raw(course)
    assert raw.venue_name == "Lehrküche" and raw.is_free is True
    assert not raw.description.startswith("Kurs mit")


def test_categories():
    ad = VhsAdapter(None, None)
    assert ad.category_prior == "workshops_classes"
    assert ad.map_category(to_raw(_course())) is None
    assert ad.map_category(to_raw(_course(dvv_kategorie={"#text": "3.05"}))) == "food_drink"
    assert ad.map_category(to_raw(_course(dvv_kategorie={"#text": "3.02"}))) == "sports_fitness"
    assert ad.map_category(to_raw(_course(veranstaltungsart="Einzelveranstaltung/Vortrag"))) \
        == "talks_literature"


def test_the_adapter_reads_the_one_file_and_yields_only_courses_starting_in_the_window():
    seen = []
    far = _course("9", beginn_datum=FAR)
    far["ortetermine"]["termin"] = [{"beginn_datum": FAR, "beginn_uhrzeit": "10:00"}]
    body = {"veranstaltungen": {"veranstaltung": [_course("1"), far, _course("3", webadresse=None)]}}

    def handler(request):
        seen.append(request)
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, json=body)

    f = Fetcher(cache_path=None)
    f.client = httpx.Client(transport=httpx.MockTransport(handler), headers=dict(f.client.headers))
    raws = list(VhsAdapter(f, connect(":memory:")).fetch_events(window_days=31))
    assert [r.source_event_id for r in raws] == ["1"]
    feed_calls = [r for r in seen if str(r.url) == FEED]
    assert len(feed_calls) == 1
    assert feed_calls[0].headers["user-agent"].startswith("events-pipeline/")


def test_the_course_file_is_kept_out_of_the_shared_http_cache(tmp_path):
    f = Fetcher(cache_path=str(tmp_path / "cache.db"))
    try:
        assert f._uncached is not None and f._uncached is not f.client
        assert f._uncached.headers["user-agent"] == f.client.headers["user-agent"]
        sent_on = []
        f._uncached = httpx.Client(transport=httpx.MockTransport(
            lambda r: sent_on.append("uncached") or httpx.Response(200, json={})))
        f.client = httpx.Client(transport=httpx.MockTransport(
            lambda r: sent_on.append("cached") or httpx.Response(404)))
        list(VhsAdapter(f, connect(":memory:")).fetch_events())
        assert sent_on == ["cached", "uncached"], "robots.txt through the cache, the file around it"
    finally:
        f.close()


def test_a_large_dataset_needs_its_cap_raised_explicitly(monkeypatch):
    import pytest
    import pipeline.fetch as fetch_mod
    monkeypatch.setattr(fetch_mod, "MAX_RESPONSE_BYTES", 64)

    def handler(request):
        if request.url.path == "/robots.txt":
            return httpx.Response(404)
        return httpx.Response(200, content=b"x" * 500)

    f = Fetcher(cache_path=None)
    f.client = httpx.Client(transport=httpx.MockTransport(handler))
    with pytest.raises(fetch_mod.FetchStallError):
        f.get("https://data.example/big.json")
    assert len(f.get("https://data.example/big.json", max_bytes=1000).content) == 500
    with pytest.raises(fetch_mod.FetchStallError):
        f.get("https://data.example/big.json", max_bytes=400)
