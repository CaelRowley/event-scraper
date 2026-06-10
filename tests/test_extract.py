import json

from pipeline.extract.braces import extract_json_object
from pipeline.extract.jsonld import events_from_html


def test_jsonld_basic_event():
    html = """<html><head><script type="application/ld+json">
    {"@context":"https://schema.org","@type":"MusicEvent","name":"Test Gig",
     "startDate":"2026-06-14T20:00:00+02:00",
     "location":{"@type":"Place","name":"SO36"}}
    </script></head><body></body></html>"""
    events = events_from_html(html)
    assert len(events) == 1 and events[0]["name"] == "Test Gig"


def test_jsonld_graph_and_list():
    html = """<script type="application/ld+json">
    {"@graph":[{"@type":"Event","name":"A","startDate":"2026-06-14"},
               {"@type":"WebSite","name":"ignored"}]}
    </script>
    <script type="application/ld+json">
    [{"@type":"TheaterEvent","name":"B","startDate":"2026-06-15"}]
    </script>"""
    names = {e["name"] for e in events_from_html(html)}
    assert names == {"A", "B"}


def test_jsonld_type_as_url_and_list():
    html = """<script type="application/ld+json">
    {"@type":["https://schema.org/MusicEvent"],"name":"C","startDate":"2026-06-14"}
    </script>"""
    assert events_from_html(html)[0]["name"] == "C"


def test_jsonld_malformed_falls_back_to_chompjs():
    html = """<script type="application/ld+json">
    {"@type":"Event","name":"Trailing Comma Event","startDate":"2026-06-14",}
    </script>"""
    events = events_from_html(html)
    assert len(events) == 1 and events[0]["name"] == "Trailing Comma Event"


def test_brace_matcher_full_blob():
    inner = {"a": {"deep": [1, 2, {"s": 'quote " and brace } inside'}]}, "n": 5}
    text = f"<script>window.__SERVER_DATA__ = {json.dumps(inner)};</script>"
    assert extract_json_object(text, "__SERVER_DATA__") == inner


def test_brace_matcher_escaped_quotes():
    text = r'__SERVER_DATA__ = {"k": "va\"lue}", "x": 1};'
    assert extract_json_object(text, "__SERVER_DATA__") == {"k": 'va"lue}', "x": 1}


def test_brace_matcher_missing_marker():
    assert extract_json_object("nothing here", "__SERVER_DATA__") is None
