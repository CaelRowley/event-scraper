from pipeline.categorise.llm import OUTPUT_SCHEMA
from pipeline.categorise.rules import FAMILY_PRIORITY, classify, classify_best_effort
from pipeline.categorise.taxonomy import CATEGORIES, map_ticketmaster
from pipeline.models import Event, Price


def _event(title, schema_type=None, description=None):
    return Event(
        id="", source="test", source_event_id="1", source_url="https://x", title=title,
        city="berlin", source_category_raw=schema_type, price=Price(), description=description,
    )


def test_source_prior_wins():
    c = classify(_event("Anything At All"), source_prior="club_nightlife")
    assert c.category == "club_nightlife" and c.tier == "source_prior"


def test_schema_type_mapping():
    c = classify(_event("Some Show", schema_type="TheaterEvent"))
    assert c.category == "theatre_performance" and c.tier == "source_field"


def test_dance_event_resolved_by_venue_prior():
    club = classify(_event("X", schema_type="DanceEvent"), venue_prior="club_nightlife")
    stage = classify(_event("X", schema_type="DanceEvent"), venue_prior=None)
    assert club.category == "club_nightlife"
    assert stage.category == "theatre_performance"


def test_generic_event_falls_to_venue_prior():
    c = classify(_event("Untitled Evening", schema_type="Event"), venue_prior="art_exhibitions")
    assert c.category == "art_exhibitions" and c.tier == "venue_prior"


def test_single_keyword_family_fires():
    c = classify(_event("Vernissage: Neue Malerei aus Mitte"))
    assert c.category == "art_exhibitions" and c.tier == "keyword"


def test_multi_family_goes_to_llm():
    # both club (party) and live music (konzert) keywords → ambiguous → None
    assert classify(_event("Konzert + Aftershow-Party")) is None


def test_no_signal_goes_to_llm():
    assert classify(_event("Zusammen am Mittwoch")) is None


def test_tags_extracted_alongside():
    c = classify(_event("Open Air Konzert im Park"))
    assert c.category == "live_music" and "open-air" in c.tags


def test_ticketmaster_music_splits_on_genre():
    assert map_ticketmaster("Music", "Dance/Electronic") == "club_nightlife"
    assert map_ticketmaster("Music", "Rock") == "live_music"
    assert map_ticketmaster("Arts & Theatre", "Comedy") == "comedy"
    assert map_ticketmaster("Sports", None) == "sports_fitness"


def test_llm_schema_enums_match_taxonomy():
    assert OUTPUT_SCHEMA["properties"]["category"]["enum"] == CATEGORIES


# --- rules-only mode (no AI key) -------------------------------------------------

def test_best_effort_agrees_with_classify_when_unambiguous():
    ev = _event("Vernissage: Neue Malerei aus Mitte")
    assert classify_best_effort(ev).category == classify(ev).category


def test_best_effort_resolves_multi_match():
    ev = _event("Konzert + Aftershow-Party")
    assert classify(ev) is None  # AI mode defers this to the LLM
    cls = classify_best_effort(ev)  # rules mode takes its best shot
    assert cls is not None and cls.tier == "keyword_multi" and cls.confidence == 0.5
    assert cls.category in ("live_music", "club_nightlife")


def test_best_effort_match_count_wins():
    ev = _event("Rave! DJ-Set, Techno, Afterhour — und ein Konzert vorweg")
    assert classify_best_effort(ev).category == "club_nightlife"


def test_best_effort_zero_signal_stays_none():
    assert classify_best_effort(_event("Zusammen am Mittwoch")) is None


def test_family_priority_covers_all_keyword_families():
    import yaml
    from pathlib import Path
    import pipeline.categorise.rules as rules_mod

    families = yaml.safe_load(
        (Path(rules_mod.__file__).parent / "keywords.yaml").read_text()
    )["families"]
    assert set(FAMILY_PRIORITY) == set(families)
