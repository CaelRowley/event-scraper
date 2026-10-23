from pipeline.adapters.kulturdaten import calendar_url


def test_calendar_url_from_bezirkskalender_attraction():
    attr = {"metadata": {"origin": "bezirkskalender", "originObjectID": "271366"}}
    assert calendar_url(attr) == "https://www.berlin.de/land/kalender/index.php?detail=271366"


def test_no_calendar_url_for_other_origins_or_missing_ids():
    assert calendar_url({}) is None
    assert calendar_url({"metadata": {"origin": "other", "originObjectID": "1"}}) is None
    assert calendar_url({"metadata": {"origin": "bezirkskalender", "originObjectID": ""}}) is None
    assert calendar_url({"metadata": {"origin": "bezirkskalender", "originObjectID": "x1"}}) is None
