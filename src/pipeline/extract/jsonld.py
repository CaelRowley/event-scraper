"""schema.org Event JSON-LD extraction: selectolax → json.loads → chompjs fallback."""

from __future__ import annotations

import json
import logging

import chompjs
from selectolax.parser import HTMLParser

log = logging.getLogger(__name__)

# The 23 schema.org Event subtypes (verified 2026-06-10) plus the generic type.
EVENT_TYPES = {
    "Event", "BusinessEvent", "ChildrensEvent", "ComedyEvent", "ConferenceEvent",
    "CourseInstance", "DanceEvent", "DeliveryEvent", "EducationEvent", "EventSeries",
    "ExhibitionEvent", "Festival", "FoodEvent", "Hackathon", "LiteraryEvent",
    "MusicEvent", "PerformingArtsEvent", "PublicationEvent", "SaleEvent",
    "ScreeningEvent", "SocialEvent", "SportsEvent", "TheaterEvent", "VisualArtsEvent",
}


def _norm_type(value) -> str | None:
    """@type may be a string, a list, or a full URL."""
    if isinstance(value, list):
        for v in value:
            t = _norm_type(v)
            if t:
                return t
        return None
    if isinstance(value, str):
        t = value.rsplit("/", 1)[-1].rsplit(":", 1)[-1]
        return t if t in EVENT_TYPES else None
    return None


def _walk(node, found: list[dict]) -> None:
    if isinstance(node, dict):
        if _norm_type(node.get("@type")):
            found.append(node)
        else:
            for key in ("@graph", "itemListElement", "item", "mainEntity"):
                if key in node:
                    _walk(node[key], found)
    elif isinstance(node, list):
        for item in node:
            _walk(item, found)


def events_from_html(html: str | bytes) -> list[dict]:
    """All schema.org Event objects embedded in a page's ld+json blocks."""
    tree = HTMLParser(html)
    found: list[dict] = []
    for node in tree.css('script[type="application/ld+json"]'):
        text = node.text(strip=False)
        if not text or "vent" not in text:  # cheap pre-filter: *Event / event
            continue
        data = None
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            try:  # malformed blocks: trailing commas, JS-isms
                data = chompjs.parse_js_object(text)
            except Exception:
                log.debug("unparseable ld+json block (%d bytes)", len(text))
        if data is not None:
            _walk(data, found)
    return found


def first_of(node: dict, *keys, default=None):
    for k in keys:
        v = node.get(k)
        if v not in (None, "", []):
            return v
    return default


def text_value(value) -> str | None:
    """schema.org values may be strings, lists, or {'@value': ...} / {'name': ...} objects."""
    if value is None:
        return None
    if isinstance(value, list):
        return text_value(value[0]) if value else None
    if isinstance(value, dict):
        return text_value(value.get("name") or value.get("@value"))
    return str(value).strip() or None
