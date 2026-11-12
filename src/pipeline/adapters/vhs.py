"""Berliner Volkshochschulen — open course data (Berlin Open Data, `cc-by`).

One JSON file holds every course of the twelve district adult-education centres,
refreshed hourly by their service centre: German and other language courses,
cooking, crafts, movement, talks. Cheap, open to everyone, and where a lot of
newcomers actually start — which is the audience.

https://www.vhsit.berlin.de/VHSKURSE/OpenData/Kurse.json  (~48 MB, ~10,000 courses)

What becomes an event: a course whose *first* session falls inside the window —
that is when it can still be joined. It is listed once, on that first session;
the description says how many sessions follow. Left out, and so never stored:

* courses with no booking page (`webadresse.uri`) — nothing to link to;
* online courses, counselling slots and exam sittings — not things to go to;
* courses that are already full.

The file also names each course's contact person, its teachers, and a phone
number and email for registration. None of that is read into an event or kept.
"""

from __future__ import annotations

import html
import logging
import re
from collections.abc import Iterator
from datetime import date, timedelta

from ..fetch import RateSpec
from ..models import RawEvent
from .base import SourceAdapter, has_external_link

log = logging.getLogger(__name__)

FEED = "https://www.vhsit.berlin.de/VHSKURSE/OpenData/Kurse.json"
# The file is far over the fetcher's default body cap, which exists to stop a
# page trickling bytes forever — not to refuse a published dataset its size.
MAX_BYTES = 120 * 1024 * 1024

SKIP_KINDS = frozenset({"Beratung", "Prüfungsbezogener Kurs"})
SKIP_KEYWORDS = frozenset({"Prüfungstermin"})

_TAG = re.compile(r"<[^>]+>")
_HREF = re.compile(r'href="[^"]*"?')


def _text(value: str | None) -> str:
    """Course prose arrives with inline HTML and the odd broken `href=` fragment."""
    if not value:
        return ""
    value = _HREF.sub(" ", _TAG.sub(" ", value))
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def _as_list(value) -> list:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _float(value) -> float | None:
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _is_online(address: dict) -> bool:
    blob = " ".join(str(address.get(k) or "") for k in ("lehrstaette", "raum", "strasse")).lower()
    return "online" in blob or "digital" in blob


def _is_full(course: dict) -> bool:
    try:
        return int(course["aktuelle_teilnehmerzahl"]) >= int(course["maximale_teilnehmerzahl"]) > 0
    except (KeyError, TypeError, ValueError):
        return False


def to_raw(course: dict, *, slug: str = "vhs") -> RawEvent | None:
    """One course → one RawEvent on its first session, or None if it is left out."""
    title = (course.get("name") or "").strip()
    link = ((course.get("webadresse") or {}).get("uri") or "").strip()
    guid = str(course.get("guid") or "").strip()
    if not title or not guid or not has_external_link(link):
        return None
    if course.get("veranstaltungsart") in SKIP_KINDS or _is_full(course):
        return None
    if SKIP_KEYWORDS & {str(k) for k in _as_list(course.get("schlagwort"))}:
        return None

    where = course.get("ortetermine") or {}
    sessions = sorted(
        (s for s in _as_list(where.get("termin")) if isinstance(s, dict) and s.get("beginn_datum")),
        key=lambda s: (s["beginn_datum"], s.get("beginn_uhrzeit") or ""),
    )
    if not sessions:
        return None
    address = next((a for a in _as_list(where.get("adresse")) if isinstance(a, dict)), {})
    if _is_online(address):
        return None

    first = sessions[0]
    day, begins, ends = first["beginn_datum"], first.get("beginn_uhrzeit"), first.get("ende_uhrzeit")
    start = f"{day}T{begins}:00" if begins else day
    end = f"{day}T{ends}:00" if begins and ends and ends > begins else None

    subtitle = (course.get("untertitel") or "").strip()
    body = next((_text(t.get("text")) for t in _as_list(course.get("text"))
                 if isinstance(t, dict) and t.get("eigenschaft") == "Beschreibung"), "")
    lead = ""
    if len(sessions) > 1:
        last = sessions[-1]["beginn_datum"]
        lead = (f"Kurs mit {len(sessions)} Terminen, "
                f"{date.fromisoformat(day):%d.%m.%Y} bis {date.fromisoformat(last):%d.%m.%Y}.")
    if subtitle and subtitle[-1] not in ".!?":
        subtitle += "."
    description = " ".join(p for p in (lead, subtitle, body) if p)[:1500] or None

    price = course.get("preis") or {}
    amount = _float(price.get("betrag"))
    extra = (price.get("zusatz") or "").strip()
    price_text = None
    if amount is not None:
        price_text = f"{amount:g} €" + (f" ({extra})" if extra else "")

    street = (address.get("strasse") or "").split(",")[0].strip() or None
    return RawEvent(
        source=slug,
        source_event_id=guid,
        source_url=link.replace("http://www.vhsit.berlin.de/", "https://www.vhsit.berlin.de/"),
        title=title,
        start=start,
        end=end,
        date_only=not begins,
        venue_name=(address.get("lehrstaette") or "").strip() or None,
        street=street,
        postal_code=(address.get("plz") or "").strip() or None,
        city=(address.get("ort") or "").strip() or "Berlin",
        lat=_float(address.get("breitengrad")),
        lon=_float(address.get("laengengrad")),
        price_value=amount,
        price_text=price_text,
        is_free=True if amount == 0 else None,
        category_raw=f"{(course.get('dvv_kategorie') or {}).get('#text') or ''}|"
                     f"{course.get('veranstaltungsart') or ''}",
        description=description,
        description_public=True,  # cc-by open data — published with its credit
        # Facts only. The contact person, teachers and registration phone/email
        # in the record are not copied anywhere.
        payload={"vhs": {"guid": guid, "nummer": course.get("nummer"),
                         "bezirk": course.get("bezirk"), "sessions": len(sessions)}},
    )


class VhsAdapter(SourceAdapter):
    slug = "vhs"
    category_prior = "workshops_classes"
    rate = RateSpec(min_interval=2.0, jitter=(0.5, 1.5))
    cheap_delta = False  # one large file; the nightly full run is enough

    def fetch_events(self, window_days: int = 14, limit: int | None = None) -> Iterator[RawEvent]:
        today = date.today().isoformat()
        horizon = (date.today() + timedelta(days=window_days)).isoformat()
        # cache=False: a 48 MB body is re-downloaded daily anyway and would only
        # bloat the HTTP cache every other request shares.
        resp = self.get(FEED, max_bytes=MAX_BYTES, cache=False)
        resp.raise_for_status()
        courses = ((resp.json().get("veranstaltungen") or {}).get("veranstaltung")) or []
        yielded = 0
        for course in _as_list(courses):
            # the cheap date test first — nine courses in ten are outside the window
            if not today <= (course.get("beginn_datum") or "") <= horizon:
                continue
            raw = to_raw(course, slug=self.slug)
            if raw is None or not today <= str(raw.start)[:10] <= horizon:
                continue
            yielded += 1
            yield raw
            if limit and yielded >= limit:
                return

    def map_category(self, raw: RawEvent) -> str | None:
        code, _, kind = (raw.category_raw or "").partition("|")
        if kind == "Einzelveranstaltung/Vortrag":
            return "talks_literature"
        if code == "3.05":
            return "food_drink"        # cooking and nutrition
        if code in ("3.01", "3.02"):
            return "sports_fitness"    # movement, yoga, relaxation
        return None                    # → the source prior: workshops_classes
