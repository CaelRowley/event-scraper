"""Canonical venue table: alias resolution + category priors.

Seeded with hand-curated high-volume Berlin venues; extend via the `venues-bootstrap`
CLI command (Overpass/OSM pull, ODbL — attribute if redistributed).
"""

from __future__ import annotations

import json
import re
import unicodedata

from .ids import venue_id

# (canonical_name, aliases, category_prior)
BERLIN_SEED = [
    # clubs
    ("Berghain", ["Berghain / Panorama Bar", "Panorama Bar", "Säule", "Berghain Kantine", "Kantine am Berghain"], "club_nightlife"),
    ("://about blank", ["about blank", "about:blank"], "club_nightlife"),
    ("Tresor", ["Tresor Berlin", "Tresor.West"], "club_nightlife"),
    ("Sisyphos", [], "club_nightlife"),
    ("KitKatClub", ["KitKat Club", "KitKat"], "club_nightlife"),
    ("Salon zur Wilden Renate", ["Wilde Renate", "Renate"], "club_nightlife"),
    ("RSO.Berlin", ["RSO", "Revier Südost"], "club_nightlife"),
    ("Kater Blau", [], "club_nightlife"),
    ("Ritter Butzke", [], "club_nightlife"),
    ("Suicide Circus", ["Suicide Club"], "club_nightlife"),
    ("Anomalie Art Club", ["Anomalie"], "club_nightlife"),
    ("OXI", ["OXI Berlin"], "club_nightlife"),
    ("Else", ["ELSE"], "club_nightlife"),
    ("Klunkerkranich", [], "club_nightlife"),
    ("Golden Gate", [], "club_nightlife"),
    # live music
    ("SO36", [], "live_music"),
    ("Lido", [], "live_music"),
    ("Astra Kulturhaus", ["Astra"], "live_music"),
    ("Columbiahalle", ["Columbia Theater"], "live_music"),
    ("Huxleys Neue Welt", ["Huxleys", "Huxley's Neue Welt"], "live_music"),
    ("Festsaal Kreuzberg", [], "live_music"),
    ("Kesselhaus", ["Kesselhaus in der Kulturbrauerei", "Maschinenhaus"], "live_music"),
    ("Cassiopeia", [], "live_music"),
    ("Badehaus", ["Badehaus Szimpla"], "live_music"),
    ("Privatclub", [], "live_music"),
    ("Frannz Club", ["Frannz"], "live_music"),
    ("Quasimodo", [], "live_music"),
    ("A-Trane", ["A Trane"], "live_music"),
    ("Lark", [], "live_music"),
    ("Hole44", ["Hole 44"], "live_music"),
    ("Uber Arena", ["Mercedes-Benz Arena", "Mercedes Benz Arena"], "live_music"),
    ("Max-Schmeling-Halle", ["Max Schmeling Halle"], "live_music"),
    ("Velodrom", [], "live_music"),
    ("Tempodrom", [], "live_music"),
    ("Waldbühne", ["Waldbuehne"], "live_music"),
    ("Zitadelle Spandau", ["Zitadelle"], "live_music"),
    ("Philharmonie Berlin", ["Berliner Philharmonie", "Philharmonie", "Kammermusiksaal"], "live_music"),
    ("Konzerthaus Berlin", ["Konzerthaus"], "live_music"),
    # theatre / performance
    ("Volksbühne", ["Volksbühne am Rosa-Luxemburg-Platz", "Volksbuehne"], "theatre_performance"),
    ("Schaubühne", ["Schaubühne am Lehniner Platz", "Schaubuehne"], "theatre_performance"),
    ("Deutsches Theater", ["Deutsches Theater Berlin", "DT Berlin"], "theatre_performance"),
    ("Berliner Ensemble", [], "theatre_performance"),
    ("Maxim Gorki Theater", ["Gorki", "Gorki Theater"], "theatre_performance"),
    ("HAU Hebbel am Ufer", ["HAU", "HAU1", "HAU2", "HAU3", "Hebbel am Ufer"], "theatre_performance"),
    ("Sophiensæle", ["Sophiensaele", "Sophiensäle"], "theatre_performance"),
    ("Friedrichstadt-Palast", ["Friedrichstadtpalast"], "theatre_performance"),
    ("Admiralspalast", [], "theatre_performance"),
    ("Theater des Westens", [], "theatre_performance"),
    ("Komische Oper", ["Komische Oper Berlin"], "theatre_performance"),
    ("Staatsoper Unter den Linden", ["Staatsoper"], "theatre_performance"),
    ("Deutsche Oper Berlin", ["Deutsche Oper"], "theatre_performance"),
    ("Radialsystem", ["Radialsystem V"], "theatre_performance"),
    ("Dock 11", ["DOCK 11", "Dock11"], "theatre_performance"),
    # comedy / kabarett
    ("Bar jeder Vernunft", [], "comedy"),
    ("Tipi am Kanzleramt", ["Tipi"], "comedy"),
    ("Die Wühlmäuse", ["Wühlmäuse", "Wuehlmaeuse"], "comedy"),
    ("Distel", ["Kabarett-Theater Distel"], "comedy"),
    ("Mehringhof-Theater", ["Mehringhoftheater"], "comedy"),
    ("Quatsch Comedy Club", [], "comedy"),
    # art
    ("Gropius Bau", ["Martin-Gropius-Bau"], "art_exhibitions"),
    ("Hamburger Bahnhof", ["Hamburger Bahnhof – Nationalgalerie der Gegenwart"], "art_exhibitions"),
    ("C/O Berlin", ["CO Berlin"], "art_exhibitions"),
    ("KW Institute for Contemporary Art", ["KW", "Kunst-Werke"], "art_exhibitions"),
    ("König Galerie", ["Koenig Galerie", "St. Agnes"], "art_exhibitions"),
    ("Urban Spree", [], "art_exhibitions"),
    ("Fotografiska Berlin", ["Fotografiska"], "art_exhibitions"),
    ("Neue Nationalgalerie", [], "art_exhibitions"),
    ("Alte Nationalgalerie", [], "art_exhibitions"),
    ("Berlinische Galerie", [], "art_exhibitions"),
    # cinema
    ("Babylon", ["Babylon Mitte", "Kino Babylon"], "film_cinema"),
    ("Kino International", [], "film_cinema"),
    ("Freiluftkino Kreuzberg", [], "film_cinema"),
    ("Freiluftkino Friedrichshain", [], "film_cinema"),
    ("Arsenal", ["Kino Arsenal"], "film_cinema"),
    # literature / talks
    ("Literaturhaus Berlin", ["Literaturhaus"], "talks_literature"),
    ("Haus für Poesie", ["Haus fuer Poesie"], "talks_literature"),
    ("Urania", ["Urania Berlin"], "talks_literature"),
]


def norm_name(name: str) -> str:
    """NFC → casefold (ß→ss) → strip diacritics → strip punctuation/whitespace runs."""
    s = unicodedata.normalize("NFC", name).casefold()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if not unicodedata.combining(c))
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def seed_venues(conn) -> None:
    for canonical, aliases, prior in BERLIN_SEED:
        exists = conn.execute(
            "SELECT 1 FROM venues WHERE canonical_name=? AND city='berlin'", (canonical,)
        ).fetchone()
        if not exists:
            conn.execute(
                "INSERT INTO venues(id, canonical_name, aliases_json, city, category_prior) VALUES(?,?,?,?,?)",
                (venue_id(), canonical, json.dumps(aliases), "berlin", prior),
            )


class VenueIndex:
    """In-memory normalised-name → (venue_id, prior) lookup, built once per run."""

    def __init__(self, conn, city: str):
        self._exact: dict[str, tuple[str, str | None]] = {}
        for row in conn.execute("SELECT * FROM venues WHERE city=?", (city,)):
            entry = (row["id"], row["category_prior"])
            self._exact[norm_name(row["canonical_name"])] = entry
            for alias in json.loads(row["aliases_json"] or "[]"):
                self._exact[norm_name(alias)] = entry

    def resolve(self, venue_name: str | None) -> tuple[str | None, str | None]:
        if not venue_name:
            return None, None
        key = norm_name(venue_name)
        if key in self._exact:
            return self._exact[key]
        # containment pass: "Berghain / Panorama Bar" hits the "berghain" entry
        for known, entry in self._exact.items():
            if len(known) >= 5 and (known in key or key in known):
                return entry
        return None, None


OVERPASS_QUERY = """
[out:json][timeout:90];
area["name"="Berlin"]["admin_level"="4"]->.b;
(
  nwr(area.b)["amenity"~"^(nightclub|theatre|cinema|arts_centre)$"];
  nwr(area.b)["tourism"~"^(gallery|museum)$"];
);
out center tags;
"""

_OSM_PRIOR = {
    "nightclub": "club_nightlife", "theatre": "theatre_performance",
    "cinema": "film_cinema", "arts_centre": "art_exhibitions",
    "gallery": "art_exhibitions", "museum": "art_exhibitions",
}


def bootstrap_from_overpass(conn, fetcher) -> int:
    """One-off Overpass pull of Berlin venue POIs (ODbL — attribute if redistributed)."""
    resp = fetcher.post(
        "https://overpass-api.de/api/interpreter", data={"data": OVERPASS_QUERY},
        check_robots=False,
    )
    resp.raise_for_status()
    added = 0
    for el in resp.json().get("elements", []):
        tags = el.get("tags", {})
        name = tags.get("name")
        if not name:
            continue
        kind = tags.get("amenity") or tags.get("tourism")
        prior = _OSM_PRIOR.get(kind)
        if conn.execute(
            "SELECT 1 FROM venues WHERE city='berlin' AND canonical_name=?", (name,)
        ).fetchone():
            continue
        lat = el.get("lat") or el.get("center", {}).get("lat")
        lon = el.get("lon") or el.get("center", {}).get("lon")
        conn.execute(
            "INSERT INTO venues(id, canonical_name, aliases_json, lat, lon, city, osm_id, category_prior) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (venue_id(), name, "[]", lat, lon, "berlin", f"{el.get('type','n')}/{el.get('id')}", prior),
        )
        added += 1
    return added
