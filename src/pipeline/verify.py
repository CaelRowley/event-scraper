"""Per-run data sanity checks — cheap, deterministic guards against bad upstream data.

Fixes what is safely fixable (out-of-city geo → nulled, absurd prices → reverted to
raw text) and counts what is only worth watching (mojibake titles). Results land in
run telemetry so regressions are visible immediately.
"""

from __future__ import annotations

import json
import logging

log = logging.getLogger(__name__)

# generous city bounding boxes — catches geocoder garbage and leaked other-city events
CITY_BBOX = {
    "berlin": (52.30, 52.70, 12.90, 13.80),  # lat_min, lat_max, lon_min, lon_max
}
PRICE_CAP = 500.0  # above this it's almost always a mis-parse (year, phone number…)


def run_sanity_checks(conn, city: str) -> dict:
    stats = {"geo_nulled": 0, "price_reverted": 0, "mojibake_titles": 0}
    bbox = CITY_BBOX.get(city)

    if bbox:
        lat_min, lat_max, lon_min, lon_max = bbox
        cur = conn.execute(
            """UPDATE events SET lat=NULL, lon=NULL
               WHERE city=? AND lat IS NOT NULL
                 AND (lat < ? OR lat > ? OR lon < ? OR lon > ?)""",
            (city, lat_min, lat_max, lon_min, lon_max),
        )
        stats["geo_nulled"] = cur.rowcount

    for row in conn.execute(
        "SELECT id, price_json FROM events WHERE city=? AND price_json IS NOT NULL", (city,)
    ).fetchall():
        try:
            price = json.loads(row["price_json"])
        except json.JSONDecodeError:
            continue
        values = [v for v in (price.get("min"), price.get("max")) if v is not None]
        if values and max(values) > PRICE_CAP:
            # keep the raw text (recovery path), drop the numeric mis-parse
            reverted = {**price, "min": None, "max": None, "presale": None, "door": None,
                        "reduced": None, "type": "unknown"}
            conn.execute("UPDATE events SET price_json=? WHERE id=?",
                         (json.dumps(reverted), row["id"]))
            stats["price_reverted"] += 1

    stats["mojibake_titles"] = conn.execute(
        "SELECT COUNT(*) AS n FROM events WHERE city=? AND title LIKE '%�%'", (city,)
    ).fetchone()["n"]
    if stats["mojibake_titles"]:
        log.warning("%d titles contain U+FFFD replacement chars — check source encodings",
                    stats["mojibake_titles"])

    log.info("sanity checks: %s", stats)
    return stats
