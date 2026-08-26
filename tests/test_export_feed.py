"""The published feed shape: content-addressing, list/detail split, counts."""
import gzip
import json
from datetime import date, timedelta
from pathlib import Path

from pipeline.db import connect, upsert_event
from pipeline.export import LIST_FIELDS, PRIVATE_FIELDS, build_feed, export_city
from pipeline.models import Event, Occurrence, Price

DAY1 = date.today().isoformat()
DAY2 = (date.today() + timedelta(days=1)).isoformat()


def _ev(sid, title, day, hour, *, desc=None, img=None, lat=None, lon=None, url=None, extra_hours=()):
    occ = [
        Occurrence(starts_at_utc=f"{d}T{h:02d}:00:00Z",
                   starts_at_local=f"{d}T{h:02d}:00:00+02:00", nightlife_date=d)
        for d, h in [(day, hour)] + [(day, x) for x in extra_hours]
    ]
    return Event(
        id="", source="ra", source_event_id=sid, source_url=url or f"https://ra.example/{sid}",
        title=title, city="berlin", venue_name="V", lat=lat, lon=lon, price=Price(),
        description=desc, image_url=img, occurrences=occ,
    )


def _seed():
    conn = connect(":memory:")
    upsert_event(conn, _ev("1", "Has Image", DAY1, 22, img="http://i/1", lat=52.5, lon=13.4))
    upsert_event(conn, _ev("2", "Nothing To Show", DAY1, 20))
    upsert_event(conn, _ev("3", "Two Slots", DAY2, 18, img="http://i/3", extra_hours=(21,)))
    conn.commit()
    return conn


def _build(conn):
    from pipeline.export import _row_to_item
    rows = conn.execute(
        """SELECT e.*, o.starts_at_utc, o.ends_at_utc, o.starts_at_local, o.doors_at_local,
                  o.nightlife_date, o.time_unknown, o.status AS occ_status
           FROM occurrences o JOIN events e ON e.id = o.event_id
           WHERE e.city='berlin' AND e.canonical_id = e.id ORDER BY o.starts_at_utc"""
    ).fetchall()
    items = [_row_to_item(r) for r in rows]
    meta = {"generated_at": "2026-01-01T00:00:00Z",
            "window": {"from": DAY1, "to": DAY2}, "attribution": "x"}
    return build_feed(conn, "berlin", items, meta)


def test_list_rows_carry_only_list_fields_and_no_description():
    built = _build(_seed())
    rows = built["feed"][1]
    assert rows, "expected presentable rows"
    for r in rows:
        assert set(r.keys()) == set(LIST_FIELDS)
        assert "description" not in r


def test_unpresentable_rows_are_hidden_from_the_list_but_still_get_an_object():
    built = _build(_seed())
    listed = {r["title"] for r in built["feed"][1]}
    assert "Nothing To Show" not in listed
    # ...but a deep link to it must still open.
    objects = {json.loads(json.dumps(v))["title"] for v in built["events"].values()}
    assert "Nothing To Show" in objects


def test_occurrences_collapse_into_one_event_object():
    built = _build(_seed())
    two = [v for v in built["events"].values() if v["title"] == "Two Slots"][0]
    assert len(two["occurrences"]) == 2
    # ...but remain separate, addressable rows in the list.
    assert sum(1 for r in built["feed"][1] if r["title"] == "Two Slots") == 2


def test_day_and_category_counts_count_events_not_occurrences():
    built = _build(_seed())
    day2 = [d for d in built["manifest"]["days"] if d["date"] == DAY2][0]
    assert day2["count"] == 1, "two slots on one night is one card"
    assert sum(c["count"] for c in built["manifest"]["categories"].values()) == 2


def test_list_row_hash_addresses_an_existing_event_object():
    built = _build(_seed())
    for row in built["feed"][1]:
        assert f"events/{row['event_id']}.{row['h']}.json" in built["events"]


def test_private_fields_never_publish():
    built = _build(_seed())
    for payload in built["events"].values():
        assert not (PRIVATE_FIELDS & set(payload.keys()))
    for row in built["feed"][1]:
        assert not (PRIVATE_FIELDS & set(row.keys()))


def test_unchanged_data_produces_identical_hashes():
    """Unstable hashes would make every client re-download daily for nothing.

    Same database exported twice — ids are ULIDs minted at insert time, so this
    has to reuse one connection rather than reseed.
    """
    conn = _seed()
    a, b = _build(conn), _build(conn)
    assert a["manifest"]["feed"]["hash"] == b["manifest"]["feed"]["hash"]
    assert a["manifest"]["geo"]["hash"] == b["manifest"]["geo"]["hash"]
    assert sorted(a["events"]) == sorted(b["events"])


def test_changing_one_event_changes_only_its_object_and_the_feed():
    conn = _seed()
    before = _build(conn)
    conn.execute("UPDATE events SET title='Renamed' WHERE title='Has Image'")
    after = _build(conn)
    assert before["manifest"]["feed"]["hash"] != after["manifest"]["feed"]["hash"]
    unchanged = set(before["events"]) & set(after["events"])
    assert unchanged, "the untouched event should keep its content-addressed name"
    assert any(v["title"] == "Two Slots" for v in
               (before["events"][k] for k in unchanged))


def test_export_city_writes_manifest_last_and_gzips_the_feed(tmp_path: Path):
    conn = _seed()
    export_city(conn, "berlin", tmp_path)
    city = tmp_path / "berlin"
    manifest = json.loads((city / "manifest.json").read_text())
    feed_path = city / manifest["feed"]["url"]
    assert feed_path.exists()
    rows = json.loads(gzip.decompress(feed_path.read_bytes()))
    assert len(rows) == manifest["feed"]["count"]
    # every object the manifest names is on disk
    assert (city / manifest["geo"]["url"]).exists()
