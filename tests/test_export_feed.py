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
        description=desc, description_public=bool(desc), image_url=img, occurrences=occ,
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


def test_unpresentable_rows_get_no_object_because_none_could_be_addressed():
    """A detail URL needs the `h` from a list row, and an unlisted event has none."""
    conn = _seed()
    built = _build(conn)
    hidden = conn.execute("SELECT id FROM events WHERE title='Nothing To Show'").fetchone()["id"]
    assert hidden not in {r["event_id"] for r in built["feed"][1]}
    assert not any(k.startswith(f"events/{hidden}.") for k in built["events"])


def test_occurrences_collapse_into_one_event_object():
    built = _build(_seed())
    row = next(r for r in built["feed"][1] if r["title"] == "Two Slots")
    two = built["events"][f"events/{row['event_id']}.{row['h']}.json.gz"]
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
        assert f"events/{row['event_id']}.{row['h']}.json.gz" in built["events"]


def test_private_fields_never_publish():
    built = _build(_seed())
    for payload in built["events"].values():
        assert not (PRIVATE_FIELDS & set(payload.keys()))
    for row in built["feed"][1]:
        assert not (PRIVATE_FIELDS & set(row.keys()))


def test_unchanged_data_produces_identical_hashes():
    """Unstable hashes would make every client re-download daily for nothing."""
    conn = _seed()
    a, b = _build(conn), _build(conn)
    assert a["manifest"]["feed"]["hash"] == b["manifest"]["feed"]["hash"]
    assert a["manifest"]["geo"]["hash"] == b["manifest"]["geo"]["hash"]
    assert sorted(a["events"]) == sorted(b["events"])


def test_changing_one_event_changes_only_its_object_and_the_feed():
    conn = _seed()
    before = _build(conn)
    target = conn.execute("SELECT id FROM events WHERE title='Has Image'").fetchone()["id"]
    conn.execute("UPDATE events SET description='Now with prose' WHERE id=?", (target,))
    conn.execute("UPDATE events SET description=NULL WHERE id != ?", (target,))
    after = _build(conn)

    assert before["manifest"]["feed"]["hash"] != after["manifest"]["feed"]["hash"]
    moved = {k for k in set(before["events"]) ^ set(after["events"])}
    assert all(k.startswith(f"events/{target}.") for k in moved), \
        f"only the edited event should get a new object name: {moved}"


def test_a_list_only_field_does_not_churn_the_detail_object():
    """Title lives on the list row, so renaming re-publishes the feed and nothing else."""
    conn = _seed()
    before = _build(conn)
    conn.execute("UPDATE events SET title='Renamed' WHERE title='Has Image'")
    after = _build(conn)
    assert before["manifest"]["feed"]["hash"] != after["manifest"]["feed"]["hash"]
    assert sorted(before["events"]) == sorted(after["events"])


def test_the_detail_object_carries_only_what_a_list_row_lacks():
    built = _build(_seed())
    allowed = {"event_id", "description", "occurrences", "aliases"}
    for key, payload in built["events"].items():
        assert set(payload) <= allowed, f"{key} carries redundant fields: {set(payload) - allowed}"


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


def test_manifest_carries_an_alias_to_canonical_map():
    conn = _seed()
    canonical = conn.execute("SELECT id FROM events WHERE title='Has Image'").fetchone()["id"]
    conn.execute("INSERT INTO event_aliases(alias_id, canonical_id, noted_at) VALUES (?,?,?)",
                 ("old-id", canonical, "2026-01-01T00:00:00Z"))
    built = _build(conn)
    assert built["manifest"]["aliases"] == {"old-id": canonical}
    obj = next(v for v in built["events"].values() if v["event_id"] == canonical)
    assert obj["aliases"] == ["old-id"]


def test_export_city_removes_superseded_hashed_objects(tmp_path: Path):
    conn = _seed()
    export_city(conn, "berlin", tmp_path)
    city = tmp_path / "berlin"
    conn.execute("UPDATE events SET title='Renamed' WHERE title='Has Image'")
    export_city(conn, "berlin", tmp_path)
    manifest = json.loads((city / "manifest.json").read_text())
    assert [p.name for p in city.glob("feed.*.json.gz")] == [manifest["feed"]["url"]]
    names = sorted(p.name for p in (city / "events").glob("*.json.gz"))
    # "Nothing To Show" is unlisted, so only the two presentable events have objects
    assert len(names) == 2 and len({n.split(".")[0] for n in names}) == 2, \
        f"one object per listed event, no stale hashes: {names}"


def test_buffer_day_is_scraped_but_not_published(tmp_path: Path):
    """The scrape horizon runs one day past the export horizon.

    An event on the buffer day is stored, so it gets a second pass before its
    first publication, but it must not reach the feed until the window slides
    onto it — otherwise the feed's last day is always the one discovered minutes
    earlier, the thinnest and least verified day in it.
    """
    from pipeline import config as cfg

    assert cfg.SCRAPE_WINDOW_DAYS == cfg.EXPORT_WINDOW_DAYS + 1

    edge = (date.today() + timedelta(days=cfg.EXPORT_WINDOW_DAYS)).isoformat()
    buffer_day = (date.today() + timedelta(days=cfg.SCRAPE_WINDOW_DAYS)).isoformat()

    conn = connect(":memory:")
    upsert_event(conn, _ev("edge", "Last Published Day", edge, 20, img="http://i/e"))
    upsert_event(conn, _ev("buffer", "One Day Too Far", buffer_day, 20, img="http://i/b"))
    conn.commit()

    export_city(conn, "berlin", tmp_path)
    city = tmp_path / "berlin"
    manifest = json.loads((city / "manifest.json").read_text())
    rows = json.loads(gzip.decompress((city / manifest["feed"]["url"]).read_bytes()))

    titles = {r["title"] for r in rows}
    assert "Last Published Day" in titles
    assert "One Day Too Far" not in titles, "the buffer day must not reach the feed"
    assert manifest["window"]["to"] == edge
