from pipeline.sync_d1 import (
    BUDGET_BYTES,
    COLUMNS,
    CONTENT_COLUMNS,
    _row_dict,
    _sql_literal,
    content_hash,
    iter_delete_batches,
    iter_insert_batches,
)


def _item(**over):
    base = {
        "id": "ev1",
        "title": "Test Show",
        "category": "live_music",
        "category_label": "Live Music",
        "tags": ["free-entry"],
        "starts_at_utc": "2026-06-23T20:00:00Z",
        "starts_at_local": "2026-06-23T22:00:00+02:00",
        "ends_at_utc": None,
        "doors_at_local": None,
        "nightlife_date": "2026-06-23",
        "time_unknown": False,
        "is_range": False,
        "range_start": None,
        "range_end": None,
        "event_status": "scheduled",
        "venue_name": "Club X",
        "address": {"city": "Berlin", "country": "DE"},
        "geo": {"lat": 52.5, "lon": 13.4},
        "price": {"is_free": True},
        "image_url": None,
        "placeholder_url": "https://loremflickr.com/x",
        "description": None,
        "source": "ra",
        "source_url": "https://ra.co/events/1",
        "source_is_record": False,
    }
    base.update(over)
    return base


def _row_values(item, city="berlin", h="h", synced="2026-06-23T00:00:00Z"):
    d = _row_dict(item, city)
    return [d[c] for c in CONTENT_COLUMNS] + [h, synced]


def test_row_dict_shape():
    d = _row_dict(_item(), "berlin")
    assert d["id"] == "ev1::2026-06-23T20:00:00Z"   # occurrence-unique
    assert d["event_id"] == "ev1"
    assert d["city"] == "berlin"
    assert d["time_unknown"] == 0 and d["source_is_record"] == 0   # bools coerced
    assert d["lat"] == 52.5 and d["lon"] == 13.4                   # geo flattened


def test_geo_none_becomes_null_latlon():
    d = _row_dict(_item(geo=None), "berlin")
    assert d["lat"] is None and d["lon"] is None


def test_row_values_align_with_columns():
    assert len(_row_values(_item())) == len(COLUMNS)


def test_content_hash_ignores_synced_and_detects_change():
    d = _row_dict(_item(), "berlin")
    h1 = content_hash(d)
    # same content → same hash (synced_at is not part of the hash)
    assert content_hash(_row_dict(_item(), "berlin")) == h1
    # a real change → different hash
    assert content_hash(_row_dict(_item(title="Different"), "berlin")) != h1


def test_sql_literal_escaping():
    assert _sql_literal(None) == "NULL"
    assert _sql_literal(True) == "1" and _sql_literal(False) == "0"
    assert _sql_literal(42) == "42"
    assert _sql_literal("it's") == "'it''s'"          # quote doubled
    assert _sql_literal(float("inf")) == "NULL"        # non-finite guarded


def test_insert_batches_pack_and_cover_all_rows():
    rows = [_row_values(_item(id=f"ev{i}")) for i in range(500)]
    batches = list(iter_insert_batches(rows))
    assert len(batches) >= 1
    for b in batches:
        assert len(b) <= BUDGET_BYTES
        assert "ON CONFLICT(id) DO UPDATE SET" in b
        assert "?" not in b                            # fully inlined, no bound params
        # id/event_id are immutable on conflict
        assert "id=excluded.id" not in b
        assert "event_id=excluded.event_id" not in b
    # every row lands in exactly one batch
    assert sum(b.count("),(") + 1 for b in batches) == len(rows)


def test_delete_batches_pack_and_inline():
    ids = [f"ev{i}::t" for i in range(2000)]
    batches = list(iter_delete_batches(ids))
    assert len(batches) >= 1
    for b in batches:
        assert len(b) <= BUDGET_BYTES
        assert b.startswith("DELETE FROM city_events WHERE id IN (")
        assert "?" not in b


def test_empty_batches():
    assert list(iter_insert_batches([])) == []
    assert list(iter_delete_batches([])) == []
