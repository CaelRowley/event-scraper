"""Cross-thread behavior: per-domain throttle serialization, cross-domain parallelism,
multi-connection WAL writes, and runner worker isolation.

These use real (small) sleeps where threads are involved — FakeTime from test_fetch
is not thread-safe and a shared fake clock makes interleaving meaningless.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

import pipeline.config as cfg
import pipeline.runner as runner_mod
from pipeline.db import connect, ledger_get, ledger_put, upsert_event
from pipeline.fetch import Fetcher, RateSpec
from pipeline.models import Event, Occurrence, Price, RawEvent
from pipeline.adapters.base import SourceAdapter
from pipeline.runner import run


def _fetcher(handler):
    f = Fetcher(cache_path=None)
    f.client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True)
    return f


def test_same_domain_requests_serialize_across_threads():
    starts = []

    def handler(request):
        starts.append(time.monotonic())
        return httpx.Response(200)

    fetcher = _fetcher(handler)
    rate = RateSpec(min_interval=0.15, jitter=(0, 0))

    def hit():
        fetcher.get("https://one.example/x", rate=rate, check_robots=False)

    threads = [threading.Thread(target=hit) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    gaps = [b - a for a, b in zip(sorted(starts), sorted(starts)[1:])]
    assert all(g >= 0.14 for g in gaps), gaps


def test_cross_domain_requests_run_in_parallel():
    def handler(request):
        time.sleep(0.3)
        return httpx.Response(200)

    fetcher = _fetcher(handler)
    rate = RateSpec(min_interval=1.0, jitter=(0, 0))
    t0 = time.monotonic()

    def hit(domain):
        fetcher.get(f"https://{domain}/x", rate=rate, check_robots=False)

    threads = [threading.Thread(target=hit, args=(d,))
               for d in ("a.example", "b.example", "c.example")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # sequential would be >= 0.9s; parallel domains overlap their handler sleeps
    assert time.monotonic() - t0 < 0.6


def test_requests_made_counter_is_exact_under_threads():
    fetcher = _fetcher(lambda request: httpx.Response(200))
    rate = RateSpec(min_interval=0, jitter=(0, 0))

    def hit(i):
        for j in range(25):
            fetcher.get(f"https://d{i}.example/{j}", rate=rate, check_robots=False)

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(hit, range(8)))
    assert fetcher.requests_made == 200


def _event(source, sid):
    return Event(
        id="", source=source, source_event_id=sid, source_url=f"https://{source}.example/{sid}",
        title=f"Event {sid}", city="berlin", price=Price(),
        occurrences=[Occurrence(starts_at_utc="2199-06-13T20:00:00Z",
                                starts_at_local="2199-06-13T22:00:00+02:00",
                                nightlife_date="2199-06-13")],
    )


def test_wal_multi_connection_writes(tmp_path):
    db = str(tmp_path / "test.db")
    connect(db).close()  # create schema once
    errors = []

    def writer(i):
        conn = connect(db)
        try:
            for j in range(50):
                upsert_event(conn, _event(f"src{i}", f"e{j}"))
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=6) as ex:
        list(ex.map(writer, range(6)))

    assert not errors
    conn = connect(db)
    assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 300


def test_busy_timeout_lets_second_writer_wait(tmp_path):
    db = str(tmp_path / "test.db")
    connect(db).close()

    holder = connect(db)
    holder.execute("INSERT INTO kv(key, value) VALUES('a', '1')")  # open write tx, no commit
    done = []

    def second():
        conn = connect(db)
        conn.execute("INSERT INTO kv(key, value) VALUES('b', '2')")
        conn.commit()
        conn.close()
        done.append(True)

    t = threading.Thread(target=second)
    t.start()
    time.sleep(0.2)
    holder.commit()  # release — the second writer must succeed via busy_timeout, not error
    t.join(timeout=10)
    holder.close()
    assert done == [True]


def test_ledger_put_never_leaves_a_transaction_open(tmp_path):
    """Adapters call ledger_put between HTTP fetches; an open write tx spanning a
    fetch starved every other worker at the WAL lock (the 2026-07-18 CI failure)."""
    db = str(tmp_path / "t.db")
    connect(db).close()
    a = connect(db)
    ledger_put(a, "test", "https://x.example/p", status=200)
    assert not a.in_transaction  # committed — holds no write lock
    b = connect(db)  # and the row is durably visible to other connections
    assert ledger_get(b, "test", "https://x.example/p") is not None
    a.close()
    b.close()


class FakeAdapterOK(SourceAdapter):
    slug = "fake_ok"

    def fetch_events(self, window_days=14, limit=None):
        for i in range(2):
            yield RawEvent(source=self.slug, source_event_id=f"ok{i}",
                           source_url=f"https://ok.example/{i}", title=f"Konzert {i}",
                           start="2199-06-13T20:00:00")


class FakeAdapterBoom(SourceAdapter):
    slug = "fake_boom"

    def fetch_events(self, window_days=14, limit=None):
        yield RawEvent(source=self.slug, source_event_id="b0",
                       source_url="https://boom.example/0", title="Pre-failure Konzert",
                       start="2199-06-13T20:00:00")
        raise RuntimeError("source exploded")


def test_runner_isolates_adapter_failures(tmp_path, monkeypatch):
    db = str(tmp_path / "run.db")
    monkeypatch.setattr(cfg, "HTTP_CACHE_PATH", None)
    monkeypatch.setattr(cfg, "PUBLIC_DIR", str(tmp_path / "public"))
    monkeypatch.setattr(cfg, "select_adapters",
                        lambda city, **kw: [("fake_ok", FakeAdapterOK),
                                            ("fake_boom", FakeAdapterBoom)])
    # the post-dedup phases would hit the network — stub them out
    for name in ("backfill_images", "check_links", "check_images"):
        monkeypatch.setattr(runner_mod, name, lambda conn, fetcher, city: {})

    telemetry = run("berlin", no_llm=True, db_path=db)

    assert list(telemetry["errors"]) == ["fake_boom"]
    assert telemetry["sources"]["fake_ok"] == 2
    conn = connect(db)
    # the failing adapter's pre-failure event must have been committed
    assert conn.execute(
        "SELECT COUNT(*) FROM events WHERE source_slug='fake_boom'").fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM events WHERE source_slug='fake_ok'").fetchone()[0] == 2
