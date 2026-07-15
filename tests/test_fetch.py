"""Fetcher wall-clock/size caps (FetchStallError) and the ensure_scheme helper."""

import httpx
import pytest

import pipeline.fetch as fetch_mod
from pipeline.adapters.base import ensure_scheme
from pipeline.fetch import Fetcher, FetchStallError, RateSpec


class FakeTime:
    """Stands in for the time module inside pipeline.fetch — bodies can 'trickle'
    for simulated minutes without the test taking real time."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class TrickleStream(httpx.SyncByteStream):
    """Yields small chunks, advancing the fake clock between them — mimics a server
    that keeps the connection alive but never finishes the body."""

    def __init__(self, faketime, step_seconds, chunks=1000):
        self.faketime = faketime
        self.step = step_seconds
        self.chunks = chunks

    def __iter__(self):
        for _ in range(self.chunks):
            self.faketime.now += self.step
            yield b"x" * 16


def _fetcher(handler):
    f = Fetcher(cache_path=None)
    f.client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True)
    return f


def test_trickling_body_raises_stall_and_never_retries(monkeypatch):
    faketime = FakeTime()
    monkeypatch.setattr(fetch_mod, "time", faketime)
    calls = []

    def handler(request):
        calls.append(request.url)
        return httpx.Response(200, stream=TrickleStream(faketime, step_seconds=10.0))

    fetcher = _fetcher(handler)
    with pytest.raises(FetchStallError):
        fetcher.get("https://slow.example/page", check_robots=False)
    assert len(calls) == 1  # non-retryable: one stall costs one attempt


def test_oversized_body_raises_stall(monkeypatch):
    monkeypatch.setattr(fetch_mod, "MAX_RESPONSE_BYTES", 64)

    def handler(request):
        return httpx.Response(200, content=b"x" * 65)

    fetcher = _fetcher(handler)
    with pytest.raises(FetchStallError):
        fetcher.get("https://big.example/page", check_robots=False)


def test_normal_response_body_still_readable():
    def handler(request):
        return httpx.Response(200, json={"ok": True})

    fetcher = _fetcher(handler)
    resp = fetcher.get("https://fine.example/api", check_robots=False)
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_head_request_works():
    def handler(request):
        assert request.method == "HEAD"
        return httpx.Response(204)

    fetcher = _fetcher(handler)
    assert fetcher.head("https://fine.example/", check_robots=False).status_code == 204


def test_retry_after_sleep_capped_at_30s(monkeypatch):
    faketime = FakeTime()
    monkeypatch.setattr(fetch_mod, "time", faketime)
    statuses = iter([429, 200])

    def handler(request):
        status = next(statuses)
        headers = {"Retry-After": "500"} if status == 429 else {}
        return httpx.Response(status, headers=headers)

    fetcher = _fetcher(handler)
    resp = fetcher.get("https://busy.example/", check_robots=False)
    assert resp.status_code == 200
    assert 30 in faketime.sleeps  # capped, not the advertised 500

    # and nothing slept longer than the cap
    assert max(faketime.sleeps) <= 30


def test_throttle_spacing_same_domain(monkeypatch):
    faketime = FakeTime()
    monkeypatch.setattr(fetch_mod, "time", faketime)
    fetcher = _fetcher(lambda request: httpx.Response(200))
    rate = RateSpec(min_interval=5, jitter=(0, 0))
    for _ in range(3):
        fetcher.get("https://one.example/x", rate=rate, check_robots=False)
    assert faketime.sleeps == [5.0, 5.0]


def test_throttle_no_wait_across_domains(monkeypatch):
    faketime = FakeTime()
    monkeypatch.setattr(fetch_mod, "time", faketime)
    fetcher = _fetcher(lambda request: httpx.Response(200))
    rate = RateSpec(min_interval=5, jitter=(0, 0))
    fetcher.get("https://one.example/x", rate=rate, check_robots=False)
    fetcher.get("https://two.example/x", rate=rate, check_robots=False)
    assert faketime.sleeps == []


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("www.laubinger.de", "https://www.laubinger.de"),
        ("kunstfest-pankow.berlin/programm", "https://kunstfest-pankow.berlin/programm"),
        ("example.de?p=1", "https://example.de?p=1"),
        ("https://already.fine/", "https://already.fine/"),
        ("http://plain.example", "http://plain.example"),
        ("mailto:info@x.de", "mailto:info@x.de"),
        ("Genaue Adresse folgt", "Genaue Adresse folgt"),
        ("", ""),
        (None, None),
    ],
)
def test_ensure_scheme(raw, expected):
    assert ensure_scheme(raw) == expected
