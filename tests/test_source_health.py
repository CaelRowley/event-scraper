"""A blocked source must not pass for a quiet one."""
import httpx
import pytest
import respx

from pipeline.fetch import Fetcher, _outcome


@pytest.mark.parametrize("status,expected", [
    (200, "ok"), (204, "ok"), (301, "ok"),
    (403, "blocked"), (405, "blocked"), (429, "blocked"), (451, "blocked"),
    (404, "other"), (410, "other"),
])
def test_only_refusals_count_as_blocked(status, expected):
    # 404 says the page is gone; 403 says we are not welcome. Only the second is
    # evidence about the source's health.
    assert _outcome(status) == expected


@respx.mock
def test_a_source_that_is_refused_everywhere_is_reported():
    respx.get("https://blocked.example/robots.txt").mock(httpx.Response(404))
    respx.get("https://blocked.example/events").mock(httpx.Response(403))
    fetcher = Fetcher(cache_path=None)
    import threading
    threading.current_thread().name = "livegigs"
    try:
        fetcher.get("https://blocked.example/events")
        counts = fetcher.source_http()["livegigs"]
        assert counts.get("blocked") == 1 and not counts.get("ok")
    finally:
        threading.current_thread().name = "MainThread"
        fetcher.close()


@respx.mock
def test_robots_fetches_do_not_make_a_blocked_source_look_healthy():
    """robots.txt is fetched off the raw client, so it must not land in the tally."""
    respx.get("https://blocked.example/robots.txt").mock(httpx.Response(200, text=""))
    respx.get("https://blocked.example/events").mock(httpx.Response(403))
    fetcher = Fetcher(cache_path=None)
    import threading
    threading.current_thread().name = "comedy_cafe"
    try:
        fetcher.get("https://blocked.example/events")
        counts = fetcher.source_http()["comedy_cafe"]
        assert not counts.get("ok"), f"robots.txt leaked into the tally: {counts}"
        assert counts.get("blocked") == 1
    finally:
        threading.current_thread().name = "MainThread"
        fetcher.close()


@respx.mock
def test_a_source_that_answers_is_not_flagged():
    respx.get("https://fine.example/robots.txt").mock(httpx.Response(404))
    respx.get("https://fine.example/events").mock(httpx.Response(200, text="ok"))
    fetcher = Fetcher(cache_path=None)
    import threading
    threading.current_thread().name = "kulturdaten"
    try:
        fetcher.get("https://fine.example/events")
        counts = fetcher.source_http()["kulturdaten"]
        assert counts.get("ok") == 1 and not counts.get("blocked")
    finally:
        threading.current_thread().name = "MainThread"
        fetcher.close()


# --- browser headers -------------------------------------------------------------

def test_browser_ua_requests_carry_chromes_companion_headers():
    """Claiming to be Chrome while sending none of Chrome's headers is detectable."""
    from pipeline.fetch import BROWSER_UA, _browser_headers
    h = _browser_headers(BROWSER_UA, {})
    assert h["User-Agent"] == BROWSER_UA
    assert h["Sec-Ch-Ua-Platform"] == '"Linux"'
    assert "Sec-Fetch-Mode" in h and "Accept" in h


def test_the_honest_bot_ua_stays_minimal():
    """We do not pretend to be a browser when we have said we are a crawler."""
    from pipeline.fetch import DEFAULT_UA, _browser_headers
    h = _browser_headers(DEFAULT_UA, {})
    assert h == {"User-Agent": DEFAULT_UA}


def test_caller_headers_win():
    """An adapter asking for JSON must not be given Chrome's HTML Accept."""
    from pipeline.fetch import BROWSER_UA, _browser_headers
    h = _browser_headers(BROWSER_UA, {"Accept": "application/json"})
    assert h["Accept"] == "application/json"
    assert "Sec-Ch-Ua" in h


def test_no_accept_encoding_is_advertised():
    """httpx negotiates it; advertising br/zstd we cannot decode breaks the body."""
    from pipeline.fetch import BROWSER_HEADERS
    assert not any(k.lower() == "accept-encoding" for k in BROWSER_HEADERS)


# --- the relay for hosts that refuse datacenter IPs ------------------------------

def _fetcher(monkeypatch, *, proxy=True):
    if proxy:
        monkeypatch.setenv("FETCH_PROXY_URL", "https://relay.example/fetch")
        monkeypatch.setenv("FETCH_PROXY_TOKEN", "s3cret")
    else:
        monkeypatch.delenv("FETCH_PROXY_URL", raising=False)
        monkeypatch.delenv("FETCH_PROXY_TOKEN", raising=False)
    return Fetcher(cache_path=None)


def test_a_blocked_host_is_routed_through_the_relay(monkeypatch):
    f = _fetcher(monkeypatch)
    try:
        url, headers = f._proxied("GET", "https://www.eventbrite.de/d/berlin/events/", {"User-Agent": "x"})
        assert url.startswith("https://relay.example/fetch?url=")
        assert "https%3A%2F%2Fwww.eventbrite.de" in url
        assert headers["Authorization"] == "Bearer s3cret"
        assert headers["User-Agent"] == "x", "the caller's headers must survive"
    finally:
        f.close()


def test_every_other_host_still_goes_direct(monkeypatch):
    f = _fetcher(monkeypatch)
    try:
        url, headers = f._proxied("GET", "https://api-v2.kulturdaten.berlin/x", {})
        assert url == "https://api-v2.kulturdaten.berlin/x"
        assert "Authorization" not in headers, "no token leaks to unrelated hosts"
    finally:
        f.close()


def test_post_is_never_relayed(monkeypatch):
    """RA's GraphQL POST is not blocked, and the relay refuses writes anyway."""
    f = _fetcher(monkeypatch)
    try:
        url, _ = f._proxied("POST", "https://www.eventbrite.de/d/berlin/events/", {})
        assert url == "https://www.eventbrite.de/d/berlin/events/"
    finally:
        f.close()


def test_without_credentials_everything_goes_direct(monkeypatch):
    f = _fetcher(monkeypatch, proxy=False)
    try:
        url, headers = f._proxied("GET", "https://www.eventbrite.de/x", {})
        assert url == "https://www.eventbrite.de/x"
        assert "Authorization" not in headers
    finally:
        f.close()


def test_a_lookalike_host_is_not_relayed(monkeypatch):
    """Membership is exact — eventbrite.de.evil.com must not borrow the token."""
    f = _fetcher(monkeypatch)
    try:
        url, headers = f._proxied("GET", "https://www.eventbrite.de.evil.com/x", {})
        assert url == "https://www.eventbrite.de.evil.com/x"
        assert "Authorization" not in headers
    finally:
        f.close()
