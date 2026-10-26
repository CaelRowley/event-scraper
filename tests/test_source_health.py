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


# --- sites that refused us are not contacted at all -----------------------------

def test_a_denied_domain_is_never_requested():
    """No request leaves for a denied site — not the page, not robots.txt."""
    from pipeline.fetch import HostBlockedError

    def handler(request):
        raise AssertionError(f"request sent to {request.url}")

    f = Fetcher(cache_path=None)
    f.client = httpx.Client(transport=httpx.MockTransport(handler))
    try:
        for url in ("https://www.tip-berlin.de/event/x/",
                    "https://www.eventbrite.de/d/berlin/events/",
                    "https://rausgegangen.de/sitemap-events.xml"):
            assert f.tripped(url)
            assert not f.allowed(url)
            with pytest.raises(HostBlockedError):
                f.get(url, check_robots=False)
        assert f.tripped_hosts() == [], "denied is standing policy, not a run event"
    finally:
        f.close()


def test_denial_matches_subdomains_but_not_lookalikes():
    from pipeline.fetch import is_denied
    assert is_denied("https://tip-berlin.de/x")
    assert is_denied("https://www.ticketmaster.de/event/123")
    assert not is_denied("https://app.ticketmaster.com/discovery/v2/events.json")
    assert not is_denied("https://notrausgegangen.de/x")
    assert not is_denied("https://eventbrite.de.evil.com/x")
