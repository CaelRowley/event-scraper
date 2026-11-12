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


# --- one honest identity ---------------------------------------------------------

def _recording_fetcher():
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(404 if request.url.path == "/robots.txt" else 200, json={})

    f = Fetcher(cache_path=None)
    f.client = httpx.Client(transport=httpx.MockTransport(handler),
                            headers=dict(f.client.headers))
    return f, seen


def test_every_request_identifies_the_collector_with_a_contact_url():
    from pipeline.fetch import CONTACT_URL, USER_AGENT
    assert USER_AGENT.startswith("events-pipeline/")
    assert CONTACT_URL.startswith("https://") and f"(+{CONTACT_URL})" in USER_AGENT
    f, seen = _recording_fetcher()
    try:
        f.get("https://site.example/page")
        f.head("https://site.example/page")
        f.post("https://site.example/graphql", json={})
        assert len(seen) == 4  # robots.txt once, then the three requests
        assert {r.headers["user-agent"] for r in seen} == {USER_AGENT}
    finally:
        f.close()


def test_a_caller_cannot_swap_the_user_agent_or_claim_to_be_a_browser():
    from pipeline.fetch import USER_AGENT
    f, seen = _recording_fetcher()
    try:
        f.get("https://site.example/page",
              headers={"User-Agent": "Mozilla/5.0 Chrome/131", "Accept": "application/json"})
        sent = seen[-1].headers
        assert sent["user-agent"] == USER_AGENT
        assert sent["accept"] == "application/json"  # ordinary headers still pass
        assert not any(k.lower().startswith("sec-") for k in sent)
    finally:
        f.close()


def test_no_browser_user_agent_or_robots_bypass_exists_in_the_source_tree():
    """The mechanisms themselves are gone, not just unused: nothing can opt back in."""
    from pathlib import Path
    import pipeline
    src = Path(pipeline.__file__).parent
    code = "\n".join(p.read_text() for p in src.rglob("*.py"))
    for needle in ("BROWSER_UA", "BROWSER_HEADERS", "Sec-Ch-Ua", "Mozilla/5.0",
                   "check_robots", "FETCH_PROXY"):
        assert needle not in code, f"{needle} is back in src/"


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
                f.get(url)
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
