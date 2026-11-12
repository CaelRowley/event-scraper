"""Fetch layer: httpx + hishel RFC-9111 cache, robots gate, per-domain rate limit, retries.

Politeness lives here and only here. Adapters declare a RateSpec but cannot
bypass the per-domain bucket, cannot change the User-Agent, and cannot skip
robots.txt: every request is identified as this collector and checked against
the host's rules before it is sent.
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx
from protego import Protego
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from . import __version__, sources

log = logging.getLogger(__name__)

# Who we say we are, on every request, to every host. One identity: there is no
# per-source override and no browser User-Agent anywhere in this codebase. The
# contact URL is the operator's imprint, which names this collector and says how
# to make it stop — so a site that minds us has somewhere to say so.
CONTACT_URL = os.environ.get("PIPELINE_CONTACT_URL", "https://gobento.clorostica.com/impressum")
USER_AGENT = f"events-pipeline/{__version__} (+{CONTACT_URL})"

# Hard per-request caps. httpx's read timeout is per-chunk, so a server trickling
# bytes can hold a request open for many minutes — observed costing ~36 min/run in CI.
MAX_RESPONSE_SECONDS = 60.0
MAX_RESPONSE_BYTES = 10 * 1024 * 1024

# robots.txt governs every request this module sends. The one exception is a
# documented public API whose own usage policy is the rule and whose site-wide
# robots.txt is written for page crawlers: Wikimedia's blocks `/w/` wholesale,
# while its API etiquette asks only for an identifying User-Agent (which we
# send). Matched as exact host + path prefix; nothing else is exempt, and an
# adapter cannot add to this list at call time.
ROBOTS_EXEMPT_APIS = {
    "commons.wikimedia.org": "/w/api.php",
    # Overpass: robots.txt disallows `/api/` to crawlers; the interpreter is the
    # public query API, used here for a one-off venue bootstrap under its usage policy.
    "overpass-api.de": "/api/interpreter",
}

# What `_robots` holds for a host whose robots.txt could not be read (server
# error, refusal, network failure). RFC 9309 says to assume everything is
# disallowed in that case; a missing file (404) means no rules at all.
_UNREACHABLE = object()


def is_denied(url: str) -> bool:
    """Whether this URL belongs to a site we do not contact.

    A site that refuses the collector, forbids it in its terms, or disallows it
    in robots.txt has said no, and we do not route around a refusal. Its source
    is switched off in `sources.py`, and that is what puts its domains here —
    matched as the domain or any subdomain — so the adapter, the image scan and
    the link check all leave it alone, robots.txt included. A source comes back
    only with the site's permission (`PIPELINE_PERMITTED_SOURCES`).
    """
    host = (urlsplit(url).hostname or "").lower()
    return any(host == d or host.endswith("." + d) for d in sources.denied_domains())


# A host that refuses this many requests in a row is not asked again for the rest
# of the run. Without it, one WAF'd source spent ~14 minutes of every run on 229
# straight 403s (tip-berlin, 2026-10-03), and the image scan and link check then
# paid for the same refusals again.
TRIP_AFTER = 8


class HostBlockedError(PermissionError):
    """The host has refused every recent request; this one was not sent.

    A PermissionError on purpose: adapters already treat that as "stop this
    source, don't burn the budget" (see robots handling), which is exactly right.
    """


def is_refusal(status: int) -> bool:
    """403/405/429/451 — the site refused us, as opposed to the page being gone."""
    return _outcome(status) == "blocked"


def _outcome(status: int) -> str:
    """Blocked means "the site refused us", not "the page is gone".

    404/410 are ordinary and say nothing about our standing; 403/405/429/451 are
    what a WAF returns when it has decided we are a bot or the wrong IP, and a
    source that only ever gets those is not a quiet source, it is a shut door.
    """
    if status in (403, 405, 429, 451):
        return "blocked"
    if 200 <= status < 400:
        return "ok"
    return "other"


@dataclass(frozen=True)
class RateSpec:
    """Minimum seconds between requests to one domain, plus uniform jitter."""

    min_interval: float = 2.0
    jitter: tuple[float, float] = (0.5, 2.0)


class FetchStallError(Exception):
    """Response exceeded MAX_RESPONSE_SECONDS or MAX_RESPONSE_BYTES. Never retried —
    the stall is a property of the URL, and retrying would triple the cost."""


def _retryable(exc: BaseException) -> bool:
    if isinstance(exc, FetchStallError):
        return False
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in (429, 500, 502, 503, 504)
    return False


class Fetcher:
    """One instance per run, shared across adapters. One honest User-Agent for everything."""

    def __init__(self, cache_path: str | None = "data/http_cache.db"):
        headers = {"User-Agent": USER_AGENT, "Accept-Language": "de-DE,de;q=0.9,en;q=0.7"}
        if cache_path:
            import hishel
            from hishel.httpx import SyncCacheClient

            self.client: httpx.Client = SyncCacheClient(
                storage=hishel.SyncSqliteStorage(database_path=cache_path),
                headers=headers,
                timeout=30.0,
                follow_redirects=True,
            )
            # Same identity, no cache: for the rare body too large to be worth
            # keeping (`cache=False`). Everything else about a request is unchanged.
            self._uncached: httpx.Client | None = httpx.Client(
                headers=headers, timeout=30.0, follow_redirects=True)
        else:
            self.client = httpx.Client(headers=headers, timeout=30.0, follow_redirects=True)
            self._uncached = None
        self._last_request: dict[str, float] = {}
        # _robots is unlocked by choice: a race costs at most one duplicate robots.txt GET
        self._robots: dict[str, object] = {}
        self._locks_guard = threading.Lock()
        self._domain_locks: dict[str, threading.Lock] = {}
        self._counter_lock = threading.Lock()
        self.requests_made = 0
        # Per-source response tallies, keyed by thread name — the runner names each
        # adapter thread after its slug, so attribution costs nothing here. robots.txt
        # is fetched off `self.client` directly and never reaches this, which is what
        # keeps a blocked source from looking healthy on the strength of one 200.
        self._source_http: dict[str, Counter] = defaultdict(Counter)
        # Consecutive refusals per host, and the hosts that crossed TRIP_AFTER.
        self._refusals: Counter = Counter()
        self._tripped: set[str] = set()

    # --- politeness -----------------------------------------------------------

    def _domain_lock(self, domain: str) -> threading.Lock:
        with self._locks_guard:
            return self._domain_locks.setdefault(domain, threading.Lock())

    def _throttle(self, url: str, rate: RateSpec) -> None:
        # Sleeping while holding the domain lock is intentional: same-domain callers
        # queue here and each computes its wait from the previous caller's stamp, so
        # start-to-start spacing stays exactly min_interval+jitter across all threads.
        # Different domains hold different locks and never block each other.
        # (Retry backoff and Retry-After sleeps run outside this lock, as before.)
        domain = urlsplit(url).netloc
        # A Crawl-delay the site asks for wins over a shorter interval of ours.
        interval = max(rate.min_interval, self._crawl_delay(domain))
        with self._domain_lock(domain):
            last = self._last_request.get(domain)
            if last is not None:
                wait = interval + random.uniform(*rate.jitter) - (time.monotonic() - last)
                if wait > 0:
                    time.sleep(wait)
            self._last_request[domain] = time.monotonic()

    # --- circuit breaker --------------------------------------------------------

    def tripped(self, url: str) -> bool:
        """Whether this URL's host is denied, or was given up on for this run."""
        return is_denied(url) or urlsplit(url).netloc in self._tripped

    def tripped_hosts(self) -> list[str]:
        with self._counter_lock:
            return sorted(self._tripped)

    def _record_outcome(self, host: str, status: int) -> None:
        # caller holds _counter_lock
        if _outcome(status) == "blocked":
            self._refusals[host] += 1
            if self._refusals[host] >= TRIP_AFTER and host not in self._tripped:
                self._tripped.add(host)
                log.warning("%s refused %d requests in a row — skipping it for the rest "
                            "of this run", host, self._refusals[host])
        elif _outcome(status) == "ok":
            self._refusals[host] = 0

    def _guard(self, url: str) -> None:
        if self.tripped(url):
            raise HostBlockedError(f"{urlsplit(url).netloc} is refusing requests: {url}")

    def source_http(self) -> dict[str, dict[str, int]]:
        """`{thread name: {ok|blocked|other: n}}` — a snapshot, safe to call at the end."""
        with self._counter_lock:
            return {name: dict(counts) for name, counts in self._source_http.items()}

    def _robots_for(self, url: str):
        """Parsed rules for the URL's host, None when it has none, or `_UNREACHABLE`."""
        domain = urlsplit(url).netloc
        if domain not in self._robots:
            robots_url = f"{urlsplit(url).scheme}://{domain}/robots.txt"
            try:
                resp = self.client.get(robots_url)
            except httpx.HTTPError:
                self._robots[domain] = _UNREACHABLE
            else:
                if resp.status_code == 200:
                    self._robots[domain] = Protego.parse(resp.text)
                elif resp.status_code in (401, 403, 429) or resp.status_code >= 500:
                    # Refused or broken: we cannot know the rules, so we assume
                    # the strictest ones for this run rather than the loosest.
                    log.warning("robots.txt for %s answered %d — treating the host as "
                                "disallowed for this run", domain, resp.status_code)
                    self._robots[domain] = _UNREACHABLE
                else:
                    self._robots[domain] = None  # no robots.txt: no rules
        return self._robots[domain]

    def _crawl_delay(self, domain: str) -> float:
        """Seconds the host's robots.txt asks between requests; 0 if it asks nothing.

        Reads only what a request already loaded — it never fetches robots.txt
        itself, so it costs nothing and cannot contact a host by accident.
        """
        rp = self._robots.get(domain)
        if not isinstance(rp, Protego):
            return 0.0
        try:
            return float(rp.crawl_delay(USER_AGENT) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def allowed(self, url: str) -> bool:
        """Whether robots.txt lets this collector fetch the URL."""
        if is_denied(url):
            return False  # not even robots.txt
        parts = urlsplit(url)
        prefix = ROBOTS_EXEMPT_APIS.get((parts.hostname or "").lower())
        if prefix and parts.path.startswith(prefix):
            return True
        rp = self._robots_for(url)
        if rp is None:
            return True
        if rp is _UNREACHABLE:
            return False
        return rp.can_fetch(url, USER_AGENT)

    # --- requests --------------------------------------------------------------

    @retry(
        retry=retry_if_exception(_retryable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, max=60),
        reraise=True,
    )
    def _send(self, method: str, url: str, **kwargs) -> httpx.Response:
        # A caller may raise the size cap for a dataset it knows to be large (and
        # gets a proportionally longer clock); it cannot remove either.
        max_bytes = kwargs.pop("max_bytes", None) or MAX_RESPONSE_BYTES
        cached = kwargs.pop("cache", True)
        client = self.client if cached or self._uncached is None else self._uncached
        seconds = MAX_RESPONSE_SECONDS * max(1.0, max_bytes / MAX_RESPONSE_BYTES)
        deadline = time.monotonic() + seconds
        host = urlsplit(url).netloc
        kwargs["headers"] = dict(kwargs.get("headers") or {})
        request = client.build_request(method, url, **kwargs)
        resp = client.send(request, stream=True)
        with self._counter_lock:
            self.requests_made += 1
            self._source_http[threading.current_thread().name][
                _outcome(resp.status_code)] += 1
            self._record_outcome(host, resp.status_code)
        buf = bytearray()
        try:
            for chunk in resp.iter_bytes():
                buf += chunk
                if time.monotonic() > deadline or len(buf) > max_bytes:
                    elapsed = time.monotonic() - (deadline - seconds)
                    log.warning("fetch stalled after %.0fs / %d bytes — aborting: %s",
                                elapsed, len(buf), url)
                    raise FetchStallError(url)
        finally:
            resp.close()
        # documented read() path has no wall-clock cap, so the body is assembled here
        resp._content = bytes(buf)  # noqa: SLF001
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                time.sleep(min(int(retry_after), 30))
            resp.raise_for_status()
        if resp.status_code >= 500:
            resp.raise_for_status()
        return resp

    def _request(self, method: str, url: str, rate: RateSpec, kwargs: dict) -> httpx.Response:
        self._guard(url)
        if not self.allowed(url):
            if self._robots.get(urlsplit(url).netloc) is _UNREACHABLE:
                # The host would not even show us its rules. That is a refusal,
                # so it is tallied as one: a source that only ever gets this far
                # is reported as blocked rather than passing for a quiet one.
                with self._counter_lock:
                    self._source_http[threading.current_thread().name]["blocked"] += 1
                raise HostBlockedError(f"robots.txt unavailable for {url}")
            raise PermissionError(f"robots.txt disallows {url}")
        self._throttle(url, rate)
        headers = dict(kwargs.pop("headers", None) or {})
        # The client's own User-Agent is the only one; a caller cannot replace it.
        headers = {k: v for k, v in headers.items() if k.lower() != "user-agent"}
        return self._send(method, url, headers=headers, **kwargs)

    def get(self, url: str, *, rate: RateSpec = RateSpec(), **kwargs) -> httpx.Response:
        return self._request("GET", url, rate, kwargs)

    def head(self, url: str, *, rate: RateSpec = RateSpec(), **kwargs) -> httpx.Response:
        return self._request("HEAD", url, rate, kwargs)

    def post(self, url: str, *, rate: RateSpec = RateSpec(), **kwargs) -> httpx.Response:
        return self._request("POST", url, rate, kwargs)

    def close(self) -> None:
        self.client.close()
        if self._uncached is not None:
            self._uncached.close()
