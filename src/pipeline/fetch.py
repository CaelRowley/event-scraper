"""Fetch layer: httpx + hishel RFC-9111 cache, robots gate, per-domain rate limit, retries.

Rate limiting lives here and only here — adapters declare a RateSpec but cannot bypass
the per-domain bucket.
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from urllib.parse import quote, urlsplit

import httpx
from protego import Protego
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger(__name__)

DEFAULT_UA = "events-pipeline/0.1 (+https://github.com/CaelRowley/event-scraper)"

# Hard per-request caps. httpx's read timeout is per-chunk, so a server trickling
# bytes can hold a request open for many minutes — observed costing ~36 min/run in CI.
MAX_RESPONSE_SECONDS = 60.0
MAX_RESPONSE_BYTES = 10 * 1024 * 1024
BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# The headers a real Chrome 131 sends alongside that User-Agent. Claiming to be a
# browser while sending none of these is trivially detectable — a bot filter does
# not need to fingerprint TLS to notice that "Chrome" forgot `Sec-Ch-Ua`. Sent
# only for sources that opted into BROWSER_UA; the honest bot UA keeps its own
# minimal, truthful header set.
#
# Accept-Encoding is deliberately absent: httpx negotiates it against what it can
# actually decode, and advertising `br, zstd` we cannot unpack breaks the body.
BROWSER_HEADERS = {
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,image/apng,*/*;q=0.8,"
               "application/signed-exchange;v=b3;q=0.7"),
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Linux"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}


def _browser_headers(user_agent: str | None, extra: dict) -> dict:
    """Caller headers, plus Chrome's companions when the caller claims to be Chrome.

    The caller's own keys win: an adapter asking for `Accept: application/json`
    means it, and the JSON APIs behind these WAFs would rightly reject a request
    that says it wants HTML.
    """
    headers = dict(BROWSER_HEADERS) if user_agent == BROWSER_UA else {}
    if user_agent:
        headers["User-Agent"] = user_agent
    headers.update(extra)
    return headers


# Hosts worth routing through the relay: refused from the runner's IP, accepted
# from Cloudflare's. Their robots.txt allows the paths we ask for, so the refusal
# is a blanket datacenter-IP rule rather than a decision about this crawler.
#
# Only eventbrite qualifies, and only measurement settled that. livegigs and the
# two Tribe comedy venues refuse the deployed Worker's egress as well — the pool
# a deployed Worker sends from is not the one `wrangler dev --remote` uses, so an
# earlier probe against the preview environment said all four would work and only
# one did. Routing the other three here would spend a request to get the same 403.
PROXIED_HOSTS = frozenset({"www.eventbrite.de"})


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
    """One instance per run, shared across adapters. Honest UA by default; per-request override."""

    def __init__(self, cache_path: str | None = "data/http_cache.db", user_agent: str = DEFAULT_UA):
        headers = {"User-Agent": user_agent, "Accept-Language": "de-DE,de;q=0.9,en;q=0.7"}
        if cache_path:
            import hishel
            from hishel.httpx import SyncCacheClient

            self.client: httpx.Client = SyncCacheClient(
                storage=hishel.SyncSqliteStorage(database_path=cache_path),
                headers=headers,
                timeout=30.0,
                follow_redirects=True,
            )
        else:
            self.client = httpx.Client(headers=headers, timeout=30.0, follow_redirects=True)
        self._last_request: dict[str, float] = {}
        # _robots is unlocked by choice: a race costs at most one duplicate robots.txt GET
        self._robots: dict[str, Protego | None] = {}
        self._locks_guard = threading.Lock()
        self._domain_locks: dict[str, threading.Lock] = {}
        self._counter_lock = threading.Lock()
        self.default_ua = user_agent
        self.requests_made = 0
        # Per-source response tallies, keyed by thread name — the runner names each
        # adapter thread after its slug, so attribution costs nothing here. robots.txt
        # is fetched off `self.client` directly and never reaches this, which is what
        # keeps a blocked source from looking healthy on the strength of one 200.
        self._source_http: dict[str, Counter] = defaultdict(Counter)
        # Set both to route PROXIED_HOSTS through the relay; unset, every request
        # goes direct and the blocked sources simply stay blocked.
        self._proxy_url = os.environ.get("FETCH_PROXY_URL") or None
        self._proxy_token = os.environ.get("FETCH_PROXY_TOKEN") or None

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
        with self._domain_lock(domain):
            last = self._last_request.get(domain)
            if last is not None:
                wait = rate.min_interval + random.uniform(*rate.jitter) - (time.monotonic() - last)
                if wait > 0:
                    time.sleep(wait)
            self._last_request[domain] = time.monotonic()

    def source_http(self) -> dict[str, dict[str, int]]:
        """`{thread name: {ok|blocked|other: n}}` — a snapshot, safe to call at the end."""
        with self._counter_lock:
            return {name: dict(counts) for name, counts in self._source_http.items()}

    def _robots_for(self, url: str) -> Protego | None:
        domain = urlsplit(url).netloc
        if domain not in self._robots:
            robots_url = f"{urlsplit(url).scheme}://{domain}/robots.txt"
            try:
                resp = self.client.get(robots_url)
                self._robots[domain] = Protego.parse(resp.text) if resp.status_code == 200 else None
            except httpx.HTTPError:
                self._robots[domain] = None
        return self._robots[domain]

    def allowed(self, url: str, user_agent: str | None = None) -> bool:
        rp = self._robots_for(url)
        if rp is None:
            return True
        return rp.can_fetch(url, user_agent or self.default_ua)

    # --- requests --------------------------------------------------------------

    def _proxied(self, method: str, url: str, headers: dict) -> tuple[str, dict]:
        """Rewrite to the relay when this host needs it. Otherwise unchanged.

        Only GET and HEAD: the relay refuses anything else, and the one POST we
        make (RA's GraphQL) is not blocked. robots and the per-domain rate limit
        were already applied to the real URL by the caller, so routing the bytes
        elsewhere does not loosen either.
        """
        if not (self._proxy_url and self._proxy_token):
            return url, headers
        if method not in ("GET", "HEAD"):
            return url, headers
        if urlsplit(url).netloc not in PROXIED_HOSTS:
            return url, headers
        headers = dict(headers)
        headers["Authorization"] = f"Bearer {self._proxy_token}"
        return f"{self._proxy_url}?url={quote(url, safe='')}", headers

    @retry(
        retry=retry_if_exception(_retryable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, max=60),
        reraise=True,
    )
    def _send(self, method: str, url: str, **kwargs) -> httpx.Response:
        deadline = time.monotonic() + MAX_RESPONSE_SECONDS
        kwargs["headers"] = dict(kwargs.get("headers") or {})
        url, kwargs["headers"] = self._proxied(method, url, kwargs["headers"])
        request = self.client.build_request(method, url, **kwargs)
        resp = self.client.send(request, stream=True)
        with self._counter_lock:
            self.requests_made += 1
            self._source_http[threading.current_thread().name][
                _outcome(resp.status_code)] += 1
        buf = bytearray()
        try:
            for chunk in resp.iter_bytes():
                buf += chunk
                if time.monotonic() > deadline or len(buf) > MAX_RESPONSE_BYTES:
                    elapsed = time.monotonic() - (deadline - MAX_RESPONSE_SECONDS)
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

    def get(self, url: str, *, rate: RateSpec = RateSpec(), user_agent: str | None = None,
            check_robots: bool = True, **kwargs) -> httpx.Response:
        if check_robots and not self.allowed(url, user_agent):
            raise PermissionError(f"robots.txt disallows {url}")
        self._throttle(url, rate)
        headers = _browser_headers(user_agent, kwargs.pop("headers", {}))
        return self._send("GET", url, headers=headers, **kwargs)

    def head(self, url: str, *, rate: RateSpec = RateSpec(), user_agent: str | None = None,
             check_robots: bool = True, **kwargs) -> httpx.Response:
        if check_robots and not self.allowed(url, user_agent):
            raise PermissionError(f"robots.txt disallows {url}")
        self._throttle(url, rate)
        headers = _browser_headers(user_agent, kwargs.pop("headers", {}))
        return self._send("HEAD", url, headers=headers, **kwargs)

    def post(self, url: str, *, rate: RateSpec = RateSpec(), user_agent: str | None = None,
             check_robots: bool = True, **kwargs) -> httpx.Response:
        if check_robots and not self.allowed(url, user_agent):
            raise PermissionError(f"robots.txt disallows {url}")
        self._throttle(url, rate)
        headers = _browser_headers(user_agent, kwargs.pop("headers", {}))
        return self._send("POST", url, headers=headers, **kwargs)

    def close(self) -> None:
        self.client.close()
