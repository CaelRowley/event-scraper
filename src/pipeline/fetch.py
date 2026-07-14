"""Fetch layer: httpx + hishel RFC-9111 cache, robots gate, per-domain rate limit, retries.

Rate limiting lives here and only here — adapters declare a RateSpec but cannot bypass
the per-domain bucket.
"""

from __future__ import annotations

import logging
import random
import time
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
        self._robots: dict[str, Protego | None] = {}
        self.default_ua = user_agent
        self.requests_made = 0

    # --- politeness -----------------------------------------------------------

    def _throttle(self, url: str, rate: RateSpec) -> None:
        domain = urlsplit(url).netloc
        last = self._last_request.get(domain)
        if last is not None:
            wait = rate.min_interval + random.uniform(*rate.jitter) - (time.monotonic() - last)
            if wait > 0:
                time.sleep(wait)
        self._last_request[domain] = time.monotonic()

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

    @retry(
        retry=retry_if_exception(_retryable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, max=60),
        reraise=True,
    )
    def _send(self, method: str, url: str, **kwargs) -> httpx.Response:
        deadline = time.monotonic() + MAX_RESPONSE_SECONDS
        request = self.client.build_request(method, url, **kwargs)
        resp = self.client.send(request, stream=True)
        self.requests_made += 1
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
        headers = dict(kwargs.pop("headers", {}))
        if user_agent:
            headers["User-Agent"] = user_agent
        return self._send("GET", url, headers=headers, **kwargs)

    def head(self, url: str, *, rate: RateSpec = RateSpec(), user_agent: str | None = None,
             check_robots: bool = True, **kwargs) -> httpx.Response:
        if check_robots and not self.allowed(url, user_agent):
            raise PermissionError(f"robots.txt disallows {url}")
        self._throttle(url, rate)
        headers = dict(kwargs.pop("headers", {}))
        if user_agent:
            headers["User-Agent"] = user_agent
        return self._send("HEAD", url, headers=headers, **kwargs)

    def post(self, url: str, *, rate: RateSpec = RateSpec(), user_agent: str | None = None,
             check_robots: bool = True, **kwargs) -> httpx.Response:
        if check_robots and not self.allowed(url, user_agent):
            raise PermissionError(f"robots.txt disallows {url}")
        self._throttle(url, rate)
        headers = dict(kwargs.pop("headers", {}))
        if user_agent:
            headers["User-Agent"] = user_agent
        return self._send("POST", url, headers=headers, **kwargs)

    def close(self) -> None:
        self.client.close()
