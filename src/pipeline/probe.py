"""Fan per-URL HTTP work out across hosts; keep SQLite on the calling thread.

The post-scrape phases (image scan, link check, image check) used to probe one
URL at a time, so a run spent most of its post-scrape time waiting on rate-limit
sleeps for hosts that had nothing to do with each other — ~13 minutes in the
last green run. Different hosts never needed to wait for each other.

Each host gets one worker, which walks that host's URLs in order. More than one
thread per host would buy nothing: the Fetcher already spaces a host's requests
behind its per-domain lock, so extra threads would only park there while other
hosts sat idle. Results come back to the caller as they land, and the caller does
every database write itself — the single-writer rule the runner relies on (two
concurrent writers produced WAL lock failures in CI) still holds.
"""

from __future__ import annotations

import queue
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVar
from urllib.parse import urlsplit

T = TypeVar("T")
R = TypeVar("R")

MAX_WORKERS = 8

_DONE = object()


def select(items: Iterable[T], *, budget: int, per_host: int | None = None,
           url_of: Callable[[T], str] = lambda item: item) -> list[T]:
    """The first `budget` items in priority order, at most `per_host` from any host.

    The per-host cap keeps one big host from taking the whole budget — and, since
    a host's URLs run back to back, from setting the length of the whole phase.
    """
    picked: list[T] = []
    per: dict[str, int] = {}
    for item in items:
        if len(picked) >= budget:
            break
        host = urlsplit(url_of(item)).netloc
        if per_host is not None and per.get(host, 0) >= per_host:
            continue
        per[host] = per.get(host, 0) + 1
        picked.append(item)
    return picked


def fan_out(items: Iterable[T], work: Callable[[T], R], *,
            url_of: Callable[[T], str] = lambda item: item,
            fetcher=None, workers: int = MAX_WORKERS) -> Iterator[tuple[T, R]]:
    """Yield `(item, work(item))` as each finishes, hosts in parallel.

    `work` runs on a worker thread and must not touch SQLite. If `fetcher` is
    given, a host it has given up on (circuit breaker) stops early: its remaining
    items are not yielded at all, so callers record nothing and retry next run.
    """
    hosts: dict[str, list[T]] = {}
    for item in items:
        hosts.setdefault(urlsplit(url_of(item)).netloc, []).append(item)
    if not hosts:
        return

    results: queue.Queue = queue.Queue()

    def run_host(host_items: list[T]) -> None:
        try:
            for item in host_items:
                if fetcher is not None and fetcher.tripped(url_of(item)):
                    break
                results.put((item, work(item)))
        finally:
            results.put(_DONE)

    with ThreadPoolExecutor(max_workers=min(workers, len(hosts)),
                            thread_name_prefix="probe") as pool:
        futures = [pool.submit(run_host, host_items) for host_items in hosts.values()]
        remaining = len(futures)
        while remaining:
            got = results.get()
            if got is _DONE:
                remaining -= 1
                continue
            yield got
        for fut in futures:
            fut.result()  # surface a bug in `work` rather than losing it
