"""probe.fan_out: hosts in parallel, one host at a time, results to the caller."""
import threading
import time

from pipeline.probe import fan_out, select


def test_select_caps_any_one_host():
    urls = [f"https://big.example/{i}" for i in range(10)] + ["https://small.example/1"]
    picked = select(urls, budget=5, per_host=3)
    assert picked == ["https://big.example/0", "https://big.example/1",
                      "https://big.example/2", "https://small.example/1"]


def test_select_respects_budget_in_priority_order():
    urls = [f"https://h{i}.example/" for i in range(10)]
    assert select(urls, budget=3) == urls[:3]


def test_hosts_run_in_parallel_but_one_host_runs_serially():
    active: dict[str, int] = {}
    peak: dict[str, int] = {}
    overall = {"now": 0, "peak": 0}
    lock = threading.Lock()

    def work(url):
        host = url.split("/")[2]
        with lock:
            active[host] = active.get(host, 0) + 1
            peak[host] = max(peak.get(host, 0), active[host])
            overall["now"] += 1
            overall["peak"] = max(overall["peak"], overall["now"])
        time.sleep(0.05)
        with lock:
            active[host] -= 1
            overall["now"] -= 1
        return url.upper()

    urls = [f"https://h{h}.example/{i}" for h in range(4) for i in range(3)]
    results = dict(fan_out(urls, work))
    assert results == {u: u.upper() for u in urls}
    assert max(peak.values()) == 1, "a host must never see two requests at once"
    assert overall["peak"] > 1, "different hosts must overlap"


def test_a_tripped_host_stops_without_yielding_its_remainder():
    class Fetcher:
        def __init__(self):
            self.dead = set()

        def tripped(self, url):
            return url.split("/")[2] in self.dead

    fetcher = Fetcher()

    def work(url):
        if "blocked.example" in url:
            fetcher.dead.add("blocked.example")
        return "ok"

    urls = [f"https://blocked.example/{i}" for i in range(5)] + ["https://fine.example/1"]
    done = [u for u, _ in fan_out(urls, work, fetcher=fetcher)]
    assert done.count("https://blocked.example/0") == 1
    assert not any(u.startswith("https://blocked.example/") and u != "https://blocked.example/0"
                   for u in done)
    assert "https://fine.example/1" in done
