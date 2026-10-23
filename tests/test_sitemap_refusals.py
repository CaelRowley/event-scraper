"""A page the site refused to serve has not been seen — it must be retried."""
from pipeline.adapters.jsonld_sitemap import JsonLdSitemapAdapter
from pipeline.db import connect, ledger_get


class _Resp:
    def __init__(self, status, text=""):
        self.status_code = status
        self.text = text


class _Fetcher:
    def __init__(self, statuses):
        self.statuses = statuses

    def get(self, url, **kw):
        return _Resp(self.statuses[url])


class _Adapter(JsonLdSitemapAdapter):
    slug = "fake_sitemap"

    def _discover(self):
        return [("https://x.example/refused", "2026-10-01"), ("https://x.example/gone", "2026-10-01")]


def test_refused_pages_are_not_ledgered_but_gone_pages_are():
    conn = connect(":memory:")
    adapter = _Adapter(_Fetcher({"https://x.example/refused": 403,
                                 "https://x.example/gone": 404}), conn)
    assert list(adapter.fetch_events()) == []
    assert ledger_get(conn, "fake_sitemap", "https://x.example/refused") is None
    assert ledger_get(conn, "fake_sitemap", "https://x.example/gone")["last_status"] == 404
