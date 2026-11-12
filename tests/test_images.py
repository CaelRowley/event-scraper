from datetime import datetime, timedelta

from pipeline.db import connect, ledger_get, ledger_put
from pipeline.images import find_image, scan_is_due


def test_og_image():
    html = '<head><meta property="og:image" content="https://x.example/poster.jpg"></head>'
    assert find_image(html, "https://x.example/e/1") == "https://x.example/poster.jpg"


def test_jsonld_image_beats_og():
    html = """
    <script type="application/ld+json">
    {"@type":"Event","name":"X","startDate":"2026-06-14","image":"https://x.example/jsonld.jpg"}
    </script>
    <meta property="og:image" content="https://x.example/og.jpg">
    """
    assert find_image(html, "https://x.example/") == "https://x.example/jsonld.jpg"


def test_relative_url_resolved():
    html = '<meta property="og:image" content="/img/flyer.png">'
    assert find_image(html, "https://venue.example/events/1") == "https://venue.example/img/flyer.png"


def test_twitter_fallback():
    html = '<meta name="twitter:image" content="https://x.example/tw.jpg">'
    assert find_image(html, "https://x.example/") == "https://x.example/tw.jpg"


def test_logo_and_svg_rejected():
    html = """
    <meta property="og:image" content="https://x.example/logo.png">
    <meta name="twitter:image" content="https://x.example/icon.svg">
    """
    assert find_image(html, "https://x.example/") is None


def test_jsonld_image_object_and_list():
    html = """
    <script type="application/ld+json">
    {"@type":"Event","name":"X","startDate":"2026-06-14",
     "image":[{"@type":"ImageObject","url":"https://x.example/a.jpg"}]}
    </script>
    """
    assert find_image(html, "https://x.example/") == "https://x.example/a.jpg"


def test_no_image_returns_none():
    assert find_image("<html><body>nothing</body></html>", "https://x.example/") is None


def test_logo_og_falls_through_to_content_image():
    # tip-berlin pattern: og:image is the site logo, real hero image in the article
    html = """
    <meta property="og:image" content="https://site.example/tipberlin-logo-2023.png">
    <article>
      <img src="/wp-content/uploads/icon-small.png" width="64" height="64">
      <img src="/wp-content/uploads/2026/06/hero.jpg" width="1024" height="683">
    </article>
    """
    assert find_image(html, "https://site.example/e/1") == \
        "https://site.example/wp-content/uploads/2026/06/hero.jpg"


def test_content_image_without_size_attrs_accepted():
    html = "<main><img data-src='/media/flyer.jpg'></main>"
    assert find_image(html, "https://site.example/") == "https://site.example/media/flyer.jpg"


def test_image_scan_miss_retries_after_ttl():
    conn = connect(":memory:")
    ledger_put(conn, "imgscan", "https://site.example/event", status=200)
    row = ledger_get(conn, "imgscan", "https://site.example/event")
    checked_at = datetime.fromisoformat(row["last_seen_at"].replace("Z", "+00:00"))

    assert not scan_is_due(row, checked_at + timedelta(days=6))
    assert scan_is_due(row, checked_at + timedelta(days=8))


BERLIN_CAL = "https://www.berlin.de/land/kalender/index.php?detail=271366"


def test_berlin_calendar_photo_found_in_aria_main():
    """kulturdaten's event pages: no article/main/figure, only role=main around the photo."""
    html = """
    <meta name="og:image" content="https://www.berlin.de/i9f/r1/images/favicon/Berlin_de_FB_Social.png">
    <div role="main"><section class="modul-text_bild">
      <div class="image"><img src="data/271366-1740055264.jpg" alt="Kidsführung"></div>
      <img class="qr-print" src="qrcode.php?id=271366&type=calendar">
    </section></div>
    """
    assert find_image(html, BERLIN_CAL) == "https://www.berlin.de/land/kalender/data/271366-1740055264.jpg"


def test_berlin_calendar_without_photo_ignores_icons_and_qr_codes():
    html = """
    <div role="main">
      <img src="/i9f/images/icon/piktogramm_rollstuhlgerecht.gif" alt="rollstuhlgerecht">
      <img class="qr-print" src="qrcode.php?id=281089&type=web">
    </div>
    """
    assert find_image(html, BERLIN_CAL) is None


# --- photo credits -------------------------------------------------------------------

CAL_PAGE = """<html><body><div role="main"><div class="image">
  <img src="/land/kalender/data/281641-1.jpg" width="800" height="600" alt="x">
  <p class="image__caption">Half Life Chornobyl</p>
  <p class="image__copyright">
      Foto: arc-gestaltung, Berlin / Foto Volker Kreidler 1995
  </p></div><h1 class="title">Half Life</h1></div></body></html>"""


def test_the_credit_a_page_states_for_its_photo_is_read_as_plain_text():
    from pipeline.images import find_credit
    assert find_credit(CAL_PAGE) == "arc-gestaltung, Berlin / Foto Volker Kreidler 1995"
    assert find_credit("<html><body><p>Foto: nicht hier</p></body></html>") is None
    assert find_credit('<p class="image__copyright">  </p>') is None
    assert find_credit('<p class="image__copyright">Bild: <b>Canva</b></p>') == "Canva"


def _credit_db(image_url):
    from datetime import date, timedelta
    from pipeline.db import connect, ledger_put, upsert_event
    from pipeline.models import Event, Occurrence, Price
    day = (date.today() + timedelta(days=1)).isoformat()
    url = "https://www.berlin.de/land/kalender/index.php?detail=281641"
    conn = connect(":memory:")
    upsert_event(conn, Event(
        id="", source="kulturdaten", source_event_id="1", source_url=url, title="Half Life",
        city="berlin", price=Price(), image_url=image_url,
        occurrences=[Occurrence(starts_at_utc=f"{day}T10:00:00Z",
                                starts_at_local=f"{day}T11:00:00+01:00", nightlife_date=day)]))
    if image_url:
        ledger_put(conn, "imgscan", url, status=200)  # scanned before credits were read
    return conn, url


class _PageFetcher:
    def __init__(self, html):
        self.html, self.calls = html, []

    def allowed(self, url):
        return True

    def tripped(self, url):
        return False

    def get(self, url, **kw):
        self.calls.append(url)
        return httpx.Response(200, text=self.html, headers={"content-type": "text/html"},
                              request=httpx.Request("GET", url))


import httpx  # noqa: E402


def test_a_new_scan_stores_the_image_and_its_credit_together():
    from pipeline.images import backfill_images
    conn, url = _credit_db(None)
    stats = backfill_images(conn, _PageFetcher(CAL_PAGE), "berlin")
    row = conn.execute("SELECT image_url, image_credit FROM events").fetchone()
    assert row["image_url"] == "https://www.berlin.de/land/kalender/data/281641-1.jpg"
    assert row["image_credit"] == "arc-gestaltung, Berlin / Foto Volker Kreidler 1995"
    assert stats["found"] == 1 and stats["credits"]["due"] == 0


def test_an_image_stored_without_a_credit_gets_one_on_a_single_revisit():
    from pipeline.images import backfill_images
    conn, url = _credit_db("https://www.berlin.de/land/kalender/data/281641-1.jpg")
    fetcher = _PageFetcher(CAL_PAGE)
    stats = backfill_images(conn, fetcher, "berlin")
    assert stats["credits"] == {"due": 1, "visited": 1, "credited": 1}
    assert conn.execute("SELECT image_credit FROM events").fetchone()[0].startswith("arc-gestaltung")
    backfill_images(conn, fetcher, "berlin")
    assert fetcher.calls == [url], "the page is revisited once, not on every run"


def test_a_page_without_a_credit_is_not_revisited_forever():
    from pipeline.images import backfill_images
    conn, url = _credit_db("https://www.berlin.de/land/kalender/data/281641-1.jpg")
    fetcher = _PageFetcher(CAL_PAGE.replace("image__copyright", "image__other"))
    backfill_images(conn, fetcher, "berlin")
    backfill_images(conn, fetcher, "berlin")
    assert fetcher.calls == [url]
    assert conn.execute("SELECT image_credit FROM events").fetchone()[0] is None
