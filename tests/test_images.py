from pipeline.images import find_image


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
