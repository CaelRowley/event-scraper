from pipeline.normalise.prices import parse_price


def test_free():
    p, _ = parse_price("Eintritt frei")
    assert p.is_free and p.type == "free" and p.min == 0


def test_kostenlos():
    p, _ = parse_price("Der Eintritt ist kostenlos")
    assert p.is_free


def test_donation():
    p, _ = parse_price("auf Spendenbasis")
    assert p.type == "donation" and not p.is_free


def test_vvk_ak():
    p, _ = parse_price("VVK 12 / AK 15")
    assert p.presale == 12 and p.door == 15 and p.min == 12 and p.max == 15


def test_vvk_ak_with_euro_and_comma():
    p, _ = parse_price("VVK: 12,50 € · AK: 15 €")
    assert p.presale == 12.5 and p.door == 15


def test_from_price():
    p, _ = parse_price("Tickets ab 15€")
    assert p.type == "from" and p.min == 15 and p.max is None


def test_range():
    p, _ = parse_price("10 - 25 €")
    assert p.min == 10 and p.max == 25 and p.type == "range"


def test_comma_decimal():
    p, _ = parse_price("12,50 €")
    assert p.min == 12.5 and p.type == "fixed"


def test_euro_symbol_first():
    p, _ = parse_price("€ 18")
    assert p.min == 18


def test_reduced():
    p, _ = parse_price("20 €, ermäßigt 12 €")
    assert p.min == 20 and p.reduced == 12


def test_sold_out_kept():
    p, sold_out = parse_price("25 € — ausverkauft!")
    assert sold_out and p.min == 25


def test_mixed_free_for_members_prefers_number():
    p, _ = parse_price("ab 15€ / Eintritt frei für Mitglieder")
    assert p.type == "from" and p.min == 15 and not p.is_free


def test_unparseable_keeps_raw():
    p, _ = parse_price("Preis siehe Webseite")
    assert p.type == "unknown" and p.text == "Preis siehe Webseite" and p.min is None


def test_structured_beats_text():
    p, _ = parse_price("whatever", structured_value=9.5)
    assert p.min == 9.5 and p.type == "fixed"


def test_structured_zero_is_free():
    p, _ = parse_price(None, structured_value=0)
    assert p.is_free
