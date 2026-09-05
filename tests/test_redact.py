"""Contact-stripping: what must go, what must survive, and the export wiring."""
import json
from datetime import date
from pathlib import Path

from pipeline.db import connect, upsert_event
from pipeline.export import export_city
from pipeline.models import Event, Occurrence, Price
from pipeline.redact import contains_contact, redact_contacts

DAY = date.today().isoformat()

# Enough surrounding prose that a removal leaves a sentence behind — the shape a
# real kulturdaten description has.
LEAD = "Ein Abend mit Musik und Lesung im Garten der Bibliothek.\n"


def clean(text):
    return redact_contacts(text)[0]


# --- addresses -------------------------------------------------------------------

def test_removes_a_plain_address():
    assert "@" not in clean(LEAD + "Bei Fragen schreiben Sie an rudow@example.de gern.")


def test_removes_an_address_with_punctuation_in_the_local_part():
    assert "@" not in clean(LEAD + "Kontakt redacted@example.invalid heute")


def test_removes_an_at_obfuscated_address():
    assert "example" not in clean(LEAD + "Schreiben Sie an info (at) example (dot) de bitte")


def test_leaves_a_bare_handle_alone():
    # No TLD, so it is not an address — Instagram handles appear in these listings.
    text = LEAD + "Folgt uns auf Instagram @mobileswohnzimmer."
    assert clean(text) == text


# --- numbers ---------------------------------------------------------------------

def test_removes_an_international_number():
    assert "8410" not in clean(LEAD + "Karten über +49 (0)30 5550 1234 erhältlich.")


def test_removes_a_national_number_with_a_trunk_zero():
    assert "90239" not in clean(LEAD + "Auskunft erteilt 030 55501 234 gern.")


def test_removes_a_bracketed_area_code_together_with_its_bracket():
    cleaned = clean(LEAD + "Bei Fragen (030) 55501 234 anrufen.")
    assert "90239" not in cleaned and "(" not in cleaned


def test_removes_a_mobile_number():
    assert "5550000" not in clean(LEAD + "Mobil +49 (0) 162 5550000 erreichbar.")


def test_removes_a_local_number_that_only_a_label_identifies():
    # No trunk zero — "Tel." is the only thing marking these digits as a number.
    assert "56 58" not in clean(LEAD + "Info: Frau Muster Tel. 55 50 12 24 anrufen.")


def test_removes_a_number_written_with_slashes():
    assert "4595" not in clean(LEAD + "Erreichbar unter 030 / 5550 1234 täglich.")


# --- what must survive -----------------------------------------------------------

def test_keeps_dates():
    assert "20.03.2024" in clean(LEAD + "Neu ab 20.03.2024, Auskunft 030 55501 234.")


def test_keeps_times():
    cleaned = clean(LEAD + "Freitags 8.30 - 9.30 Uhr und 10.00 - 11.00 Uhr. Tel. 030 12345678")
    assert "8.30 - 9.30 Uhr" in cleaned and "10.00 - 11.00 Uhr" in cleaned


def test_keeps_prices():
    assert "12,50 €" in clean(LEAD + "Eintritt 12,50 € — Karten 030 555 01 23.")


def test_keeps_a_postal_code():
    assert "13088" in clean(LEAD + "Musterstr. 41, 13088 Berlin, Tel. 030 55502 345")


def test_keeps_a_year_range():
    assert "2023/24" in clean(LEAD + "Grabungen 2023/24. Buchbar 030 5550 123 45.")


def test_a_short_digit_run_is_not_a_number():
    # Below the digit floor — a room number, not a phone number.
    text = LEAD + "Treffpunkt ist Raum 0 12, wir freuen uns."
    assert clean(text) == text


def test_text_without_contacts_is_returned_untouched():
    text = "Ein Konzert im Garten.\r\n\r\nEintritt frei. Ab 18 Uhr, 20.03.2026."
    assert redact_contacts(text) == (text, 0)


def test_none_and_empty_pass_through():
    assert redact_contacts(None) == (None, 0)
    assert redact_contacts("") == ("", 0)


def test_prose_that_was_only_a_contact_line_becomes_nothing():
    # Correct for a nullable description column: there is no text left to publish.
    assert clean("Anmeldung: rudow@example.de") is None


# --- tidying ---------------------------------------------------------------------

def test_a_line_holding_only_a_label_is_dropped():
    cleaned = clean("Das Angebot ist kostenlos.\nE-Mail: lich@example.de\nBis bald.")
    assert "E-Mail" not in cleaned
    assert "Das Angebot ist kostenlos." in cleaned and "Bis bald." in cleaned


def test_a_heading_that_still_introduces_something_survives():
    cleaned = clean("Kontakt:\nUnser Team hilft gern.\nTel. 030 55501 234 anrufen.")
    assert "Kontakt:" in cleaned and "Unser Team hilft gern." in cleaned


def test_a_dangling_preposition_goes_but_the_fact_stays():
    assert clean("Der Kurs ist frei.\nAnmeldung erwünscht unter : 030 / 55 50 12 34") == (
        "Der Kurs ist frei.\nAnmeldung erwünscht")


def test_separators_between_two_removed_contacts_do_not_strand():
    cleaned = clean(LEAD + "Anmeldung: info@example.de | (030) 55501 567")
    assert "|" not in cleaned


def test_a_separator_in_ordinary_prose_survives():
    assert "Kostenfrei | Ab 12 Jahren" in clean(
        LEAD + "Tel. 030 55501 234.\nKostenfrei | Ab 12 Jahren")


def test_a_trailing_colon_promising_nothing_is_dropped():
    assert clean(LEAD + "Führungen buchbar: bildung@example.de / 030 5550 123 45") == (
        LEAD.strip() + "\nFührungen buchbar")


def test_redaction_is_idempotent():
    once = clean(LEAD + "Anmeldung: info@example.de | (030) 55501 567")
    assert redact_contacts(once) == (once, 0)


# --- the audit helper ------------------------------------------------------------

def test_contains_contact_agrees_with_the_scrubber():
    dirty = LEAD + "Anmeldung: rudow@example.de | (030) 55501 234"
    assert contains_contact(dirty)
    assert not contains_contact(clean(dirty))


def test_contains_contact_ignores_dates_and_times():
    assert not contains_contact("Am 20.03.2024 von 8.30 - 9.30 Uhr, Raum 12")


# --- wired into the export -------------------------------------------------------

def _seed(description, title="Lesung im Garten"):
    conn = connect(":memory:")
    upsert_event(conn, Event(
        id="", source="kulturdaten", source_event_id="E_1",
        source_url="https://api-v2.kulturdaten.berlin/api/events/E_1",
        title=title, city="berlin", venue_name="Stadtbibliothek", price=Price(),
        description=description, description_public=True, image_url="http://i/1",
        occurrences=[Occurrence(starts_at_utc=f"{DAY}T18:00:00Z",
                                starts_at_local=f"{DAY}T20:00:00+02:00",
                                nightlife_date=DAY)],
    ))
    conn.commit()
    return conn


def _published(tmp_path: Path) -> str:
    """Every JSON object the export wrote, concatenated — nothing may carry a contact."""
    return "".join(p.read_text(encoding="utf-8")
                   for p in (tmp_path / "berlin").rglob("*.json"))


def test_export_strips_contacts_from_every_published_shape(tmp_path: Path):
    conn = _seed("Eine Lesung im Garten.\nAnmeldung: rudow@example.de | (030) 55501 234")
    stats = export_city(conn, "berlin", tmp_path)

    assert stats["redacted"] == 1
    published = _published(tmp_path)
    assert "rudow@example.de" not in published
    assert "90239" not in published
    assert "Eine Lesung im Garten." in published


def test_export_reports_zero_when_nothing_needed_stripping(tmp_path: Path):
    conn = _seed("Eine Lesung am 20.03.2026 um 18.00 Uhr. Eintritt frei.")
    assert export_city(conn, "berlin", tmp_path)["redacted"] == 0


def test_a_contact_in_the_title_is_stripped_too(tmp_path: Path):
    conn = _seed("Eine Lesung im Garten.", title="Festivalpass (+49 (0)30 5550 1234")
    stats = export_city(conn, "berlin", tmp_path)

    assert stats["redacted"] == 1
    index = json.loads((tmp_path / "berlin" / "index.json").read_text(encoding="utf-8"))
    assert "8410" not in index["events"][0]["title"]
    assert "Festivalpass" in index["events"][0]["title"]
