"""Strip personal contact details out of source prose before it is published.

Open-licensed descriptions (kulturdaten is the only source we carry prose from)
routinely end with a booking line — "Info und Anmeldung: Frau Muster Tel. 55 50 12
24", a named organiser's work address, a private mobile. The licence lets us
republish the text; it does not make us the right party to re-publish someone's
phone number at a new address, and roughly one description in ten carries one.
Everything here is applied at the single point where a database row becomes a
feed item, so the demo export and the published feed can't diverge.

The two halves work differently on purpose:

* **Addresses** are unambiguous — an `@` between two plausible labels is never
  anything else, so the pattern can be greedy.
* **Numbers** are not. German prose is dense with digit runs that must survive:
  dates (`20.03.2024`), times (`8.30 - 9.30 Uhr`), prices (`12,50 €`), postal
  codes, years, room numbers. So a candidate has to earn removal twice — match a
  shape *and* clear a digit-count floor — and dotted runs are only ever read as
  phone numbers when a `Tel.`/`Mobil` label vouches for them. A missed number is
  a bug to fix; a mangled opening time is a bug we'd never see, because the text
  still reads fine.

Removal, not masking: a `[redacted]` marker in the middle of a German sentence is
noise for every reader, and the point is that the datum is gone. Callers get the
count back so a run can report what it dropped.
"""

from __future__ import annotations

import re

# An address, plus the `name (at) host` form people write to dodge exactly this
# kind of scrub. The TLD is required — bare `@handle` mentions aren't addresses.
_EMAIL = re.compile(
    r"[A-Za-z0-9._%+-]+"
    r"\s*(?:@|\(\s*at\s*\)|\[\s*at\s*\])\s*"
    r"[A-Za-z0-9-]+(?:\s*(?:\.|\(\s*dot\s*\)|\[\s*dot\s*\])\s*[A-Za-z0-9-]+)*"
    r"\s*(?:\.|\(\s*dot\s*\)|\[\s*dot\s*\])\s*[A-Za-z]{2,}",
    re.IGNORECASE,
)

# A label vouching for the digits behind it. Only here is `.` allowed inside a
# number, because "Tel." has already ruled out a date.
_LABELLED = re.compile(
    r"(?:\b(?:tel|telefon|telefonnummer|fon|funk|mobil|handy|phone|fax|whatsapp)\b"
    r"\.?\s*(?:nr\.?|nummer)?\s*[:.]?\s*)"
    r"\(?\s*(?:\+|00)?[\d][\d\s()/.–—-]*",
    re.IGNORECASE,
)

# Unlabelled, so the shape alone has to carry it: an international prefix, or a
# national trunk `0` followed by a separator. `.` is excluded as a separator —
# that is what keeps `08.30 - 9.30 Uhr` and `20.03.2024` intact.
# The leading `(` of "(030) 55501 234" is part of the number and is consumed with
# it — left behind, it strands an opening bracket at the end of a line.
_BARE = re.compile(
    r"(?<![\w.,/-])"
    r"\(?\s*"
    r"(?:(?:\+|00)\s?49[\s()/–—-]*\d[\d\s()/–—-]*"
    r"|0\d{1,4}[\s()/–—-]+\d[\d\s()/–—-]*)"
)

# Shortest real German number we should act on. Berlin locals run to 7-8 digits
# and mobiles to 11; a 5-digit run is a postal code or a room number.
_MIN_DIGITS = 6

# A line that held nothing but a booking route now holds nothing but its label —
# "E-Mail:", "Kartentelefon:", "Info und Anmeldung unter". The label is not
# personal data, but a stranded one reads as a broken export, so the line goes.
# Matching a whole line (rather than the label alone) is what keeps a real
# "Kontakt:" heading that still introduces something below it.
_LABEL_ONLY_LINE = re.compile(
    r"^[\s\W]*(?:(?:e[\s-]*mail|mail|telefon|telefonnummer|kartentelefon|tel|fon|funk|mobil|handy"
    r"|fax|whatsapp|info|infos|informationen|anmeldung|anmeldungen|kontakt|rückfragen|auskunft"
    r"|anfragen|buchung|buchungen|bestellung|reservierung|reservierungen|karten|nr|nummer"
    r"|unter|bei|über|per|via|und|oder)\b[\s\W]*)+$",
    re.IGNORECASE,
)

# Punctuation and whitespace stranded by any of the above. `|` earns its place:
# kulturdaten writes booking lines as "address | number | venue", so removing both
# contact halves leaves the separators holding nothing up.
_ORPHAN_PUNCT = re.compile(r"(?:^|(?<=[\s]))[–—\-/,;:|]+(?=[\s.,;)\]]*(?:$|\n))")
# The same separator stranded at the head of a line, once what preceded it went.
_ORPHAN_SEP_START = re.compile(r"(?:^|(?<=\n))[ \t]*(?:\|[ \t]*)+")
# A separator now introducing nothing: "Infos:, www.example.de".
_LEADING_ORPHAN = re.compile(r"(?<=[:\-–])[ \t]*[,;][ \t]*")
# "…für Gruppen buchbar:" — the colon promised something that is gone.
_TRAILING_COLON = re.compile(r"[ \t]*:[ \t]*(?=\r?\n|$)")
# "Anmeldung erwünscht unter" — the fact survives, the dangling preposition shouldn't.
_TRAILING_PREP = re.compile(
    r"[ \t]+\b(?:unter|bei|über|per|via|an|zu)\b[ \t]*(?=\r?\n|$)", re.IGNORECASE
)
_EMPTY_BRACKETS = re.compile(r"[(\[]\s*[)\]]")
_REPEAT_PUNCT = re.compile(r"([,;:])\s*(?=[,;:])")
_SPACE_BEFORE_PUNCT = re.compile(r"[ \t]+(?=[.,;:!?])")
_RUNS = re.compile(r"[ \t]{2,}")
_BLANK_LINES = re.compile(r"(?:[ \t]*\r?\n){3,}")


def _digits(text: str) -> int:
    return sum(c.isdigit() for c in text)


def _strip_number(match: re.Match) -> str:
    """Drop a candidate only if it holds enough digits to be a real number.

    The trailing character class runs greedily into whatever whitespace and
    punctuation follows, so the tail is walked back to the last digit first —
    otherwise a match would swallow the space and the next sentence's opener.
    """
    text = match.group(0)
    end = max((i for i, c in enumerate(text) if c.isdigit()), default=-1)
    if end < 0:
        return text
    candidate = text[: end + 1]
    if _digits(candidate) < _MIN_DIGITS:
        return text
    return text[end + 1 :]


def _tidy_line(line: str) -> str:
    """Line-local cleanup of what a removal stranded."""
    line = _EMPTY_BRACKETS.sub("", line)
    line = _LEADING_ORPHAN.sub(" ", line)
    line = _ORPHAN_SEP_START.sub("", line)
    line = _ORPHAN_PUNCT.sub("", line)
    line = _TRAILING_COLON.sub("", line)
    line = _TRAILING_PREP.sub("", line)
    line = _REPEAT_PUNCT.sub(r"\1 ", line)
    line = _RUNS.sub(" ", line)
    line = _SPACE_BEFORE_PUNCT.sub("", line)
    return line.rstrip()


def _redact_line(line: str) -> tuple[str, int]:
    """Strip contacts from one line. Returns `(line, removals)`.

    Addresses go first, so that a `Tel: 030 1234 / mail@x.de` line can't have its
    address half consumed by the number pass.
    """
    removals = 0

    def drop_email(match: re.Match) -> str:
        nonlocal removals
        removals += 1
        return ""

    def drop_number(match: re.Match) -> str:
        nonlocal removals
        result = _strip_number(match)
        if result != match.group(0):
            removals += 1
        return result

    cleaned = _EMAIL.sub(drop_email, line)
    cleaned = _LABELLED.sub(drop_number, cleaned)
    cleaned = _BARE.sub(drop_number, cleaned)
    if not removals:
        return line, 0
    return _tidy_line(cleaned), removals


def redact_contacts(text: str | None) -> tuple[str | None, int]:
    """Return `(cleaned_text, removals)`. `None` and blank text pass through.

    Line by line, for two reasons: a number pattern can't run off the end of its
    line into the digits of the next, and a line that the redaction emptied down
    to its own label ("E-Mail:") can be told apart from a "Kontakt:" heading that
    still introduces something below it. Only the former is dropped.
    """
    if not text:
        return text, 0

    removals = 0
    kept: list[str] = []
    for raw in text.split("\n"):
        # CRLF sources are the norm here; the carriage return rides along so the
        # line's own end-anchored patterns see a clean boundary.
        line, eol = (raw[:-1], "\r") if raw.endswith("\r") else (raw, "")
        cleaned, found = _redact_line(line)
        removals += found
        if found and cleaned.strip() and _LABEL_ONLY_LINE.match(cleaned):
            continue
        kept.append((cleaned + eol) if found else raw)

    if not removals:
        return text, 0

    # A label the removal left at the very end introduces nothing at all — unlike
    # the same words mid-text, where a "Kontakt:" heading still heads a block.
    while kept and (not kept[-1].strip() or _LABEL_ONLY_LINE.match(kept[-1])):
        kept.pop()

    result = _BLANK_LINES.sub("\n\n", "\n".join(kept)).strip()
    return (result or None), removals


def contains_contact(text: str | None) -> bool:
    """True when `text` still holds an address or number. For tests and auditing."""
    if not text:
        return False
    return any(_redact_line(line)[1] for line in text.split("\n"))
