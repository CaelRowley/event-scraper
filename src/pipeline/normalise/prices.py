"""German price parsing — ordered regex cascade. Raw text is always kept (recovery path).

Order is load-bearing: free/donation rules run before numeric ones, but a numeric match
wins over a members-only free mention in mixed strings like "ab 15€ / Eintritt frei für Mitglieder".
"""

from __future__ import annotations

import re

from ..models import Price

_NUM = r"(\d{1,4}(?:[.,]\d{1,2})?)"

RE_FREE = re.compile(r"eintritt\s*frei|freier\s*eintritt|kostenlos|kostenfrei|\bgratis\b|free\s+(?:entry|admission)", re.I)
RE_DONATION = re.compile(r"spendenbasis|auf\s+spende|spendenempfehlung|\bspende\b|pay\s+what\s+you\s+(?:want|can)|donation", re.I)
RE_VVK_AK = re.compile(rf"vvk\.?:?\s*{_NUM}\s*€?.{{0,16}}?\bak\.?:?\s*{_NUM}", re.I | re.S)
RE_FROM = re.compile(rf"\bab\s*{_NUM}\s*(?:€|euro)", re.I)
RE_RANGE = re.compile(rf"{_NUM}\s*(?:€\s*)?[–\-—/]\s*{_NUM}\s*€", re.I)
RE_REDUCED = re.compile(rf"erm(?:äßigt|aessigt|\.)\s*:?\s*{_NUM}", re.I)
RE_SINGLE = re.compile(rf"{_NUM}\s*(?:€|euro)|€\s*{_NUM}", re.I)
RE_SOLD_OUT = re.compile(r"ausverkauft|sold\s*out", re.I)
RE_FEES_EXCL = re.compile(r"zzgl\.?\s*geb", re.I)


def _num(s: str) -> float:
    return float(s.replace(",", "."))


def parse_price(text: str | None, *, structured_value: float | None = None,
                is_free_hint: bool | None = None) -> tuple[Price, bool]:
    """Parse a price string. Returns (Price, sold_out).

    structured_value (e.g. JSON-LD offers.price) beats text parsing when present.
    """
    sold_out = False
    p = Price(text=text)

    if is_free_hint:
        p.is_free, p.type, p.min, p.max = True, "free", 0.0, 0.0
        return p, sold_out

    if structured_value is not None:
        if structured_value == 0:
            p.is_free, p.type, p.min, p.max = True, "free", 0.0, 0.0
        else:
            p.min = p.max = structured_value
            p.type = "fixed"
        return p, sold_out

    if not text:
        return p, sold_out

    s = text.replace(" ", " ").strip()
    sold_out = bool(RE_SOLD_OUT.search(s))
    has_number = bool(re.search(r"\d", s))

    m = RE_VVK_AK.search(s)
    if m:
        p.presale, p.door = _num(m.group(1)), _num(m.group(2))
        p.min, p.max, p.type = p.presale, p.door, "range"
        _apply_reduced(p, s)
        return p, sold_out

    if not has_number:
        if RE_FREE.search(s):
            p.is_free, p.type, p.min, p.max = True, "free", 0.0, 0.0
            return p, sold_out
        if RE_DONATION.search(s):
            p.type, p.min = "donation", 0.0
            return p, sold_out
        return p, sold_out  # type stays "unknown"; raw text kept

    if RE_DONATION.search(s) and not RE_SINGLE.search(s):
        p.type, p.min = "donation", 0.0
        return p, sold_out

    m = RE_FROM.search(s)
    if m:
        p.min, p.max, p.type = _num(m.group(1)), None, "from"
        _apply_reduced(p, s)
        return p, sold_out

    m = RE_RANGE.search(s)
    if m:
        lo, hi = sorted((_num(m.group(1)), _num(m.group(2))))
        p.min, p.max, p.type = lo, hi, "range"
        _apply_reduced(p, s)
        return p, sold_out

    m = RE_REDUCED.search(s)
    reduced = _num(m.group(1)) if m else None

    m = RE_SINGLE.search(s)
    if m:
        value = _num(m.group(1) or m.group(2))
        if reduced is not None and reduced != value:
            p.min, p.max, p.reduced, p.type = value, value, reduced, "fixed"
        else:
            p.min, p.max, p.type = value, value, "fixed"
        if value == 0:
            p.is_free, p.type = True, "free"
        return p, sold_out

    if RE_FREE.search(s):
        p.is_free, p.type, p.min, p.max = True, "free", 0.0, 0.0
    return p, sold_out


def _apply_reduced(p: Price, s: str) -> None:
    m = RE_REDUCED.search(s)
    if m:
        p.reduced = _num(m.group(1))
