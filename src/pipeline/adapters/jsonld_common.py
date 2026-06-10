"""Shared schema.org Event JSON-LD → RawEvent mapping (used by every JSON-LD source)."""

from __future__ import annotations

import logging

from ..extract.jsonld import first_of, text_value
from ..models import RawEvent

log = logging.getLogger(__name__)


def _first(value):
    return value[0] if isinstance(value, list) and value else value


def _image_url(value) -> str | None:
    v = _first(value)
    if isinstance(v, dict):
        v = v.get("url") or v.get("contentUrl")
    return str(v) if v else None


def _offers(node: dict) -> tuple[float | None, str | None, bool | None]:
    """(structured price, raw price text, is_free hint) from schema.org offers."""
    offers = node.get("offers")
    if offers is None:
        return None, None, None
    offers = offers if isinstance(offers, list) else [offers]
    prices: list[float] = []
    texts: list[str] = []
    for offer in offers:
        if not isinstance(offer, dict):
            continue
        for key in ("price", "lowPrice", "highPrice"):
            v = offer.get(key)
            if v in (None, ""):
                continue
            try:
                prices.append(float(str(v).replace(",", ".")))
            except ValueError:
                texts.append(str(v))
    if prices:
        low = min(prices)
        text = f"{low:g}–{max(prices):g} €" if len(set(prices)) > 1 else f"{low:g} €"
        return low if len(set(prices)) == 1 else None, text if len(set(prices)) > 1 else f"{low:g} €", (low == 0) or None
    return None, (texts[0] if texts else None), None


def jsonld_to_raw(node: dict, *, source: str, page_url: str) -> RawEvent | None:
    title = text_value(node.get("name"))
    start = first_of(node, "startDate")
    if not title or not start:
        return None
    url = text_value(node.get("url")) or page_url
    if url.startswith("/"):
        from urllib.parse import urljoin

        url = urljoin(page_url, url)

    location = _first(node.get("location")) or {}
    if not isinstance(location, dict):
        location = {"name": str(location)}
    address = location.get("address") or {}
    if not isinstance(address, dict):
        address = {"streetAddress": str(address)}
    geo = location.get("geo") or {}

    price_value, price_text, free_hint = _offers(node)

    schema_type = node.get("@type")
    if isinstance(schema_type, list):
        schema_type = schema_type[0] if schema_type else None

    def _coord(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    return RawEvent(
        source=source,
        source_event_id=url,  # detail-page URL is the stable id for JSON-LD sources
        source_url=url,
        title=title,
        start=str(_first(start)),
        end=str(_first(node["endDate"])) if node.get("endDate") else None,
        doors=text_value(node.get("doorTime")),
        venue_name=text_value(location.get("name")),
        street=text_value(address.get("streetAddress")),
        postal_code=text_value(address.get("postalCode")),
        city=text_value(address.get("addressLocality")),
        lat=_coord(geo.get("latitude")),
        lon=_coord(geo.get("longitude")),
        price_value=price_value,
        price_text=price_text,
        is_free=free_hint or (node.get("isAccessibleForFree") is True or None),
        image_url=_image_url(node.get("image")),
        schema_type=str(schema_type) if schema_type else None,
        category_raw=str(schema_type) if schema_type else None,
        description=text_value(node.get("description")),
        event_status=str(node.get("eventStatus") or "scheduled"),
        attendance_mode=str(node.get("eventAttendanceMode") or "offline"),
        payload={"jsonld": node},
    )
