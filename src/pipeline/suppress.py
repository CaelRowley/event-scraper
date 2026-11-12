"""The takedown list: events that must not be published, whatever the sources say.

When an organiser, a venue or a rights holder asks for a listing to go, it is
added to `suppressions.yaml` at the repository root and the next export leaves
it out — of the feed, the detail objects and the demo export alike. One entry
can name a single event, or everything from one organiser:

    events:    feed event ids (an occurrence id `<event>::<start>` works too)
    urls:      a listing's URL, or a prefix ending in `*`
    domains:   every event linking to this site (subdomains included)
    venues:    every event at this venue
    titles:    every event whose title contains this text

Each entry is `- value: …`, optionally with `reason:` and `date:` for the record;
a bare string is accepted as well.

The repository is public, so the file is too. Where naming the requester would
itself expose them, put the same YAML in the `PIPELINE_SUPPRESSIONS` environment
variable (a repository secret) instead — both are read and merged.

A file that cannot be parsed stops the export. Publishing an event someone asked
to have removed, because the list had a typo in it, is the failure worth a red run.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from urllib.parse import urlsplit

import yaml

from .venues import norm_name

log = logging.getLogger(__name__)

FILE_ENV = "PIPELINE_SUPPRESSIONS_FILE"
INLINE_ENV = "PIPELINE_SUPPRESSIONS"
DEFAULT_FILE = "suppressions.yaml"

SECTIONS = ("events", "urls", "domains", "venues", "titles")


def _norm_url(url: str) -> str:
    return url.strip().lower().rstrip("/")


def _values(section, name: str) -> list[str]:
    if section is None:
        return []
    if not isinstance(section, list):
        raise ValueError(f"suppressions: `{name}` must be a list")
    out = []
    for entry in section:
        value = entry.get("value") if isinstance(entry, dict) else entry
        if value is None or not str(value).strip():
            raise ValueError(f"suppressions: an entry under `{name}` has no value: {entry!r}")
        out.append(str(value).strip())
    return out


class Suppressions:
    def __init__(self, data: dict | None = None):
        data = data or {}
        if not isinstance(data, dict):
            raise ValueError("suppressions: the top level must be a mapping of sections")
        unknown = set(data) - set(SECTIONS)
        if unknown:
            raise ValueError(f"suppressions: unknown section(s) {sorted(unknown)}; "
                             f"expected {list(SECTIONS)}")
        self.events = {v.split("::")[0] for v in _values(data.get("events"), "events")}
        urls = [_norm_url(v) for v in _values(data.get("urls"), "urls")]
        self.urls = {u for u in urls if not u.endswith("*")}
        self.url_prefixes = tuple(u[:-1] for u in urls if u.endswith("*"))
        self.domains = tuple(v.lower().lstrip(".") for v in _values(data.get("domains"), "domains"))
        self.venues = {norm_name(v) for v in _values(data.get("venues"), "venues")}
        self.titles = tuple(norm_name(v) for v in _values(data.get("titles"), "titles"))

    def __len__(self) -> int:
        return (len(self.events) + len(self.urls) + len(self.url_prefixes)
                + len(self.domains) + len(self.venues) + len(self.titles))

    def merged(self, other: "Suppressions") -> "Suppressions":
        out = Suppressions()
        out.events = self.events | other.events
        out.urls = self.urls | other.urls
        out.url_prefixes = self.url_prefixes + other.url_prefixes
        out.domains = self.domains + other.domains
        out.venues = self.venues | other.venues
        out.titles = self.titles + other.titles
        return out

    def match(self, *, event_ids=(), url: str | None = None, venue: str | None = None,
              title: str | None = None) -> str | None:
        """Which rule suppresses this event, or None. `event_ids` is the event's id
        plus every id it has ever been published under (its aliases)."""
        for eid in event_ids:
            if eid and eid.split("::")[0] in self.events:
                return "events"
        if url:
            u = _norm_url(url)
            if u in self.urls or any(u.startswith(p) for p in self.url_prefixes):
                return "urls"
            host = (urlsplit(url).hostname or "").lower()
            if any(host == d or host.endswith("." + d) for d in self.domains):
                return "domains"
        if venue and self.venues and norm_name(venue) in self.venues:
            return "venues"
        if title and self.titles:
            t = norm_name(title)
            if any(needle and needle in t for needle in self.titles):
                return "titles"
        return None


def _parse(text: str, where: str) -> Suppressions:
    try:
        return Suppressions(yaml.safe_load(text) or {})
    except yaml.YAMLError as exc:
        raise ValueError(f"suppressions: {where} is not valid YAML: {exc}") from exc


def load(path: str | Path | None = None) -> Suppressions:
    """The file (if present) merged with the inline environment list (if set)."""
    out = Suppressions()
    file_path = Path(path or os.environ.get(FILE_ENV) or DEFAULT_FILE)
    if file_path.exists():
        out = out.merged(_parse(file_path.read_text(encoding="utf-8"), str(file_path)))
    inline = os.environ.get(INLINE_ENV, "").strip()
    if inline:
        out = out.merged(_parse(inline, f"${INLINE_ENV}"))
    return out
