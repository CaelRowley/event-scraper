"""Per-source policy: whether we may collect a source at all, and what of it we
may republish, under which licence, with which credit.

This is the one place that answers "are we allowed to?". Three things read it:

* `config.select_adapters` — a source that is not permitted never runs;
* `fetch.is_denied` — its domains are not contacted by anything (not the image
  scan, not the link check, not robots.txt);
* `export` — its events are not published, and for the sources that are, only
  the fields the policy allows (description, images) leave the database. The
  licence and credit line of every source in the feed go into the manifest.

A source is permitted when its status is `active`, or when the operator has
named it in `PIPELINE_PERMITTED_SOURCES` (comma-separated slugs) — which is what
you set once a site has given permission in writing. Anything not registered
here is not permitted: an adapter added without a policy does not publish.

The verdicts below come from reading each site's own terms, licence and
robots.txt on the `checked` date. They are an operator's reading, not legal
advice; `reason` says which clause decided it.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

PERMIT_ENV = "PIPELINE_PERMITTED_SOURCES"

ACTIVE = "active"
NEEDS_PERMISSION = "needs_permission"  # off until the site agrees in writing
STOP = "stop"                          # the site's terms rule it out; use its API/partner route


@dataclass(frozen=True)
class SourcePolicy:
    slug: str
    name: str                       # how the source is credited
    homepage: str
    status: str                     # ACTIVE | NEEDS_PERMISSION | STOP
    reason: str                     # the clause or fact behind the status
    domains: tuple[str, ...] = ()   # hosts that are this source's own site
    access: str = ""                # how the data is obtained
    licence: str | None = None      # licence as the source names it; None = none stated
    licence_url: str | None = None
    terms_url: str | None = None    # the document the verdict was read from
    attribution: str = ""           # credit line a client should display
    description: bool = False       # may its prose be republished
    images: bool = True             # may its images be shown (hotlinked, never re-hosted)
    checked: str = ""               # date the terms were last read


_POLICIES = (
    SourcePolicy(
        slug="kulturdaten",
        name="kulturdaten.berlin",
        homepage="https://www.kulturdaten.berlin/",
        status=ACTIVE,
        reason="Public open-data API whose operator invites reuse in other applications. "
               "No formal data licence is published for the API records (the project "
               "README states CC BY 3.0 DE for text and content); written confirmation "
               "has been requested.",
        domains=("kulturdaten.berlin",),
        access="open-data API (keyless)",
        licence=None,
        terms_url="https://www.kulturdaten.berlin/",
        attribution="Event data: kulturdaten.berlin (Technologiestiftung Berlin), "
                    "from the berlin.de district event calendar",
        description=True,
        checked="2026-11-12",
    ),
    SourcePolicy(
        slug="berlin_de",
        name="Berlin.de — Märkte und Feste",
        homepage="https://www.berlin.de/sen/web/service/maerkte-feste/",
        status=ACTIVE,
        reason="Berlin Open Data datasets 'Straßen- und Volksfeste' and "
               "'Weihnachtsmärkte', registered as cc-by; berlin.de's robots.txt terms "
               "require an identifying User-Agent and robots compliance, both met.",
        access="open-data JSON",
        licence="CC BY",
        licence_url="https://opendefinition.org/licenses/cc-by/",
        terms_url="https://www.berlin.de/robots.txt",
        attribution="Senatsverwaltung für Wirtschaft, Energie und Betriebe / Berlin.de "
                    "(Berlin Open Data, CC BY)",
        description=True,
        checked="2026-11-12",
    ),
    SourcePolicy(
        slug="ticketmaster",
        name="Ticketmaster",
        homepage="https://www.ticketmaster.de/",
        status=ACTIVE,
        reason="Official Discovery API under the Developer Terms of Use: event content "
               "cached only as long as needed, removed within 24 hours on request, "
               "every event linked to its Ticketmaster page.",
        access="Discovery API (key)",
        licence="Ticketmaster Developer Terms of Use",
        terms_url="https://developer.ticketmaster.com/support/terms-of-use/",
        attribution="Event data and images: Ticketmaster",
        description=False,
        checked="2026-11-12",
    ),
    SourcePolicy(
        slug="prinzipal",
        name="Prinzipal Kreuzberg",
        homepage="https://prinzipal-kreuzberg.com/",
        status=ACTIVE,
        reason="The venue's own public Events Calendar REST endpoint; robots.txt allows "
               "it and the site has no terms on automated access. Facts and a link only.",
        domains=("prinzipal-kreuzberg.com",),
        access="public Events Calendar REST API",
        terms_url="https://prinzipal-kreuzberg.com/robots.txt",
        attribution="Prinzipal Kreuzberg",
        description=False,
        checked="2026-11-12",
    ),
    SourcePolicy(
        slug="ra",
        name="Resident Advisor",
        homepage="https://ra.co/",
        status=NEEDS_PERMISSION,
        reason="ra.co Terms of Use 4.4(a) and 4.4(f): automated access, and extracting "
               "data for commercial purposes, need RA's prior written agreement. The "
               "GraphQL endpoint is undocumented and only answered a browser User-Agent.",
        domains=("ra.co",),
        access="undocumented GraphQL endpoint",
        terms_url="https://ra.co/terms",
        attribution="Resident Advisor",
        checked="2026-11-12",
    ),
    SourcePolicy(
        slug="rausgegangen",
        name="Rausgegangen",
        homepage="https://rausgegangen.de/",
        status=STOP,
        reason="robots.txt disallows every crawler not on its allow-list, and AGB "
               "section IX forbids automated collection and reserves text and data "
               "mining (§ 44b UrhG). Only a written cooperation agreement changes that.",
        domains=("rausgegangen.de",),
        access="sitemap + JSON-LD",
        terms_url="https://rausgegangen.de/conditions/",
        attribution="Rausgegangen",
        checked="2026-11-12",
    ),
    SourcePolicy(
        slug="tip_berlin",
        name="tip Berlin",
        homepage="https://www.tip-berlin.de/",
        status=NEEDS_PERMISSION,
        reason="The site's firewall refuses this collector, and its calendar is "
               "licensed data from Cine Marketing GmbH, which sells a data delivery.",
        domains=("tip-berlin.de",),
        access="sitemap + JSON-LD",
        terms_url="https://cine-marketing-gmbh.de/datenlieferung-service.html",
        attribution="tip Berlin",
        checked="2026-11-12",
    ),
    SourcePolicy(
        slug="livegigs",
        name="livegigs.de",
        homepage="https://www.livegigs.de/",
        status=NEEDS_PERMISSION,
        reason="The site refuses this collector's requests; its terms reserve "
               "commercial reuse and its robots.txt reserves rights under Art. 4 DSM.",
        domains=("livegigs.de",),
        access="JSON-LD in listing pages",
        terms_url="https://www.livegigs.de/agb",
        attribution="livegigs.de",
        checked="2026-11-12",
    ),
    SourcePolicy(
        slug="eventbrite",
        name="Eventbrite",
        homepage="https://www.eventbrite.de/",
        status=STOP,
        reason="Terms of Service § 13 forbid scraping and commercial use of site "
               "content; the API has had no public event search since 2019.",
        domains=("eventbrite.de", "eventbrite.com", "eventbrite.nl", "eventbrite.co.uk"),
        access="listing-page data blob",
        terms_url="https://www.eventbrite.com/help/en-us/articles/251210/eventbrite-terms-of-service/",
        attribution="Eventbrite",
        checked="2026-11-12",
    ),
    SourcePolicy(
        slug="comedy_in_english",
        name="Comedy in English",
        homepage="https://comedyinenglish.de/",
        status=NEEDS_PERMISSION,
        reason="The site refused this collector's requests. Its robots.txt allows the "
               "endpoint with a 3-second crawl delay; ask before returning.",
        domains=("comedyinenglish.de",),
        access="public Events Calendar REST API",
        terms_url="https://comedyinenglish.de/robots.txt",
        attribution="Comedy in English",
        checked="2026-11-12",
    ),
    SourcePolicy(
        slug="comedy_cafe",
        name="Comedy Café Berlin",
        homepage="https://www.comedycafeberlin.com/",
        status=NEEDS_PERMISSION,
        reason="The site refused this collector's requests, and the adapter only ever "
               "worked with a browser User-Agent. Ask before returning.",
        domains=("comedycafeberlin.com",),
        access="public Events Calendar REST API",
        terms_url="https://www.comedycafeberlin.com/robots.txt",
        attribution="Comedy Café Berlin",
        checked="2026-11-12",
    ),
)

POLICIES: dict[str, SourcePolicy] = {p.slug: p for p in _POLICIES}

# Hosts nothing contacts whatever the source list says. ticketmaster.de's event
# pages refuse this collector (the Discovery API is a different host), so the
# image scan and the link check leave them alone.
ALWAYS_DENIED = ("ticketmaster.de",)


def _flagged() -> frozenset[str]:
    raw = os.environ.get(PERMIT_ENV, "")
    return frozenset(s.strip() for s in raw.split(",") if s.strip())


def policy(slug: str) -> SourcePolicy | None:
    return POLICIES.get(slug)


def is_permitted(slug: str) -> bool:
    """Whether this source may be collected and published right now."""
    p = POLICIES.get(slug)
    if p is not None and p.status == ACTIVE:
        return True
    return slug in _flagged()


def permitted_slugs() -> list[str]:
    return sorted({s for s in POLICIES if is_permitted(s)} | _flagged())


def unpermitted_slugs() -> list[str]:
    return sorted(s for s in POLICIES if not is_permitted(s))


def denied_domains() -> frozenset[str]:
    """Domains no request may go to: every unpermitted source's own site."""
    denied = set(ALWAYS_DENIED)
    for p in POLICIES.values():
        if not is_permitted(p.slug):
            denied.update(p.domains)
    return frozenset(denied)


def may_publish_description(slug: str) -> bool:
    p = POLICIES.get(slug)
    return bool(p and p.description)


def may_publish_images(slug: str) -> bool:
    p = POLICIES.get(slug)
    return bool(p and p.images)


def credits(slugs) -> dict[str, dict]:
    """The licence and credit line of each named source, for the published manifest."""
    out: dict[str, dict] = {}
    for slug in sorted(set(slugs)):
        p = POLICIES.get(slug)
        if p is None:
            continue
        out[slug] = {
            "name": p.name,
            "url": p.homepage,
            "access": p.access,
            "licence": p.licence,
            "licence_url": p.licence_url,
            "terms_url": p.terms_url,
            "attribution": p.attribution,
        }
    return out
