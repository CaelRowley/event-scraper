# event-scraper

Collects upcoming social events for major cities (Berlin pilot), normalises them into
canonical JSON (title, datetime, venue, category, price, image, source link), dedupes
across sources, categorises them, and exports a static feed an app can consume.

**Demo:** `public/index.html` over the exported `public/berlin/index.json`
(served by GitHub Pages once enabled for this repo).

## How it works

```
source adapters → fetch (httpx + hishel RFC-9111 cache, robots gate, per-domain
rate limit, retries) → extract (JSON-LD / hidden APIs / server-data blobs)
→ normalise (Europe/Berlin zoneinfo, German dates & prices, RRULE-ready occurrences)
→ dedup (blocked fuzzy matching, union-find) → categorise (3-tier cascade)
→ SQLite (WAL) → static JSON export → demo page
```

Categorisation cascade: **source priors/fields** (RA ⇒ club, schema.org `@type`,
Ticketmaster segment, Eventbrite category tags, …) → **venue priors + DE/EN keyword
rules** (`src/pipeline/categorise/keywords.yaml`) → **Claude Haiku batch** for the
ambiguous remainder (Message Batches API, strict JSON-schema output, ~$0.28 per
1,000 events; `confidence < 0.6` falls back to `other`).

## Sources (Berlin)

| source | access | notes |
|---|---|---|
| kulturdaten.berlin | open-data API (CC-BY) | cultural backbone; attraction/location details cached & budget-capped |
| ra.co | GraphQL | club/electronic; browser UA + ≤1 req/s; never scrape its HTML |
| rausgegangen.de | sitemap + JSON-LD | national aggregator, Berlin-filtered |
| tip-berlin.de | sitemap + JSON-LD | URL paths encode categories |
| berlin.de | Simple-Search JSON | markets & street festivals; identifying UA required |
| livegigs.de | JSON-LD in listing pages | concerts; full window in ~15 fetches |
| eventbrite.de | `__SERVER_DATA__` blob (brace-matched) | workshops/talks/community long tail |
| Ticketmaster | Discovery API (key) | supplement; minimal fields, refreshed every run |

Dead/blocked by design: Songkick, Bandsintown, Eventbrite search API, Eventim,
Google Places events. Deferred: Meetup (ToS), museumsportal, per-venue scrapers.

## Run it

```bash
uv venv --python 3.12 .venv && uv pip install -p .venv -e '.[dev]'
.venv/bin/python -m pytest -q

# smoke test two easy sources
.venv/bin/python -m pipeline run --city berlin --source kulturdaten --source berlin_de --limit 20 --no-llm

# full run (set ANTHROPIC_API_KEY to enable the LLM tier, TICKETMASTER_KEY for TM)
.venv/bin/python -m pipeline run --city berlin --mode full

# view the demo
python -m http.server -d public 8000   # → http://localhost:8000
```

Other commands: `python -m pipeline export`, `python -m pipeline venues-bootstrap`
(Overpass/OSM venue pull, ODbL).

## Deployment

`.github/workflows/scrape.yml` runs 2×/day (03:00 full / 13:00 delta UTC), commits
`data/` + `public/` back to the repo (storage + Pages serving + the 60-day cron
keep-alive in one), and fails loudly when sources break or yield nothing.
Secrets: `ANTHROPIC_API_KEY`, `TICKETMASTER_KEY`.

## Data & legal posture

Facts only (title/date/venue/price), never editorial prose; every exported item links
back to its original listing; honest identifying User-Agent with contact email
(browser UA only where a source blocks non-browser clients — RA, Eventbrite);
robots.txt enforced via Protego; per-source kill switch in `config.py`;
images are hotlinked with a placeholder fallback (no re-hosting);
Ticketmaster fields are refreshed each run, not archived.

## Multi-city

A city is config (`src/pipeline/config.py`): timezone + source list. rausgegangen
(~60 German cities), Ticketmaster, and Eventbrite already work in other cities;
adding one means picking its local sources and seeding venue priors.
