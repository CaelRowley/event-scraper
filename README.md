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

### Two categorisation modes

The mode is selected automatically — no config beyond the API key:

- **rules mode** (no `ANTHROPIC_API_KEY` set, or `--no-llm`): everything above except
  the Haiku tier. Ambiguous keyword matches are resolved by match count instead of
  deferred (tier `keyword_multi`, confidence 0.5); zero-signal events stay `other`.
  $0 to run — start here.
- **ai mode** (key present): identical, except ambiguous/zero-signal events go to the
  nightly Haiku batch and get upgraded in place — including everything rules mode
  guessed at or gave up on in *earlier* runs (the queue persists across runs).

So the upgrade path is exactly: run rules-only as long as you like, then add the
`ANTHROPIC_API_KEY` secret and the next run reclassifies the backlog. Telemetry
reports the active mode as `categorisation_mode`.

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

### Image backfill

Sources that don't ship images (kulturdaten, berlin.de, many JSON-LD pages) get a
backfill pass each run: the event's source page is fetched once ever (ledger-tracked,
robots-checked, ~150 pages/run) and the best image is pulled from JSON-LD →
`og:image`/`twitter:image` → largest content `<img>` inside `article`/`main`
(logo/icon/SVG names rejected, sub-200px images rejected). A found image is applied
to every event sharing that URL (recurring exhibitions), so coverage compounds across
runs. RA flyers come straight from its GraphQL `images[]` field — no scanning.

### Dead-link removal

Feed events' source links are re-verified every ~3 days (~250 checks/run, HEAD with
GET confirmation). Only hard 404/410 counts — blocks (403/429), server errors, and
timeouts are inconclusive, and ra.co is never checked (it blocks non-browser clients;
its GraphQL API is the liveness signal there). An event is pulled from the feed after
**two** dead strikes ≥20 h apart; it's flagged (`link_dead_at`), not deleted, and if it
headed a dedup cluster a surviving source is promoted so the event stays listed.

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
