# event-scraper

Collects upcoming social events for major cities (Berlin pilot), normalises them into
canonical JSON (title, datetime, venue, category, price, image, source link), dedupes
across sources, categorises them, and exports a static feed an app can consume.

**Demo:** `public/index.html` over the exported `public/berlin/index.json`.
The JSON isn't committed, so run the pipeline once locally to populate it.

## How it works

```
source policy (who may run, what may be published) → source adapters → fetch
(httpx + hishel RFC-9111 cache, robots gate, per-domain rate limit, retries) → extract (JSON-LD / hidden APIs / server-data blobs)
→ normalise (Europe/Berlin zoneinfo, German dates & prices, RRULE-ready occurrences)
→ dedup (blocked fuzzy matching, union-find) → categorise (rules)
→ SQLite (WAL) → takedown list → static JSON export → demo page
```

Categorisation is rules only — no model, no API key, $0 to run: **source
priors/fields** (RA ⇒ club, schema.org `@type`, Ticketmaster segment, Eventbrite
category tags, …) → **venue priors + DE/EN keyword rules**
(`src/pipeline/categorise/keywords.yaml`). An ambiguous keyword match is resolved
by match count (tier `keyword_multi`, confidence 0.5); zero-signal events stay
`other`. An earlier Claude Haiku tier was retired in October 2026: raw events
were enough. Categories it assigned are kept until their event changes or ends.

## Sources (Berlin)

Whether a source runs is decided in one place, `src/pipeline/sources.py`, from
that site's own terms, licence and robots.txt (last read 2026-11-12). A source
that is not permitted is not collected, its domains are not contacted by
anything, what it already stored is purged, and none of it is exported.

| source | access | status | what is published |
|---|---|---|---|
| kulturdaten.berlin | open-data API | **on** | facts, description, link; credited (no formal data licence is published — confirmation requested) |
| berlin.de markets & festivals | open-data JSON (`cc-by`) | **on** | facts, opening times/remarks, link; credited |
| Berliner Volkshochschulen | open-data JSON (`cc-by`) | **on** | courses starting in the window: facts, description, price, booking link; credited. No contact persons or teachers |
| Ticketmaster | Discovery API (key) | **on** | facts, image with its credit, link to the Ticketmaster page; no description |
| Prinzipal Kreuzberg | venue's public Events Calendar API | **on** | facts, image, link; no description |
| ra.co | undocumented GraphQL | off — needs RA's written agreement | — |
| tip-berlin.de | sitemap + JSON-LD | off — refuses the collector; data is licensed by Cine Marketing | — |
| livegigs.de | JSON-LD in listing pages | off — refuses the collector | — |
| comedyinenglish.de, comedycafeberlin.com | Events Calendar API | off — refused the collector; ask first | — |
| rausgegangen.de | sitemap + JSON-LD | off — robots.txt and terms forbid it | — |
| eventbrite.de | listing-page data blob | off — terms forbid it | — |

**Only events with an external link.** An event with no page to send a reader
to — no URL, something that is not a URL, or a raw API record — is skipped
before it is converted: not stored, not snapshotted, not exported
(`has_external_link`). kulturdaten events without a calendar page or website are
dropped before their location is even looked up, and rows earlier runs stored
without a link are deleted on the next run. Telemetry reports both under
`policy.no_link_skipped` and `policy.no_link_deleted`.

A source comes back when its site has agreed in writing: name its slug in
`PIPELINE_PERMITTED_SOURCES` (the `PERMITTED_SOURCES` repository variable in CI).
No code change, and nothing else switches a source on — `--source ra` does not.

Not built, by design: Songkick, Bandsintown, Meetup, Eventbrite's API (none
licenses a multi-source commercial listing without a separate deal), Eventim
(affiliate programme only, no data API), Google Places (no events).

## Run it

```bash
uv sync --extra dev          # exactly what uv.lock pins, same as CI
.venv/bin/python -m pytest -q

# smoke test two easy sources
.venv/bin/python -m pipeline run --city berlin --source kulturdaten --source berlin_de --limit 20

# full run (set TICKETMASTER_KEY for Ticketmaster)
.venv/bin/python -m pipeline run --city berlin --mode full

# view the demo
python -m http.server -d public 8000   # → http://localhost:8000
```

Other commands: `python -m pipeline export`, `python -m pipeline venues-bootstrap`
(Overpass/OSM venue pull, ODbL).

## Deployment

`.github/workflows/scrape.yml` runs twice daily, publishes the export window to
Cloudflare R2, and fails loudly when sources break or yield nothing.

| cron | mode | sources |
| --- | --- | --- |
| `0 3 * * *` | `full` | every source in the city registry |
| `0 13 * * *` | `delta` | the `cheap_delta` ones (kulturdaten, berlin.de) |

Which cron fired is what picks the mode, so the two `cron:` strings must stay
byte-identical to the `MODE` expression that reads them. The full run is the
authoritative refresh of the whole window; the delta run exists so an event
announced with a few days' notice is not invisible for up to 24 hours, and it
costs almost nothing — the HTTP cache revalidates rather than refetches. Both runs export and
publish the *whole* feed, because the export reads the database rather than
whatever the run just scraped. The `scrape` concurrency group serialises them.

Two checks bracket the publish. The **freshness gate** before it stops only what
would make the feed wrong: zero events, or a majority of sources failing. The
**source health** step after it fails the run for anything else — a source that
raised or was refused every request, or an enrichment phase that crashed — so
the alert still fires but a broken source no longer holds the whole feed back.

Dependencies install from `uv.lock` (`uv sync --frozen`), never fresh: an
unpinned `pip install` let selectolax 1.0 break every scrape for three weeks in
October 2026. Dependabot opens weekly upgrade PRs, which `test.yml` checks. The exported JSON is **not** committed — the bucket is its only
consumer, so `public/berlin/` is gitignored and regenerated by any run.

**The published feed.** Gobento reads the catalogue as static JSON, not from a
database. Every object except the manifest carries a hash of its own contents in
its name, so its bytes never change: clients cache it forever and re-fetch only
when the manifest names a hash they do not hold, and re-publishing uploads just
the events that actually changed (~500/day of ~6,400). The manifest is written
last, so a reader never sees it pointing at an object that is not there yet.

```
berlin/manifest.json                 window, per-day + per-category counts, current hashes
berlin/feed.<hash>.json.gz           every listable row, no descriptions
berlin/events/<id>.<hash>.json.gz    what a row lacks: description, occurrences, aliases
berlin/geo.<hash>.json.gz            the ~11% of rows carrying coordinates
```

`h` on a feed row is the only published route to a detail object, and two things
follow from that. A reader holding the hash necessarily holds the row, so the
object repeats none of `LIST_FIELDS` — it carries only what the row lacks. And an
event that never reaches a listing has no published hash, so no object is written
for it at all. With gzip on top (R2 serves bytes verbatim and will not compress
for you) that took Berlin's bucket from 31 MB to 12.6 MB, and the average detail
object from 1,507 B to 518 B. `SCHEMA_VERSION` is 2; a v1 client reading a v2
object finds no title or venue, so bump both sides together.

It also cut the churn. Title, venue, price and image live on the list row, so
editing one re-publishes the feed object and nothing else — only a changed
description, occurrence set or alias moves a detail object now.

Secrets: `R2_ACCOUNT_ID`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`.
A de-listed object is deleted only after 48h, because clients and CDN nodes still
hold the previous manifest.

**Source health.** A source can fail without raising. Adapters contain their own
HTTP errors, log a warning and yield nothing, so `errors` stays empty and the run
goes green — which is how four blocked sources once shipped a healthy-looking
feed. Zero yield is not the signal either: a sitemap source legitimately returns
nothing on a day with no new pages, and `tip_berlin` doing exactly that looked
identical to `livegigs` being refused 32 times.

What *is* unambiguous is a source whose every request was refused. The fetcher
tallies outcomes per source (attributed by thread name, which the runner sets to
the slug), counting 403/405/429/451 as blocked and leaving 404/410 out of it — a
missing page says nothing about our standing. `robots.txt` is fetched off the raw
client, so a blocked source cannot look healthy on the strength of one 200. The
run reports `source_http` and `blocked_sources` (which also counts a source
whose host tripped the circuit breaker mid-run), and the source health step warns
on the latter without failing the run.

A refusal is an answer, and we take it as given. Every request carries one
User-Agent — `events-pipeline/<version> (+<contact URL>)`, the contact URL being
the operator's imprint, which names the collector and says how to stop it — and
there is no browser User-Agent, no header set imitating one, and no per-source
override anywhere in the code. A Cloudflare Worker relay once routed eventbrite
around its block, and three adapters claimed to be Chrome; all of that is gone.
A site that refuses us, or whose robots.txt or terms rule us out, has its source
switched off in `sources.py`; that puts its domains on the list `fetch.is_denied`
checks before every request, so neither the adapter nor the image scan nor the
link check contacts it again (not even for robots.txt).

**robots.txt** is checked before every request, by every caller — adapters, the
image scan, the link check, the image check. There is no flag to skip it. A
`Crawl-delay` the site asks for overrides a shorter interval of ours. A
robots.txt that cannot be read (403, 429, 5xx, network error) means the host is
treated as disallowed for that run and counted as a refusal; a missing one (404)
means no rules. The only exemptions are two documented public APIs whose
site-wide robots.txt is written for page crawlers, listed in
`fetch.ROBOTS_EXEMPT_APIS` (Wikimedia Commons' `api.php`, Overpass's interpreter).

New refusals stay visible: the source health step prints a warning for every
source in `blocked_sources` and the circuit breaker gives up on a refusing host
after `TRIP_AFTER` (8) requests, so a newly hostile site costs a handful of
requests and a warning until it is parked.

**The two horizons.** `SCRAPE_WINDOW_DAYS` (31) is how far ahead adapters are
asked to look; `EXPORT_WINDOW_DAYS` (30) is how far ahead the feed publishes.
The one-day gap is deliberate: without it the last day of the feed is always the
day first discovered on that very run — the thinnest and least verified day in
it, and the one most likely to gain events tomorrow. The buffer day lands in the
database, gets a second pass, and is published the next morning as a settled
day. Neither horizon is stored state; both are derived from `today` on every
run, which is what makes the window slide on its own.

**Retention.** The export window is `today .. today + EXPORT_WINDOW_DAYS`, so an event
stops being published the morning after it happens — and used to stay in the
database forever anyway, along with its occurrences, raw snapshot, alias trail
and dedup bookkeeping. `retention.py` now deletes what is over, after the
export so a wrong cutoff could never take the feed down with it.

An event counts as over when its *last* occurrence is behind the cutoff — last,
not first, because a run of an exhibition is one event with many dates.
`RETAIN_PAST_DAYS` (2) is only a margin against clock skew and late-reported
dates; the feed never showed those days regardless. Run telemetry reports what
went as `retention`.

`crawl_ledger` is deliberately left alone: it is keyed by URL and it is what
`_discover()` reads to decide a sitemap page is new or changed, so dropping rows
there reclaims little and makes the next run re-fetch pages it already knows.
Pruning is safe to do aggressively now that ids are derived — a source
re-reporting something we deleted brings it back under the same id.

**Verifying a publish.** A green workflow proves the scrape yielded events and the
uploads returned 200 — not that the redaction fired or that the published shape is
the one this version writes; the freshness gate only counts events. Those are
properties of the artefact, so check the artefact:

```
python -m pipeline audit-feed --city berlin          # 500-object sample
python -m pipeline audit-feed --city berlin --all    # every object
```

It reads the manifest, the feed rows and the detail objects straight out of the
bucket and reports the published `schema_version`, the object counts, any pre-v2
uncompressed objects still lying around, and every email address or phone number
still reachable in a published object. That last check runs the same
`contains_contact` the export gate uses, so a disagreement between them is a bug,
not a judgement call.

**Retiring the v1 generation (once).** The cutover renames every detail object, so
the first v2 publish adds its objects beside the v1 ones rather than replacing
them, and the bucket briefly doubles. `publish`'s own pruning then refuses
forever: it compares the export against the bucket, sees it holding under half,
and reads that as a broken scrape. The guard is right to fire, so the old
generation is retired by hand instead — after the first v2 publish, run

```
python -m pipeline purge-legacy --city berlin           # list what would go
python -m pipeline purge-legacy --city berlin --apply   # delete it
```

It only ever deletes `<city>/events/*.json`, refuses unless a `.json.gz`
generation is already present, and leaves the manifest, feed and geo alone. It
matters beyond housekeeping: those objects hold source prose from before
`redact.py` existed, in a world-readable bucket. The workflow's only commit is a
keep-alive that fires just when the branch nears 60 days idle (GitHub disables
scheduled workflows on inactive repos); ordinary development makes it a no-op.

**The pipeline DB.** `data/` (SQLite DB + HTTP cache — the delta ledgers, detail
caches, image-scan ledger) persists between runs via
`actions/cache`, never via git. A cache miss costs one slow cold run and nothing
more, because the one thing that would have been expensive to lose is no longer
stored at all: **event ids are derived, not minted**. `ids.py` hashes
`(source_slug, source_event_id)`, so the same listing yields the same id on any
machine from any starting state, and a rebuilt database reproduces the catalogue
exactly rather than re-keying it. That is what makes the database a cache you can
throw away — no snapshot, no second bucket, nothing to keep in step.

Everything else in there self-heals: the HTTP cache refills, delta ledgers cost a
slower run, dedup clusters recompute. The
slowest to rebuild is the image-scan ledger (~150 pages/run), which affects
coverage, not correctness.

Secrets: `TICKETMASTER_KEY`, the `R2_*` set, and optionally `SUPPRESSIONS`
(private takedown entries). Variables: `PERMITTED_SOURCES` (sources that have
given permission), `STOCK_PHOTOS` (`1` to publish Commons stand-ins),
`SCRAPE_LIMIT`. (`ANTHROPIC_API_KEY`, `FETCH_PROXY_URL` and `FETCH_PROXY_TOKEN`
are no longer read and can be deleted.)

Sources fetch concurrently (one thread per adapter, own SQLite connection each);
per-domain politeness is enforced globally by domain locks in the shared fetcher,
so parallelism never changes request spacing toward any one site.
`PIPELINE_MAX_WORKERS` (default 12) caps the pool.

After dedup the enrichment phases run one after another, each fanning its HTTP
out across hosts (`probe.py`: one worker per host, so a host's spacing is
unchanged, and every database write stays on the main thread). A per-host cap
keeps any one site from setting a phase's length. Each phase is isolated: one
that raises is reported in `phase_errors` and the run carries on to export, and
none starts after `PIPELINE_ENRICH_DEADLINE_MIN` (default 45) minutes — they are
incremental, so a skipped phase just resumes next run.

The fetcher has a per-host circuit breaker: after `TRIP_AFTER` (8) refusals in a
row (403/405/429/451), it stops asking that host for the rest of the run.
Telemetry lists those in `tripped_hosts`. A refused sitemap page is not written
to the ledger either, so it is retried next run rather than treated as seen.

### Image backfill

Sources that don't ship images (kulturdaten, berlin.de, many JSON-LD pages) get a
backfill pass each run: the event's source page is fetched once ever (ledger-tracked,
robots-checked, ~150 pages/run) and the best image is pulled from JSON-LD →
`og:image`/`twitter:image` → largest content `<img>` inside `article`/`main`
(logo/icon/SVG names rejected, sub-200px images rejected). A found image is applied
to every event sharing that URL (recurring exhibitions), so coverage compounds across
runs. Images are hotlinked, never copied. A photo credit is published as
`image_attribution.author` where the source supplies one: Ticketmaster's
`attribution`, and the credit line berlin.de's calendar prints under each photo
(`p.image__copyright`). Images stored before credits were read are revisited
once each, 100 pages a run, until they have caught up.

### Dead-link removal

Feed events' source links are re-verified every ~3 days (~250 checks/run, HEAD with
GET confirmation). Only hard 404/410 counts — blocks (403/429), server errors, and
timeouts are inconclusive, and a page robots.txt disallows is not probed at all. An event is pulled from the feed after
**two** dead strikes ≥20 h apart; it's flagged (`link_dead_at`), not deleted, and if it
headed a dedup cluster a surviving source is promoted so the event stays listed.

### Other verifications & stock placeholders

Each run also verifies: **image liveness** (hotlinked images re-checked weekly; hard
404/410 or HTML-instead-of-image → nulled), **geo sanity** (coordinates outside the
city bounding box are dropped), **price sanity** (numeric values > 500 € are treated
as mis-parses and reverted to raw text), and **encoding health** (mojibake titles
counted in telemetry). Results land in run telemetry.

Imageless events get **no** `placeholder_url`: the client draws its own panel
from the category's colour and icon. LoremFlickr filled this slot until it was
measured — `art,gallery` (471 events, a third of all placeholders) answered HTTP
500 every time, and a nonsense keyword still returned a photo, so the keyword
never constrained the result. Broken URLs and irrelevant ones both ended at the
client's hardcoded fallback image, which is how one disco ball came to illustrate
German courses and library readings. It also hotlinked CC images without
per-image attribution, the caveat `placeholders.py` had flagged as unresolved.

`stock_photos.py` can fill the gap with a Wikimedia Commons photo, and is **off
by default** (`PIPELINE_STOCK_PHOTOS=1` turns it on). Those photos are mostly
CC BY / CC BY-SA: showing one without its author, its licence and a link to both
puts it out of licence, and Gobento shows the author's name alone, on one screen.
Until the client renders the whole credit wherever the photo appears, the feed
does not carry them. When it is on, the credit fields are plain text (Commons
returns HTML) and ride on the list row as `image_attribution`.

## Data & legal posture

Not legal advice — an operator's reading of each source's own documents, kept
next to the code that acts on it (`sources.py`).

* **Permission first.** A source runs only if its policy says so. Open data and
  official APIs are on; a site whose terms require written agreement is off
  until it gives one.
* **Honest identity, no evasion.** One identifying User-Agent with a contact
  URL on every request; robots.txt obeyed on every request; a refusal is never
  routed around.
* **Only what we may republish.** Facts (title, date, venue, price) and a link
  back for every event. Descriptions only for the sources whose licence covers
  prose (`SourcePolicy.description`), gated once at storage and again at export.
  Images are hotlinked, not re-hosted.
* **Credit travels with the data.** `manifest.sources` carries each source's
  name, licence, licence URL and credit line, keyed by a row's `source`;
  `manifest.attribution` is the same as one sentence. A client should show the
  source's credit wherever it shows the event.
* **Takedowns.** `suppressions.yaml` removes one listing — or everything from
  one organiser, venue or site — at the next export; see below.
* **Retention.** Events are deleted once they are over, and a switched-off
  source's events the same day. Ticketmaster fields are refreshed each run, not
  archived.

### Takedowns

Add an entry to `suppressions.yaml` and run the scrape workflow:

```yaml
events:
  - value: <feed event id>        # one listing
domains:
  - value: organiser.example      # everything linking to this site
venues:
  - value: Example Hall           # everything at this venue
```

`urls:` (exact, or a prefix ending in `*`) and `titles:` (contains) work the
same way. The event leaves the feed, the manifest and the demo export on that
run, and its detail object is deleted from the bucket immediately instead of
after the 48-hour grace. A list that fails to parse stops the export. The file
is public; entries that would expose the requester go in the
`PIPELINE_SUPPRESSIONS` secret instead (same YAML, merged).

### Contact stripping

Roughly one kulturdaten description in ten ends with a booking line carrying a real
person's phone number or address — council staff, a named organiser, a private
mobile. Permission to republish the text is not a basis for
re-publishing someone's contact details at a new address, and the data is German, so
GDPR applies. `redact.py` strips addresses and phone numbers from `title` and
`description` in `export.py:_row_to_item` — the one point where a stored row becomes
a published item, so the demo export and the feed cannot drift apart on it. Runs
report the count as `export.redacted` in telemetry.

Numbers are the hard half: German prose is full of digit runs that must survive
(`20.03.2024`, `8.30 - 9.30 Uhr`, `12,50 €`, postal codes). A candidate has to match
a shape *and* clear a digit-count floor, and a dotted run is only read as a number
when a `Tel.`/`Mobil` label vouches for it. Validated against 43,472 real strings
from the exported feed: 3,703 contacts removed, no date, time or price altered.

It removes contact *routes*, not identities — "Info und Anmeldung: Frau Muster" keeps
the name. Stripping names would need NER and would eat the performer and artist names
that are the point of the feed.

## Multi-city

A city is config (`src/pipeline/config.py`): timezone + source list. Ticketmaster
already works in other cities;
adding one means picking its local sources and seeding venue priors.

Trigger a full run to populate the bucket for the first time.

```
gh workflow run scrape.yml -f mode=full -R CaelRowley/event-scraper
```

`SCRAPE_LIMIT` (repo variable) caps events per source for dev smoke runs, and
makes the publish upload-only so a partial scrape can't de-list real objects;
`0` or unset means no cap — a normal full scrape with a pruning publish.

```
gh variable set SCRAPE_LIMIT --body "0" -R CaelRowley/event-scraper
```
