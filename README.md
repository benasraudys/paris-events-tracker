# paris-events-tracker

Cheap and free student / youth events in Paris, scraped from several listing
sites into one calendar feed. Rebuilt daily by GitHub Actions and published to
GitHub Pages.

**📅 Subscribe:** `https://benasraudys.github.io/paris-events-tracker/paris-events.ics`
· [landing page](https://benasraudys.github.io/paris-events-tracker/)

Single-file [uv](https://docs.astral.sh/uv/) script with inline (PEP 723) deps — no venv setup:

```bash
uv run scrape_events.py
```

## Automation

`.github/workflows/update-calendar.yml` runs daily at 05:17 UTC, then sleeps a
random 0–40 min before scraping so the sites are not hit at the same
wall-clock second every day. It can also be run by hand from the Actions tab
(with an optional `force` input to bypass every cache).

The SQLite DB is carried between runs by `actions/cache`, which is what keeps a
daily run at ~3 requests instead of ~30; a cache miss just means one cold
start. Publishing is a **separate job** that only runs if the scrape succeeded,
so a broken run leaves the previously published calendar in place rather than
replacing it with something worse.

The scraper is built for unattended running: one failing source is logged and
skipped, the calendar is still written from everything that did work, and the
run only exits non-zero if *every* source failed.

## Usage

```bash
uv run scrape_events.py                      # sync + list upcoming Paris events
uv run scrape_events.py --free-only          # only free ones
uv run scrape_events.py --city lyon          # other cities
uv run scrape_events.py --source shotgun     # one source only
uv run scrape_events.py --json data/paris.json
uv run scrape_events.py --ics                 # -> data/paris-events.ics
uv run scrape_events.py --free-only --ics free.ics
uv run scrape_events.py --no-sync --ics      # rebuild the .ics, zero requests
uv run scrape_events.py --force              # ignore caches, refetch everything
uv run scrape_events.py --delay 10 -v        # slower + verbose
```

Data lands in `data/events.db` (`events` + `http_cache` tables). Events are
upserted, never deleted, with `first_seen` / `last_seen` stamps, so the DB
doubles as a history of what was listed when.

## Not getting IP-banned

The `Fetcher` class enforces all of this centrally, for every source:

| Measure | Detail |
| --- | --- |
| robots.txt | Fetched once per host and honoured before every request |
| Serial requests | One at a time — no concurrency, ever |
| Throttle | 4 s + up to 2 s random jitter between requests (`--delay`) |
| Incremental sync | Detail pages are refetched **only** when the index says the post changed |
| Fresh-cache TTL | `max_age` serves a recent cached copy with **no request at all** |
| Conditional GET | `If-None-Match` / `If-Modified-Since` from the local cache |
| Backoff | Exponential, honouring `Retry-After`, on 429 and 5xx |
| Bail-out | A 403 aborts the run rather than hammering a host that is blocking us |

The big win is the incremental sync, not the throttling:

- **First run:** ~30 requests ≈ 3 min (robots + 2 REST + 14 event pages;
  1 Eventbrite profile + 2 event pages; 1 Shotgun venue + 8 event pages)
- **Every run after:** **3 requests** ≈ 17 s — erasmusplace detail pages are
  skipped by change-stamp, Eventbrite's and Shotgun's by TTL

So a daily cron costs a handful of requests a day, and only pays for pages that
actually changed. Note that erasmusplace.com sends no `ETag` or `Last-Modified`
on event pages, so conditional GET is inert *on this host* — the `modified`
timestamp from the REST index is what does the real work here. Eventbrite pages
*do* send ETags, so conditional GET is live on that source.

## Calendar export

`--ics` writes an RFC 5545 file you can import into any calendar app, or drop
somewhere and subscribe to. Filters apply, so `--free-only --ics free.ics`
gives you a free-events-only calendar.

- **Times are Europe/Paris.** The file embeds a real `VTIMEZONE` with DST rules
  rather than naive local times, so events show at the right wall-clock time
  even if your calendar is set to another zone.
- **Midnight rollover is handled.** These are club nights: `23:00 -> 05:00`
  correctly ends the *next* day, as does `20:00 -> 00:30`.
- **Prices are in the summary** (`Kiss My Erasmus - Free`) because a dozen
  listings share the same title, and the price is what you're filtering on.
- **Stable UIDs** (`erasmusplace-287927@events-tracker`) mean re-importing an
  updated file refreshes existing entries instead of duplicating them.
- Events with no end time get a 2-hour default (`DEFAULT_DURATION`); events with
  a date but no time become all-day entries; undated events are skipped and
  counted in the log.

## Sources

### `erasmusplace` — erasmusplace.com

WordPress + JetEngine/Elementor, with the REST API left open. The cheapest path:

1. `GET /wp-json/wp/v2/ville` — resolve the city taxonomy term (Paris = 176).
2. `GET /wp-json/wp/v2/categories-evenements` — map term ids to slugs
   (`gratuit`, `payant`, `soirees`).
3. `GET /wp-json/wp/v2/evenement?ville=<id>&per_page=100&_fields=...` — the full
   city listing in **one** request, including each post's `modified` stamp.

Date / time / venue / price are JetEngine meta and are *not* exposed over REST
(the `acf` and meta fields come back empty), so they have to come from the
rendered page — one fetch per event, then cached against `modified`.

The detail pages are Elementor markup whose element ids (`elementor-element-2d1ba3a8`)
are regenerated on template edits, so scraping them by id would be brittle.
Instead fields are read **positionally by label**: walking widgets in document
order, each `text-editor` widget whose text ends in `:` (`Où :`, `Date :`,
`Horaire :`, `Horaire de fin :`, `Prix :`) labels the next non-empty
`jet-listing-dynamic-field` widget's value. That survives restyling and only
breaks if the labels themselves are reworded.

French dates (`lundi 7 septembre 2026`) and prices (`Gratuit`,
`À partir de 13€`) are normalised to ISO dates and floats, with
`is_free` cross-checked against the `gratuit` category as a fallback.

### `eventbrite` — eventbrite.fr organizer profiles

Tracks named organizer profiles, listed in `Eventbrite.ORGANIZERS`
(currently `24250650532` = *Paris Erasmus Life*). Add an id there to track
another organizer.

**Robots constraint shapes this one.** Eventbrite's robots.txt disallows
`/api/v3/destination/events/` — precisely the internal endpoint the profile page
uses to page through an organizer's events. So we never call it. The `/o/<id>`
page *is* allowed, and it server-renders the first batch of events into
`__NEXT_DATA__.props.pageProps.upcomingEvents`, which is enough for an
organizer with a normal number of upcoming events. If `hasMoreUpcoming` is
true, the scraper logs a warning about the untracked remainder rather than
reaching for the disallowed endpoint. If you need complete coverage of a
high-volume organizer, use Eventbrite's official API with your own OAuth token
instead of scraping.

**The organizer listing's own data is partly wrong**, which is why each event
page is also fetched once:

- It reports `end_time == start_time` as a placeholder. The welcome picnic is
  listed as `15:00 → 15:00`; the event page's JSON-LD says `15:00 → 20:00`.
- Its `minimum_ticket_price.display` can show the *maximum* ("27.84 EUR" beside
  a `major_value` of 11.85), and `maximum_ticket_price` duplicates the minimum.
  Only the numeric `major_value` is trusted, and the real range comes from the
  event page's `AggregateOffer` (`11.85–27.84 EUR`).
- Free events on the `.fr` site are sometimes tagged `USD` at 0.00. Harmless at
  zero, but a non-EUR amount is never recorded as `price_eur`.

Event pages carry clean schema.org `Event` JSON-LD (offset-aware
`startDate`/`endDate`, `AggregateOffer`, venue, description) **and** send
`ETag`s, so refreshes are conditional and `DETAIL_TTL` (12 h) skips asking at
all. Cancelled and online-only events are dropped, and events are filtered to
the requested city by venue address.

### `shotgun` — shotgun.live venue profiles

Tracks venue/organizer profiles listed in `Shotgun.VENUES` (currently
`paris-erasmus-life`). robots.txt is fully permissive and no bot challenge is
served to a normal browser UA.

The venue page is a Next.js App Router stream. Its JSON-LD describes only the
venue, and the event list lives in escaped RSC chunks (`self.__next_f.push`).
Rather than scrape values out of a serialised React tree, this source takes
**only the `(event_id, slug)` pairs** from the stream — the most stable thing in
it — and reads every real field from each event page's schema.org `MusicEvent`
JSON-LD, which is clean and complete.

Three source-specific traps, all handled:

- **Times are UTC here.** `startDate: 2026-09-12T21:30:00.000Z` is **23:30**
  in Paris, and the site itself displays "11:30 PM". Anything tz-aware is
  converted to Europe/Paris (this is why `tzdata` is a dependency on Windows).
- **A €0 offer can be sold out.** One night lists "free entry for girls before
  midnight" at €0 with `availability: SoldOut`, while the cheapest real ticket
  is €12.99. Only `InStock` offers count, or the night would look free.
- **Bottle packs share the offer list** (€525 magnum VIP), so a min–max range is
  meaningless. Only the cheapest available ticket is reported, as `from X EUR`.

Note this is a *venue* page, so its events are not necessarily organised under
that name — the Yoyo reggaeton night is organised by "Soirée à Paris". Events
are therefore filtered by **city only**, never by organizer; filtering by
organizer would drop everything.

There are no `ETag`s and pages are sent `no-store`, so conditional GET is
unavailable and `max_age` (6 h venue / 12 h detail) is the only thing keeping a
cron job's traffic down.

## Known gap: cross-source duplicates

There is no deduplication. `key` is `source:source_id`, so an event listed on
two sites appears twice — and with three sources this now happens for real:

| Date | erasmusplace | eventbrite | shotgun |
| --- | --- | --- | --- |
| 25 Sep | E-BOAT PARTY, from €13 | — | International Boat Party, from €11 |
| 3 Oct | — | Aquaboulevard, €11.85–27.84 | Aquaboulevard, from €10 |

Note the same night is cheaper on Shotgun in both cases, which is an argument
for *showing* both rather than silently merging them. If you'd rather have one
entry per event, the merge key would need to be fuzzy (date + venue + time),
since titles differ across sites.

## Adding a source

Each site gets its own bespoke handler — there is no generic HTML scraper, by
design. Subclass `Source`, implement `sync(city) -> Iterator[Event]`, and
register it:

```python
class SomeSite(Source):
    name = "somesite"

    def sync(self, city: str):
        data = self.fetcher.get_json("https://...")   # throttling is automatic
        for row in data:
            yield Event(source=self.name, source_id=..., url=..., title=...)

SOURCES = {ErasmusPlace.name: ErasmusPlace, SomeSite.name: SomeSite}
```

Use `self.fetcher` for all network access so the politeness rules apply, and set
`remote_modified` whenever the site exposes a change stamp — that is what makes
subsequent runs nearly free (or a TTL via `max_age` when it exposes none).
See **Known gap: cross-source duplicates** above.
