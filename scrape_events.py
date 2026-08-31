# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "httpx>=0.27",
#     "selectolax>=0.3.21",
#     "tzdata>=2024.1; sys_platform == 'win32'",
# ]
# ///
"""
events-tracker - scrape cheap/free student & youth events in Paris.

Each source gets its own bespoke handler (see the SOURCES registry near the
bottom). Sources are deliberately NOT generic: every site is reverse-engineered
for its own cheapest, most stable data path.

Politeness is enforced centrally by Fetcher:
  * robots.txt is honoured
  * one request at a time, per-host delay + jitter
  * conditional GETs (ETag / Last-Modified) so unchanged pages cost ~0 bytes
  * incremental sync: a source can skip a page entirely when the index says
    it has not been modified since the last run
  * exponential backoff honouring Retry-After on 429/5xx

Usage:
    uv run scrape_events.py                 # sync + show upcoming Paris events
    uv run scrape_events.py --free-only
    uv run scrape_events.py --json out.json
    uv run scrape_events.py --no-sync       # print from local DB, zero requests
    uv run scrape_events.py --force         # ignore caches, refetch everything
"""

from __future__ import annotations

import argparse
import dataclasses
import html as html_mod
import json
import logging
import random
import re
import sqlite3
import sys
import time
import unicodedata
import urllib.robotparser
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator
from zoneinfo import ZoneInfo

import httpx
from selectolax.parser import HTMLParser

PARIS_TZ = ZoneInfo("Europe/Paris")

# Cached page bodies older than this are dropped, so a long-lived DB (e.g. a
# CI cache restored every day) does not grow without bound.
CACHE_RETENTION = timedelta(days=30)

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "data" / "events.db"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

log = logging.getLogger("events")


# --------------------------------------------------------------------------
# Model
# --------------------------------------------------------------------------

@dataclasses.dataclass(slots=True)
class Event:
    source: str
    source_id: str
    url: str
    title: str
    city: str | None = None
    venue: str | None = None
    start_date: str | None = None       # ISO YYYY-MM-DD
    start_time: str | None = None       # HH:MM
    end_date: str | None = None         # set when the source states it explicitly
    end_time: str | None = None
    price_text: str | None = None       # as displayed, e.g. "a partir de 13EUR"
    price_eur: float | None = None      # parsed lower bound; 0.0 == free
    is_free: bool | None = None
    categories: list[str] = dataclasses.field(default_factory=list)
    description: str | None = None
    image: str | None = None
    remote_modified: str | None = None   # source's own change stamp

    @property
    def key(self) -> str:
        return f"{self.source}:{self.source_id}"


# --------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    key             TEXT PRIMARY KEY,
    source          TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    url             TEXT NOT NULL,
    title           TEXT NOT NULL,
    city            TEXT,
    venue           TEXT,
    start_date      TEXT,
    start_time      TEXT,
    end_date        TEXT,
    end_time        TEXT,
    price_text      TEXT,
    price_eur       REAL,
    is_free         INTEGER,
    categories      TEXT,
    description     TEXT,
    image           TEXT,
    remote_modified TEXT,
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_date ON events(start_date);

CREATE TABLE IF NOT EXISTS http_cache (
    url           TEXT PRIMARY KEY,
    etag          TEXT,
    last_modified TEXT,
    body          BLOB,
    fetched_at    TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        self._migrate()
        self.db.commit()

    def _migrate(self) -> None:
        """Add columns introduced after a DB was first created."""
        have = {r["name"] for r in self.db.execute("PRAGMA table_info(events)")}
        for column, decl in (("end_date", "TEXT"),):
            if column not in have:
                log.debug("migrating: adding events.%s", column)
                self.db.execute(f"ALTER TABLE events ADD COLUMN {column} {decl}")

    # -- events ----------------------------------------------------------
    def remote_stamps(self, source: str) -> dict[str, str]:
        """source_id -> remote_modified, for incremental sync."""
        rows = self.db.execute(
            "SELECT source_id, remote_modified FROM events WHERE source = ?", (source,)
        )
        return {r["source_id"]: (r["remote_modified"] or "") for r in rows}

    def load(self, source: str, source_id: str) -> Event | None:
        row = self.db.execute(
            "SELECT * FROM events WHERE key = ?", (f"{source}:{source_id}",)
        ).fetchone()
        return _row_to_event(row) if row else None

    def upsert(self, ev: Event) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        self.db.execute(
            """
            INSERT INTO events (key, source, source_id, url, title, city, venue,
                start_date, start_time, end_date, end_time, price_text, price_eur,
                is_free, categories, description, image, remote_modified,
                first_seen, last_seen)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(key) DO UPDATE SET
                url=excluded.url, title=excluded.title, city=excluded.city,
                venue=excluded.venue, start_date=excluded.start_date,
                start_time=excluded.start_time, end_date=excluded.end_date,
                end_time=excluded.end_time,
                price_text=excluded.price_text, price_eur=excluded.price_eur,
                is_free=excluded.is_free, categories=excluded.categories,
                description=excluded.description, image=excluded.image,
                remote_modified=excluded.remote_modified, last_seen=excluded.last_seen
            """,
            (ev.key, ev.source, ev.source_id, ev.url, ev.title, ev.city, ev.venue,
             ev.start_date, ev.start_time, ev.end_date, ev.end_time,
             ev.price_text, ev.price_eur,
             None if ev.is_free is None else int(ev.is_free),
             json.dumps(ev.categories, ensure_ascii=False), ev.description, ev.image,
             ev.remote_modified, now, now),
        )
        self.db.commit()

    def touch(self, source: str, source_id: str) -> None:
        """Mark an unchanged event as still present, without refetching it."""
        self.db.execute(
            "UPDATE events SET last_seen = ? WHERE key = ?",
            (datetime.now().isoformat(timespec="seconds"), f"{source}:{source_id}"),
        )
        self.db.commit()

    def query(self, *, city: str | None = None, free_only: bool = False,
              upcoming: bool = True) -> list[Event]:
        sql = "SELECT * FROM events WHERE 1=1"
        args: list[object] = []
        if city:
            sql += " AND lower(city) = ?"
            args.append(city.lower())
        if free_only:
            sql += " AND (is_free = 1 OR price_eur = 0)"
        if upcoming:
            sql += " AND (start_date IS NULL OR start_date >= ?)"
            args.append(date.today().isoformat())
        sql += " ORDER BY start_date IS NULL, start_date, start_time"
        return [_row_to_event(r) for r in self.db.execute(sql, args)]

    # -- http cache ------------------------------------------------------
    def cache_get(self, url: str) -> sqlite3.Row | None:
        return self.db.execute(
            "SELECT * FROM http_cache WHERE url = ?", (url,)
        ).fetchone()

    def prune_cache(self, older_than: timedelta) -> None:
        """
        Drop stale cached bodies so a long-lived DB stops growing.

        Housekeeping must never sink a run: a locked DB or a VACUUM that cannot
        acquire the file is logged and shrugged off.
        """
        cutoff = (datetime.now() - older_than).isoformat(timespec="seconds")
        try:
            cur = self.db.execute("DELETE FROM http_cache WHERE fetched_at < ?",
                                  (cutoff,))
            if not cur.rowcount:
                return
            log.info("pruned %d cached page(s) older than %s", cur.rowcount, older_than)
            self.db.commit()
            self.db.execute("VACUUM")
        except sqlite3.OperationalError as exc:
            log.warning("cache prune skipped: %s", exc)

    def cache_put(self, url: str, etag: str | None, last_modified: str | None,
                  body: bytes) -> None:
        self.db.execute(
            "INSERT INTO http_cache (url, etag, last_modified, body, fetched_at) "
            "VALUES (?,?,?,?,?) ON CONFLICT(url) DO UPDATE SET etag=excluded.etag, "
            "last_modified=excluded.last_modified, body=excluded.body, "
            "fetched_at=excluded.fetched_at",
            (url, etag, last_modified, body,
             datetime.now().isoformat(timespec="seconds")),
        )
        self.db.commit()


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        source=row["source"], source_id=row["source_id"], url=row["url"],
        title=row["title"], city=row["city"], venue=row["venue"],
        start_date=row["start_date"], start_time=row["start_time"],
        end_date=row["end_date"], end_time=row["end_time"],
        price_text=row["price_text"],
        price_eur=row["price_eur"],
        is_free=None if row["is_free"] is None else bool(row["is_free"]),
        categories=json.loads(row["categories"] or "[]"),
        description=row["description"], image=row["image"],
        remote_modified=row["remote_modified"],
    )


# --------------------------------------------------------------------------
# Polite fetcher
# --------------------------------------------------------------------------

class Blocked(Exception):
    """A host is actively refusing us; stop touching it this run."""


class Fetcher:
    """Single-threaded, rate-limited, robots-aware, conditional-GET HTTP client."""

    def __init__(self, store: Store, *, delay: float = 4.0, jitter: float = 2.0,
                 force: bool = False, timeout: float = 30.0):
        self.store = store
        self.delay = delay
        self.jitter = jitter
        self.force = force
        self.client = httpx.Client(
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
                "Accept": "text/html,application/xhtml+xml,"
                          "application/json;q=0.9,*/*;q=0.8",
            },
            timeout=timeout,
            follow_redirects=True,
        )
        self._last_hit: dict[str, float] = {}
        self._robots: dict[str, urllib.robotparser.RobotFileParser | None] = {}
        # Hosts that have made it clear they don't want us this run. Retrying
        # each remaining URL against a host that is already answering 429 just
        # burns 20 minutes and annoys it further.
        self._blocked: dict[str, str] = {}
        self.stats = {"fetched": 0, "not_modified": 0, "fresh": 0, "skipped": 0}

    # -- robots ----------------------------------------------------------
    def _allowed(self, url: str) -> bool:
        parsed = httpx.URL(url)
        host = parsed.netloc.decode()
        if host not in self._robots:
            rp: urllib.robotparser.RobotFileParser | None
            rp = urllib.robotparser.RobotFileParser()
            robots_url = f"{parsed.scheme}://{host}/robots.txt"
            try:
                r = self.client.get(robots_url)
                rp.parse(r.text.splitlines() if r.status_code == 200 else [])
                log.debug("robots.txt loaded for %s (%s)", host, r.status_code)
            except httpx.HTTPError as exc:
                log.warning("robots.txt unreadable for %s (%s) - assuming allowed",
                            host, exc)
                rp = None
            self._robots[host] = rp
            self._last_hit[host] = time.monotonic()
        rp = self._robots[host]
        return True if rp is None else rp.can_fetch(USER_AGENT, url)

    # -- throttle --------------------------------------------------------
    def _wait(self, host: str) -> None:
        last = self._last_hit.get(host)
        if last is not None:
            gap = self.delay + random.uniform(0, self.jitter)
            remaining = gap - (time.monotonic() - last)
            if remaining > 0:
                log.debug("throttle %.1fs (%s)", remaining, host)
                time.sleep(remaining)
        self._last_hit[host] = time.monotonic()

    # -- get -------------------------------------------------------------
    def get(self, url: str, *, cache: bool = True, max_retries: int = 4,
            max_age: timedelta | None = None) -> bytes | None:
        """
        Return the response body, or None if unavailable/disallowed.

        `max_age` serves a still-fresh cached copy without touching the network
        at all - the cheapest possible outcome, for sources that expose no
        change stamp of their own.
        """
        if not self._allowed(url):
            log.warning("robots.txt disallows %s - skipping", url)
            self.stats["skipped"] += 1
            return None

        host = httpx.URL(url).netloc.decode()
        if host in self._blocked:
            raise Blocked(f"{host} already refused us this run "
                          f"({self._blocked[host]})")

        cached = None if self.force else (self.store.cache_get(url) if cache else None)

        if cached is not None and max_age is not None:
            age = datetime.now() - datetime.fromisoformat(cached["fetched_at"])
            if age < max_age:
                log.debug("cache still fresh (%s old): %s", age, url)
                self.stats["fresh"] += 1
                return bytes(cached["body"])

        headers: dict[str, str] = {}
        if cached:
            if cached["etag"]:
                headers["If-None-Match"] = cached["etag"]
            if cached["last_modified"]:
                headers["If-Modified-Since"] = cached["last_modified"]

        throttled = 0
        for attempt in range(max_retries):
            self._wait(host)
            try:
                r = self.client.get(url, headers=headers)
            except httpx.HTTPError as exc:
                wait = 2 ** attempt * 5
                log.warning("network error %s (%s) - retry in %ss", url, exc, wait)
                time.sleep(wait)
                continue

            if r.status_code == 304 and cached:
                log.debug("304 not modified: %s", url)
                self.stats["not_modified"] += 1
                return bytes(cached["body"])

            if r.status_code in (429, 500, 502, 503, 504):
                if r.status_code == 429:
                    throttled += 1
                retry_after = r.headers.get("Retry-After", "")
                wait = float(retry_after) if retry_after.isdigit() else 2 ** attempt * 10
                log.warning("HTTP %s on %s - backing off %.0fs",
                            r.status_code, url, wait)
                time.sleep(wait)
                continue

            if r.status_code == 403:
                raise Blocked(
                    f"HTTP 403 on {url} - the site may be blocking this client; "
                    f"abandoning this source so we do not make it worse")

            r.raise_for_status()
            self.stats["fetched"] += 1
            if cache:
                self.store.cache_put(url, r.headers.get("ETag"),
                                     r.headers.get("Last-Modified"), r.content)
            return r.content

        if throttled == max_retries:
            # Rate-limited on every attempt, including the first: this host is
            # refusing this client outright (datacenter IPs often are), not
            # reacting to our pace. Give up on the whole host, not just the URL.
            reason = f"HTTP 429 on every attempt at {url}"
            self._blocked[host] = reason
            raise Blocked(f"{host} is rate-limiting this client - {reason}")
        log.error("giving up on %s after %d attempts", url, max_retries)
        return None

    def get_json(self, url: str, **kw):
        body = self.get(url, **kw)
        return None if body is None else json.loads(body)

    def close(self) -> None:
        self.client.close()


# --------------------------------------------------------------------------
# Shared parsing helpers
# --------------------------------------------------------------------------

FR_MONTHS = {
    "janvier": 1, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6,
    "juillet": 7, "aout": 8, "septembre": 9, "octobre": 10, "novembre": 11,
    "decembre": 12,
}


def _deaccent(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")


def parse_fr_date(text: str) -> str | None:
    """'lundi 7 septembre 2026' -> '2026-09-07'."""
    if not text:
        return None
    m = re.search(r"(\d{1,2})\s+([a-z]+)\s+(\d{4})", _deaccent(text.lower()))
    if not m:
        return None
    day, month_name, year = m.groups()
    month = FR_MONTHS.get(month_name)
    if not month:
        return None
    try:
        return date(int(year), month, int(day)).isoformat()
    except ValueError:
        return None


def parse_price(text: str) -> tuple[float | None, bool | None]:
    """'Gratuit' -> (0.0, True); 'a partir de 13EUR' -> (13.0, False)."""
    if not text or not text.strip():
        return None, None
    t = _deaccent(text.lower())
    if "gratuit" in t or "free" in t:
        return 0.0, True
    m = re.search(r"(\d+(?:[.,]\d{1,2})?)", t)
    if m:
        value = float(m.group(1).replace(",", "."))
        return value, value == 0.0
    return None, None


def clean(text: str | None) -> str | None:
    if text is None:
        return None
    return re.sub(r"\s+", " ", text).strip() or None


def _time_only(s: str | None) -> str | None:
    if not s:
        return None
    m = re.search(r"(\d{1,2})[:hH](\d{2})", s)
    return f"{int(m.group(1)):02d}:{m.group(2)}" if m else clean(s)


def _truncate(s: str | None, n: int) -> str | None:
    return s[: n - 1].rstrip() + "..." if s and len(s) > n else s


# --------------------------------------------------------------------------
# Source base
# --------------------------------------------------------------------------

class Source:
    name: str = ""

    def __init__(self, fetcher: Fetcher, store: Store):
        self.fetcher = fetcher
        self.store = store

    def sync(self, city: str) -> Iterator[Event]:
        raise NotImplementedError


# --------------------------------------------------------------------------
# Source: erasmusplace.com
# --------------------------------------------------------------------------

class ErasmusPlace(Source):
    """
    erasmusplace.com is WordPress with an open REST API and a JetEngine/Elementor
    front end.

    Strategy (cheapest path found):
      1. One REST call resolves the `ville` taxonomy term for the city.
      2. One REST call lists every `evenement` in that city, including each
         post's `modified` stamp and its `categories-evenements` terms
         (gratuit / payant / soirees).
      3. Date, time, venue and price live only in the rendered page, so each
         event page is fetched ONCE and never again unless `modified` changes.
         A steady-state run is therefore ~3 requests, not ~17.

    The detail page is Elementor markup with unstable element ids, so fields are
    read positionally by label: each `text-editor` widget whose text ends in ':'
    labels the next non-empty `jet-listing-dynamic-field` widget's value.
    """

    name = "erasmusplace"
    BASE = "https://erasmusplace.com"
    API = f"{BASE}/wp-json/wp/v2"

    LABELS = {
        "ou": "venue",
        "date": "date",
        "horaire": "start_time",
        "horaire de fin": "end_time",
        "prix": "price",
    }

    def sync(self, city: str) -> Iterator[Event]:
        term = self._city_term(city)
        if term is None:
            log.error("[%s] unknown city %r", self.name, city)
            return
        cats = self._categories()
        listing = self._list_events(term)
        if listing is None:
            return

        known = self.store.remote_stamps(self.name)
        log.info("[%s] %d event(s) listed for %s", self.name, len(listing), city)

        for post in listing:
            sid = str(post["id"])
            modified = post.get("modified") or ""
            if not self.fetcher.force and known.get(sid) == modified:
                cached = self.store.load(self.name, sid)
                if cached:
                    self.store.touch(self.name, sid)
                    log.debug("[%s] unchanged, using local copy: %s", self.name, sid)
                    yield cached
                    continue

            ev = self._detail(post, city, cats)
            if ev is not None:
                yield ev

    # -- REST ------------------------------------------------------------
    def _city_term(self, city: str) -> int | None:
        data = self.fetcher.get_json(
            f"{self.API}/ville?per_page=100&_fields=id,slug,name", cache=False
        )
        if not data:
            return None
        want = _deaccent(city.lower())
        for t in data:
            if want in (_deaccent(t["slug"].lower()), _deaccent(t["name"].lower())):
                return t["id"]
        log.info("[%s] available cities: %s", self.name,
                 ", ".join(sorted(t["slug"] for t in data)))
        return None

    def _categories(self) -> dict[int, str]:
        data = self.fetcher.get_json(
            f"{self.API}/categories-evenements?per_page=100&_fields=id,slug", cache=False
        )
        return {t["id"]: t["slug"] for t in (data or [])}

    def _list_events(self, term_id: int) -> list[dict] | None:
        url = (f"{self.API}/evenement?ville={term_id}&per_page=100&orderby=date"
               "&_fields=id,slug,link,title,modified,categories-evenements")
        return self.fetcher.get_json(url, cache=False)

    # -- detail page -----------------------------------------------------
    def _detail(self, post: dict, city: str, cats: dict[int, str]) -> Event | None:
        url = post["link"]
        body = self.fetcher.get(url)
        if body is None:
            return None

        tree = HTMLParser(body.decode("utf-8", "replace"))
        fields = self._read_labelled_fields(tree)

        price_text = clean(fields.get("price"))
        price_eur, is_free = parse_price(price_text or "")
        cat_slugs = [cats.get(c, str(c)) for c in post.get("categories-evenements", [])]
        if is_free is None and "gratuit" in cat_slugs:
            price_eur, is_free = 0.0, True

        og = tree.css_first('meta[property="og:image"]')

        return Event(
            source=self.name,
            source_id=str(post["id"]),
            url=url,
            title=clean(html_mod.unescape(post["title"]["rendered"])) or post["slug"],
            city=city.title(),
            venue=clean(fields.get("venue")),
            start_date=parse_fr_date(fields.get("date", "")),
            start_time=_time_only(fields.get("start_time")),
            end_time=_time_only(fields.get("end_time")),
            price_text=price_text,
            price_eur=price_eur,
            is_free=is_free,
            categories=cat_slugs,
            description=_truncate(clean(self._description(tree)), 1200),
            image=og.attributes.get("content") if og else None,
            remote_modified=post.get("modified"),
        )

    def _read_labelled_fields(self, tree: HTMLParser) -> dict[str, str]:
        """Walk widgets in document order, binding each ':' label to the next value."""
        out: dict[str, str] = {}
        pending: str | None = None
        for node in tree.css("div.elementor-widget[data-widget_type]"):
            kind = node.attributes.get("data-widget_type") or ""
            text = clean(node.text()) or ""
            if kind.startswith("text-editor"):
                if text.endswith(":") and len(text) < 30:
                    pending = self.LABELS.get(_deaccent(text.rstrip(":").strip().lower()))
            elif kind.startswith("jet-listing-dynamic-field") and pending and text:
                out[pending] = text
                pending = None
        return out

    @staticmethod
    def _description(tree: HTMLParser) -> str | None:
        best = ""
        for node in tree.css(".jet-listing-dynamic-field__content"):
            text = clean(node.text()) or ""
            if len(text) > len(best):
                best = text
        return best or None


# --------------------------------------------------------------------------
# Source: eventbrite.fr organizer profiles
# --------------------------------------------------------------------------

class Eventbrite(Source):
    """
    Tracks specific Eventbrite organizer profiles (see ORGANIZERS).

    Robots note: Eventbrite's robots.txt disallows
    `/api/v3/destination/events/`, which is the internal endpoint the profile
    page uses to page through an organizer's events. We therefore never call it.
    Instead the organizer page itself is allowed, and it server-renders the
    first batch of events into `__NEXT_DATA__` - enough for organizers with a
    normal number of upcoming events. If `hasMoreUpcoming` is true we log a
    warning rather than reaching for the disallowed endpoint.

    The organizer listing's times cannot be trusted: it reports
    `end_time == start_time` as a placeholder, and its `maximum_ticket_price`
    contradicts its own `display` string. The event page's JSON-LD carries the
    real start/end (with offsets) and a proper AggregateOffer price range, so
    each event page is fetched once and cached. Those pages do send ETags, so
    refreshes are conditional, and DETAIL_TTL avoids even asking for a while.
    """

    name = "eventbrite"

    # Organizer profiles to track: id -> label (label is for humans only).
    ORGANIZERS = {
        "24250650532": "Paris Erasmus Life",
    }

    PROFILE_TTL = timedelta(hours=6)    # new events appear at most a few times a week
    DETAIL_TTL = timedelta(hours=12)    # prices/times move slowly

    def sync(self, city: str) -> Iterator[Event]:
        for oid, label in self.ORGANIZERS.items():
            log.info("[%s] organizer %s (%s)", self.name, oid, label)
            yield from self._organizer(oid, city)

    def _organizer(self, oid: str, city: str) -> Iterator[Event]:
        body = self.fetcher.get(f"https://www.eventbrite.fr/o/{oid}",
                                max_age=self.PROFILE_TTL)
        if body is None:
            return
        data = _next_data(body)
        if data is None:
            log.error("[%s] no __NEXT_DATA__ on organizer %s - page layout changed",
                      self.name, oid)
            return

        props = data.get("props", {}).get("pageProps", {})
        listing = props.get("upcomingEvents") or []
        total = props.get("upcomingEventsTotal")
        if props.get("hasMoreUpcoming"):
            log.warning("[%s] organizer %s embeds %d of %s upcoming events; paging "
                        "needs /api/v3/destination/events/ which robots.txt "
                        "disallows, so the rest are not tracked",
                        self.name, oid, len(listing), total)
        log.info("[%s] %d upcoming event(s) embedded", self.name, len(listing))

        for item in listing:
            ev = self._event(item, city)
            if ev is not None:
                yield ev

    def _event(self, item: dict, city: str) -> Event | None:
        if item.get("is_cancelled"):
            log.debug("[%s] skipping cancelled: %s", self.name, item.get("name"))
            return None
        if item.get("is_online_event"):
            log.debug("[%s] skipping online: %s", self.name, item.get("name"))
            return None

        venue = item.get("primary_venue") or {}
        address = venue.get("address") or {}
        ev_city = address.get("city")
        if city and (ev_city or "").lower() != city.lower():
            log.debug("[%s] skipping %s (city=%s)", self.name, item.get("name"), ev_city)
            return None

        url = item["url"]
        ld = self._detail_ld(url)

        # Times: prefer the event page's JSON-LD, which is offset-aware and
        # actually carries an end. Fall back to the listing's own fields.
        start_date = start_time = end_date = end_time = None
        if ld and ld.get("startDate"):
            start_date, start_time = _split_iso(ld["startDate"])
            if ld.get("endDate"):
                end_date, end_time = _split_iso(ld["endDate"])
        if start_date is None:
            start_date = item.get("start_date")
            start_time = (item.get("start_time") or "")[:5] or None
            # The listing repeats start_time as end_time; that is not an end.
            if item.get("end_time") and item.get("end_time") != item.get("start_time"):
                end_date = item.get("end_date")
                end_time = item["end_time"][:5]

        price_text, price_eur, is_free = self._price(item, ld)

        location = venue.get("name")
        display = address.get("localized_address_display")
        if location and display and display not in location:
            location = f"{location}, {display}"

        image = (item.get("image") or {}).get("url")
        description = clean(ld.get("description")) if ld else None

        return Event(
            source=self.name,
            source_id=str(item["id"]),
            url=url,
            title=clean(html_mod.unescape(item.get("name") or "")) or url,
            city=(ev_city or city).title(),
            venue=clean(location),
            start_date=start_date,
            start_time=start_time,
            end_date=end_date,
            end_time=end_time,
            price_text=price_text,
            price_eur=price_eur,
            is_free=is_free,
            categories=["sold-out"] if (item.get("ticket_availability") or {}
                                        ).get("is_sold_out") else [],
            description=_truncate(description, 1200),
            image=image,
            remote_modified=None,   # Eventbrite exposes none; DETAIL_TTL covers it
        )

    def _detail_ld(self, url: str) -> dict | None:
        """The event page's schema.org Event blob, or None."""
        body = self.fetcher.get(url, max_age=self.DETAIL_TTL)
        if body is None:
            return None
        ld = find_event_ld(body)
        if ld is None:
            log.debug("[%s] no Event JSON-LD on %s", self.name, url)
        return ld

    def _price(self, item: dict, ld: dict | None) -> tuple[str | None, float | None,
                                                           bool | None]:
        tickets = item.get("ticket_availability") or {}
        is_free = tickets.get("is_free")

        low = high = None
        currency = None
        offers = (ld or {}).get("offers") or []
        for offer in (offers if isinstance(offers, list) else [offers]):
            if not isinstance(offer, dict):
                continue
            currency = offer.get("priceCurrency") or currency
            for field, target in (("lowPrice", "low"), ("highPrice", "high")):
                try:
                    value = float(offer[field])
                except (KeyError, TypeError, ValueError):
                    continue
                if target == "low":
                    low = value if low is None else min(low, value)
                else:
                    high = value if high is None else max(high, value)

        if low is None:
            # Fall back to the listing's minimum. Its `display` string is
            # unreliable (it can show the maximum), so use the numeric field.
            minimum = tickets.get("minimum_ticket_price") or {}
            currency = minimum.get("currency") or currency
            try:
                low = float(minimum["major_value"])
            except (KeyError, TypeError, ValueError):
                low = None

        if is_free or (low == 0 and high in (None, 0)):
            return "Free", 0.0, True
        if low is None:
            return None, None, is_free
        # Only report a EUR figure when the source actually said EUR; free
        # events on the .fr site are sometimes tagged USD, which is harmless
        # at zero but would be wrong to trust for a real amount.
        if currency and currency != "EUR":
            log.debug("[%s] non-EUR price (%s) on %s", self.name, currency, item["url"])
            return f"{low:.2f} {currency}", None, False
        if high is not None and high > low:
            return f"{low:.2f}-{high:.2f} EUR", low, False
        return f"{low:.2f} EUR", low, False


def find_event_ld(body: bytes) -> dict | None:
    """First schema.org Event-ish JSON-LD blob on a page (any *Event @type)."""
    tree = HTMLParser(body.decode("utf-8", "replace"))
    for node in tree.css('script[type="application/ld+json"]'):
        try:
            blob = json.loads(node.text())
        except (json.JSONDecodeError, ValueError):
            continue
        for candidate in (blob if isinstance(blob, list) else [blob]):
            if (isinstance(candidate, dict)
                    and "Event" in str(candidate.get("@type", ""))
                    and candidate.get("startDate")):
                return candidate
    return None


def _next_data(body: bytes) -> dict | None:
    """Extract Next.js server state from a page."""
    tree = HTMLParser(body.decode("utf-8", "replace"))
    node = tree.css_first("script#__NEXT_DATA__")
    if node is None:
        return None
    try:
        return json.loads(node.text())
    except (json.JSONDecodeError, ValueError):
        return None


def _split_iso(stamp: str) -> tuple[str | None, str | None]:
    """
    ISO stamp -> ('YYYY-MM-DD', 'HH:MM') in Paris wall-clock time.

    Sources differ: Eventbrite sends local offsets (+02:00) while Shotgun sends
    UTC ('...21:30:00.000Z' is 23:30 in Paris). Anything tz-aware is converted;
    naive stamps are assumed to already be local.
    """
    try:
        parsed = datetime.fromisoformat(stamp)
    except ValueError:
        return None, None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(PARIS_TZ)
    return parsed.date().isoformat(), parsed.strftime("%H:%M")


# --------------------------------------------------------------------------
# Registry - add a new site by writing a Source subclass and listing it here
# --------------------------------------------------------------------------

class Shotgun(Source):
    """
    Tracks shotgun.live "venue" (organizer) profiles, listed in VENUES.

    robots.txt is fully permissive (`Allow: /`) and no bot challenge is served
    to a normal browser UA, but the pages are large React payloads, so this
    source leans hard on TTL caching. There are no `ETag`s and the pages are
    sent `no-store`, so conditional GET is unavailable - `max_age` is the only
    thing standing between a cron job and a lot of pointless traffic.

    The venue page is a Next.js App Router stream: the event list lives in
    escaped RSC chunks (`self.__next_f.push`), not in JSON-LD, whose blocks
    describe only the venue itself. Rather than scrape values out of a
    serialised React tree, this source takes only the (id, slug) pairs from the
    stream - the most stable thing in it - and reads every real field from each
    event page's schema.org `MusicEvent` JSON-LD.

    Two traps worth knowing about, both handled in _price():
      * A EUR 0 "free entry for girls before midnight" offer can be present but
        SoldOut. Only InStock offers count, or the whole night looks free.
      * Bottle-service packs (EUR 525 magnums) sit in the same offer list, so a
        min-max range is meaningless. Only the cheapest available ticket is
        reported.

    Note the profile is a *venue* page: its events are not necessarily organised
    by that name (the Yoyo reggaeton night is organised by "Soiree a Paris"), so
    events are NOT filtered by organizer - only by city.
    """

    name = "shotgun"
    BASE = "https://shotgun.live"

    # Venue/organizer profile slugs to track: slug -> label (label for humans).
    VENUES = {
        "paris-erasmus-life": "Paris Erasmus Life",
    }

    VENUE_TTL = timedelta(hours=6)
    DETAIL_TTL = timedelta(hours=12)

    def sync(self, city: str) -> Iterator[Event]:
        for slug, label in self.VENUES.items():
            log.info("[%s] venue %s (%s)", self.name, slug, label)
            yield from self._venue(slug, city)

    def _venue(self, venue_slug: str, city: str) -> Iterator[Event]:
        body = self.fetcher.get(f"{self.BASE}/en/venues/{venue_slug}",
                                max_age=self.VENUE_TTL)
        if body is None:
            return
        refs = self._event_refs(body)
        if not refs:
            log.error("[%s] no events found on venue %s - page layout changed",
                      self.name, venue_slug)
            return
        log.info("[%s] %d event(s) listed", self.name, len(refs))
        for event_id, slug in refs:
            ev = self._event(event_id, slug, city)
            if ev is not None:
                yield ev

    @staticmethod
    def _event_refs(body: bytes) -> list[tuple[str, str]]:
        """(event_id, slug) pairs from the RSC stream, newest-first as listed."""
        html = body.decode("utf-8", "replace")
        chunks: list[str] = []
        for m in re.finditer(r'self\.__next_f\.push\(\[1,\s*(".*?")\]\)', html, re.S):
            try:
                chunks.append(json.loads(m.group(1)))
            except (json.JSONDecodeError, ValueError):
                continue
        payload = "".join(chunks)

        found: dict[str, str] = {}      # slug -> id, preserving order
        for event_id, slug in re.findall(
                r'shotgun_event:(\d+)",\{"href":"/events/([a-z0-9\-]+)"', payload):
            found.setdefault(slug, event_id)
        if found:
            return [(eid, slug) for slug, eid in found.items()]

        # Fallback: plain hrefs, with the slug doubling as the id.
        log.warning("[%s] could not pair event ids; falling back to slugs", Shotgun.name)
        return [(s, s) for s in dict.fromkeys(re.findall(r'/events/([a-z0-9\-]+)', html))]

    def _event(self, event_id: str, slug: str, city: str) -> Event | None:
        url = f"{self.BASE}/en/events/{slug}"
        body = self.fetcher.get(url, max_age=self.DETAIL_TTL)
        if body is None:
            return None
        ld = find_event_ld(body)
        if ld is None:
            log.warning("[%s] no Event JSON-LD on %s", self.name, url)
            return None

        if str(ld.get("eventStatus", "")).endswith("EventCancelled"):
            log.debug("[%s] skipping cancelled: %s", self.name, slug)
            return None

        place = ld.get("location") or {}
        address = place.get("address") or {}
        ev_city = address.get("addressLocality")
        if city and (ev_city or "").lower() != city.lower():
            log.debug("[%s] skipping %s (city=%s)", self.name, slug, ev_city)
            return None

        start_date, start_time = _split_iso(ld.get("startDate") or "")
        end_date, end_time = _split_iso(ld.get("endDate") or "")

        venue = place.get("name")
        street = address.get("streetAddress")
        if venue and street and street not in venue:
            venue = f"{venue}, {street}"

        price_text, price_eur, is_free, sold_out = self._price(ld)

        return Event(
            source=self.name,
            source_id=str(event_id),
            url=ld.get("url") or url,
            title=clean(html_mod.unescape(ld.get("name") or "")) or slug,
            city=(ev_city or city).title(),
            venue=clean(venue),
            start_date=start_date,
            start_time=start_time,
            end_date=end_date,
            end_time=end_time,
            price_text=price_text,
            price_eur=price_eur,
            is_free=is_free,
            categories=["sold-out"] if sold_out else [],
            description=_truncate(clean(ld.get("description")), 1200),
            image=ld.get("image") if isinstance(ld.get("image"), str) else None,
            remote_modified=None,      # none exposed; DETAIL_TTL covers refresh
        )

    @staticmethod
    def _price(ld: dict) -> tuple[str | None, float | None, bool | None, bool]:
        """
        (price_text, price_eur, is_free, sold_out) from the offer list.

        Only InStock offers are considered: a EUR 0 offer that is SoldOut must
        not make the event look free. Because the same list mixes entry tickets
        with bottle packs, only the cheapest available ticket is reported.
        """
        offers = ld.get("offers") or []
        if isinstance(offers, dict):
            offers = [offers]

        available: list[float] = []
        any_offer = False
        for offer in offers:
            if not isinstance(offer, dict):
                continue
            currency = offer.get("priceCurrency")
            if currency is not None and currency != "EUR":
                continue
            try:
                price = float(offer["price"])
            except (KeyError, TypeError, ValueError):
                continue
            any_offer = True
            if str(offer.get("availability", "")).endswith("InStock"):
                available.append(price)

        if available:
            low = min(available)
            if low == 0:
                return "Free", 0.0, True, False
            return f"from {low:.2f} EUR", low, False, False
        if any_offer:
            return "Sold out", None, None, True
        return None, None, None, False


# --------------------------------------------------------------------------
# Registry - add a new site by writing a Source subclass and listing it here
# --------------------------------------------------------------------------

SOURCES: dict[str, type[Source]] = {
    ErasmusPlace.name: ErasmusPlace,
    Eventbrite.name: Eventbrite,
    Shotgun.name: Shotgun,
}


# --------------------------------------------------------------------------
# iCalendar export (RFC 5545)
# --------------------------------------------------------------------------

# Paris local time with DST rules, so clients render the right wall-clock time
# regardless of the reader's own timezone.
VTIMEZONE = """BEGIN:VTIMEZONE
TZID:Europe/Paris
BEGIN:DAYLIGHT
TZOFFSETFROM:+0100
TZOFFSETTO:+0200
TZNAME:CEST
DTSTART:19700329T020000
RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU
END:DAYLIGHT
BEGIN:STANDARD
TZOFFSETFROM:+0200
TZOFFSETTO:+0100
TZNAME:CET
DTSTART:19701025T030000
RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU
END:STANDARD
END:VTIMEZONE"""

DEFAULT_DURATION = timedelta(hours=2)   # used when a source gives no end time


def _ics_escape(text: str) -> str:
    return (text.replace("\\", "\\\\").replace(";", "\\;")
                .replace(",", "\\,").replace("\r\n", "\\n").replace("\n", "\\n"))


def _ics_fold(line: str) -> str:
    """Fold to 75 octets per RFC 5545, never splitting a UTF-8 sequence."""
    data = line.encode("utf-8")
    if len(data) <= 75:
        return line
    parts: list[str] = []
    start, limit = 0, 75
    while start < len(data):
        end = min(start + limit, len(data))
        while end < len(data) and (data[end] & 0xC0) == 0x80:   # mid-character
            end -= 1
        parts.append(data[start:end].decode("utf-8"))
        start, limit = end, 74      # continuation lines carry a leading space
    return "\r\n ".join(parts)


def _at(day: date, hhmm: str) -> datetime:
    hour, minute = (int(x) for x in hhmm.split(":")[:2])
    return datetime.combine(day, datetime.min.time()) + timedelta(hours=hour,
                                                                 minutes=minute)


def _vevent(ev: Event, stamp: str) -> list[str]:
    day = date.fromisoformat(ev.start_date)          # caller guarantees a date
    out = ["BEGIN:VEVENT",
           f"UID:{ev.source}-{ev.source_id}@events-tracker",
           f"DTSTAMP:{stamp}"]

    if ev.start_time:
        start = _at(day, ev.start_time)
        if ev.end_time:
            # Trust an explicit end_date when the source gives one (multi-day
            # events need it); otherwise assume a same-night event and roll
            # past midnight if the clock went backwards.
            end = _at(date.fromisoformat(ev.end_date) if ev.end_date else day,
                      ev.end_time)
            if end <= start:          # 23:00 -> 05:00 runs past midnight
                end += timedelta(days=1)
        else:
            end = start + DEFAULT_DURATION
        out.append(f"DTSTART;TZID=Europe/Paris:{start:%Y%m%dT%H%M%S}")
        out.append(f"DTEND;TZID=Europe/Paris:{end:%Y%m%dT%H%M%S}")
    else:                              # date known but no time -> all-day
        out.append(f"DTSTART;VALUE=DATE:{day:%Y%m%d}")
        out.append(f"DTEND;VALUE=DATE:{day + timedelta(days=1):%Y%m%d}")

    price = "Free" if ev.is_free else (ev.price_text or None)
    # Many listings share a title ("Kiss My Erasmus"), so the price goes in the
    # summary - it is the thing you are actually filtering on.
    summary = f"{ev.title} - {price}" if price else ev.title
    out.append(f"SUMMARY:{_ics_escape(summary)}")

    if ev.venue:
        out.append(f"LOCATION:{_ics_escape(ev.venue)}")
    out.append(f"URL:{_ics_escape(ev.url)}")
    if ev.categories:
        out.append("CATEGORIES:" + ",".join(_ics_escape(c) for c in ev.categories))

    body = "\n".join(filter(None, [
        f"Price: {price}" if price else None,
        f"Venue: {ev.venue}" if ev.venue else None,
        ev.description,
        f"\n{ev.url}",
    ]))
    if body:
        out.append(f"DESCRIPTION:{_ics_escape(body)}")
    out.append("END:VEVENT")
    return out


def build_ics(events: Iterable[Event]) -> tuple[str, int, int]:
    """Return (ics_text, written, skipped_for_missing_date)."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lines = ["BEGIN:VCALENDAR",
             "VERSION:2.0",
             "PRODID:-//events-tracker//Paris student events//EN",
             "CALSCALE:GREGORIAN",
             "METHOD:PUBLISH",
             "X-WR-CALNAME:Paris student & youth events",
             "X-WR-TIMEZONE:Europe/Paris",
             *VTIMEZONE.split("\n")]

    written = skipped = 0
    for ev in events:
        if not ev.start_date:          # undated entries cannot be calendared
            skipped += 1
            continue
        lines.extend(_vevent(ev, stamp))
        written += 1

    lines.append("END:VCALENDAR")
    return "\r\n".join(_ics_fold(ln) for ln in lines) + "\r\n", written, skipped


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def render(events: Iterable[Event]) -> str:
    rows = list(events)
    if not rows:
        return "No matching events."
    lines: list[str] = []
    current: object = object()
    for ev in rows:
        if ev.start_date != current:
            current = ev.start_date
            label = (date.fromisoformat(ev.start_date).strftime("%a %d %b %Y")
                     if ev.start_date else "Date unknown")
            lines.append(f"\n\x1b[1m{label}\x1b[0m")
        price = "FREE" if ev.is_free else (ev.price_text or "-")
        lines.append(f"  {ev.start_time or '--:--'}  {price:<18} {ev.title}")
        lines.append(f"         {ev.venue or '?'} - {ev.source} - {ev.url}")
    lines.append(f"\n{len(rows)} event(s).")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description="Track cheap student & youth events.")
    p.add_argument("--city", default="paris")
    p.add_argument("--source", action="append", choices=sorted(SOURCES),
                   help="limit to these sources (repeatable); default: all")
    p.add_argument("--exclude", action="append", choices=sorted(SOURCES), default=[],
                   help="skip these sources (repeatable) - useful for a host "
                        "that refuses the environment you run in")
    p.add_argument("--no-sync", action="store_true",
                   help="read the local DB only, make no requests")
    p.add_argument("--force", action="store_true",
                   help="ignore caches and refetch everything")
    p.add_argument("--free-only", action="store_true")
    p.add_argument("--all-dates", action="store_true", help="include past events")
    p.add_argument("--delay", type=float, default=4.0,
                   help="minimum seconds between requests (default: 4)")
    p.add_argument("--json", metavar="PATH", help="also write results to a JSON file")
    p.add_argument("--ics", metavar="PATH", nargs="?", const="data/paris-events.ics",
                   help="write an iCalendar file (default: data/paris-events.ics)")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)-7s %(message)s",
        stream=sys.stderr,
    )
    for noisy in ("httpx", "httpcore"):      # keep -v about our own logic
        logging.getLogger(noisy).setLevel(logging.WARNING)
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    store = Store(DB_PATH)

    wanted = [n for n in (args.source or sorted(SOURCES)) if n not in args.exclude]
    if args.exclude:
        log.info("excluding source(s): %s", ", ".join(args.exclude))
    failed: list[str] = []

    if not args.no_sync:
        store.prune_cache(CACHE_RETENTION)
        fetcher = Fetcher(store, delay=args.delay, force=args.force)
        try:
            for name in wanted:
                count = 0
                # One broken source must not cost us the others, nor the
                # outputs: unattended runs still publish what they did get.
                try:
                    for ev in SOURCES[name](fetcher, store).sync(args.city):
                        store.upsert(ev)
                        count += 1
                except Blocked as exc:
                    failed.append(name)
                    log.error("[%s] %s", name, exc)
                except Exception:
                    failed.append(name)
                    log.exception("[%s] source failed", name)
                log.info("[%s] synced %d event(s)", name, count)
        finally:
            fetcher.close()
            log.info("HTTP: %d fetched, %d unchanged (304), %d served from "
                     "fresh cache, %d skipped",
                     fetcher.stats["fetched"], fetcher.stats["not_modified"],
                     fetcher.stats["fresh"], fetcher.stats["skipped"])

    events = store.query(city=args.city, free_only=args.free_only,
                         upcoming=not args.all_dates)
    if args.source:
        events = [e for e in events if e.source in set(args.source)]
    # Excluded sources are not scraped, but events already in the DB from a
    # previous run are still perfectly good calendar entries, so they stay.

    print(render(events))

    if args.json:
        out = Path(args.json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps([dataclasses.asdict(e) for e in events],
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        log.info("wrote %s (%d events)", out, len(events))

    if args.ics:
        out = Path(args.ics)
        out.parent.mkdir(parents=True, exist_ok=True)
        text, written, skipped = build_ics(events)
        # \r\n line endings are mandated by RFC 5545, so write bytes verbatim.
        out.write_bytes(text.encode("utf-8"))
        log.info("wrote %s (%d events%s)", out, written,
                 f", {skipped} skipped for missing date" if skipped else "")

    if failed:
        # Outputs are already written, so a partial failure still publishes.
        # Only a total wipe-out is worth failing the run over.
        if len(failed) == len(wanted):
            log.error("every source failed (%s)", ", ".join(failed))
            return 1
        log.warning("continuing without: %s", ", ".join(failed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
