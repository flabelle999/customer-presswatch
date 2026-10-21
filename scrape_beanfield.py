#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Beanfield Newsroom scraper (WordPress/Elementor).

NOTE (Oct 2026): blog.beanfield.com (the newsroom) was decommissioned: the whole
host now 302-redirects to www.beanfield.com and every URL is a 404. Beanfield has
no other public newsroom and does not publish on PR Newswire Canada / GlobeNewswire /
Business Wire. The legacy blog scrape is kept (it is tried first, in case the blog
comes back); when it is unavailable the scraper falls back to Cartt.ca, the Canadian
telecom trade press, via its public WordPress API (RSS as backup), keeping only
articles with "Beanfield" in the title and skipping events already in the CSV.

- Legacy blog: scrapes list pages:
  https://blog.beanfield.com/category/newsroom/
  https://blog.beanfield.com/category/newsroom/page/2/
  ...

- For each post, fetches the post page and extracts:
  - title
  - published date (robust: meta -> JSON-LD -> <time> -> visible regex)
  - URL

Appends new rows (>= cutoff year) into customer_press_releases_master.csv.

Python: 3.9+ compatible.

-----------------------------------------
DEBUG INSTRUCTIONS
-----------------------------------------
To enable date-debug logs, set:
    DEBUG_DATE = True

You will then see logs like:
    [BEANFIELD][DATE] <url> meta[...] = ...
    [BEANFIELD][DATE] <url> jsonld datePublished = ...
    [BEANFIELD][DATE] <url> <time datetime> = ...
    [BEANFIELD][DATE] <url> visible match = ...
    [BEANFIELD][DATE] <url> FAIL: no parsable date found
"""

import csv
import json
import re
from datetime import datetime, timezone
from typing import List, Optional, Set, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import os

# ----------------------------
# Config
# ----------------------------
COMPANY = "Beanfield"
BASE_DOMAIN = "https://blog.beanfield.com/"
LIST_URL_PAGE1 = "https://blog.beanfield.com/category/newsroom/"
LIST_URL_PAGED = "https://blog.beanfield.com/category/newsroom/page/{page}/"

CUTOFF_YEAR = 2025

MASTER_CSV = "press_releases_master.csv"  # in current working directory
CSV_FIELDS = ["id","company", "title", "link", "date"]

# Safety: don't loop forever if site changes
MAX_LIST_PAGES = 30

# Optional verbose date debug (set True when diagnosing)
DEBUG_DATE = False
DEBUG_SAVE_FAIL_HTML = False

# ----------------------------
# Helpers
# ----------------------------
def log(msg: str) -> None:
    print(f"[BEANFIELD] {msg}")


def dbg_date(msg: str) -> None:
    """Verbose date debugging (high signal)."""
    if DEBUG_DATE:
        print(f"[BEANFIELD][DATE] {msg}")


def _try_parse_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()

    # Common: 2025-03-24T12:34:56Z
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    # datetime.fromisoformat handles both date and datetime (with optional offset)
    try:
        dt = datetime.fromisoformat(s)
        # normalize timezone-aware to UTC, then drop tzinfo (store as naive date)
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return None

def slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    return path.split("/")[-1] if path else ""

def fetch_wp_date(session: requests.Session, url: str) -> Optional[datetime]:
    slug = slug_from_url(url)
    if not slug:
        return None

    api = f"https://blog.beanfield.com/wp-json/wp/v2/posts?slug={slug}&per_page=1&_fields=date,modified,link"
    try:
        r = session.get(api, timeout=30)
        r.raise_for_status()
        arr = r.json()
        if not arr:
            dbg_date(f"{url} wp-json empty for slug={slug}")
            return None

        raw = (arr[0].get("date") or "").strip()
        dbg_date(f"{url} wp-json date = {raw}")
        dt = _try_parse_iso(raw)
        if dt:
            dbg_date(f"{url} wp-json parsed -> {dt}")
            return dt
    except Exception as e:
        dbg_date(f"{url} wp-json error: {e}")
    return None

# --- Dates "humaines" robustes (avec/sans virgule, mois abrégés, point optionnel) ---
MONTHS_FULL = r"(January|February|March|April|May|June|July|August|September|October|November|December)"
MONTHS_ABBR = r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?"
MONTHS_RE = rf"({MONTHS_FULL}|{MONTHS_ABBR})"

# Ex: "March 24, 2025" OR "March 24 2025" OR "Aug. 7, 2024" OR "Aug 7 2024"
HUMAN_DATE_RE = re.compile(rf"\b{MONTHS_RE}\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,)?\s+\d{{4}}\b")


def _normalize_human_date(s: str) -> str:
    """Remove ordinal suffixes and month-abbrev dots so strptime can parse."""
    s = re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1", s)  # 7th -> 7
    s = re.sub(r"\b(Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.\b", r"\1", s)  # Aug. -> Aug
    return s


def extract_post_links(list_html: str) -> List[str]:
    """
    Extract post links from a Beanfield newsroom list page.
    Filters out category/tag/navigation links.
    """
    soup = BeautifulSoup(list_html, "html.parser")
    out: List[str] = []

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue

        url = urljoin(BASE_DOMAIN, href)

        # ✅ Exclude obvious non-post URLs
        if "/category/" in url:
            continue
        if "/tag/" in url:
            continue
        if "/author/" in url:
            continue

        # Avoid the newsroom listing itself or pagination links being treated as posts
        if url.rstrip("/") == LIST_URL_PAGE1.rstrip("/"):
            continue
        if "/category/newsroom/page/" in url:
            continue

        # ✅ Keep only Beanfield domain
        if not url.startswith(BASE_DOMAIN):
            continue

        # Skip WP internals
        if "/wp-content/" in url or "/wp-admin/" in url or "/wp-json/" in url:
            continue

        out.append(url)

    # Dedupe while preserving order
    seen: Set[str] = set()
    dedup: List[str] = []
    for u in out:
        u = u.rstrip("/")
        if u not in seen:
            seen.add(u)
            dedup.append(u)

    return dedup


def parse_post_title(post_html: str) -> str:
    soup = BeautifulSoup(post_html, "html.parser")

    h1 = soup.select_one("h1")
    if h1:
        title = h1.get_text(" ", strip=True)
        if title:
            return title

    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content"):
        return og.get("content").strip()

    t = soup.select_one("title")
    if t:
        return t.get_text(strip=True)

    return ""


def parse_post_date(post_html: str, url: str = "") -> Optional[datetime]:
    """
    Robust date extraction:
      1) Meta tags (article:published_time, date, etc.)
      2) JSON-LD (datePublished)
      3) <time datetime="...">
      4) Visible text regex (e.g., "TORONTO – March 24, 2025")
    """
    soup = BeautifulSoup(post_html, "html.parser")

    # 1) Meta tags
    meta_selectors: List[Tuple[str, str]] = [
        ('meta[property="article:published_time"]', "content"),
        ('meta[name="article:published_time"]', "content"),
        ('meta[property="og:published_time"]', "content"),
        ('meta[name="pubdate"]', "content"),
        ('meta[name="publish_date"]', "content"),
        ('meta[name="date"]', "content"),
    ]
    for sel, attr in meta_selectors:
        tag = soup.select_one(sel)
        if tag and tag.get(attr):
            raw = str(tag.get(attr)).strip()
            dbg_date(f"{url} meta[{sel}] = {raw}")
            dt = _try_parse_iso(raw)
            if dt:
                dbg_date(f"{url} meta parsed -> {dt}")
                return dt
            dbg_date(f"{url} meta parse failed")

    # 2) JSON-LD datePublished
    for script in soup.select('script[type="application/ld+json"]'):
        txt = script.get_text(strip=True)
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue

        def _walk(obj) -> Optional[datetime]:
            if isinstance(obj, dict):
                if "datePublished" in obj:
                    raw = str(obj.get("datePublished") or "").strip()
                    dbg_date(f"{url} jsonld datePublished = {raw}")
                    dt2 = _try_parse_iso(raw)
                    if dt2:
                        dbg_date(f"{url} jsonld parsed -> {dt2}")
                        return dt2
                    dbg_date(f"{url} jsonld parse failed")
                for v in obj.values():
                    got = _walk(v)
                    if got:
                        return got
            elif isinstance(obj, list):
                for it in obj:
                    got = _walk(it)
                    if got:
                        return got
            return None

        dt = _walk(data)
        if dt:
            return dt

    # 3) <time datetime="...">
    t = soup.select_one("time[datetime]")
    if t and t.get("datetime"):
        raw = str(t.get("datetime")).strip()
        dbg_date(f"{url} <time datetime> = {raw}")
        dt = _try_parse_iso(raw)
        if dt:
            dbg_date(f"{url} <time> parsed -> {dt}")
            return dt
        dbg_date(f"{url} <time> parse failed")

    # 4) Visible text (robuste)
    text = soup.get_text(" ", strip=True)
    m = HUMAN_DATE_RE.search(text)
    if m:
        raw = m.group(0)
        norm = _normalize_human_date(raw)
        dbg_date(f"{url} visible match = '{raw}' (norm='{norm}')")

        # Detect abbreviated month vs full month
        is_abbr = bool(re.match(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\b", norm))
        has_comma = "," in norm

        if is_abbr:
            fmts = ["%b %d, %Y", "%b %d %Y"] if has_comma else ["%b %d %Y", "%b %d, %Y"]
        else:
            fmts = ["%B %d, %Y", "%B %d %Y"] if has_comma else ["%B %d %Y", "%B %d, %Y"]

        for fmt in fmts:
            try:
                dt = datetime.strptime(norm, fmt)
                dbg_date(f"{url} visible parsed -> {dt} via fmt='{fmt}'")
                return dt
            except ValueError:
                continue

        dbg_date(f"{url} visible parse failed for all fmts tried: {fmts}")

    # 5) WordPress REST API fallback (most reliable when Elementor hides dates)
    # NOTE: needs session; easiest is to do this fallback in main loop (see next step)

    dbg_date(f"{url} FAIL: no parsable date found")
    return None


def load_existing_links(csv_path: str) -> Set[str]:
    existing: Set[str] = set()
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if (row.get("company") or "").strip() != COMPANY:
                    continue
                link = (row.get("link") or row.get("url") or "").strip().rstrip("/")
                if link:
                    existing.add(link)
    except FileNotFoundError:
        return set()
    return existing


def ensure_csv_header(csv_path: str) -> None:
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            if f.read(1):
                return
    except FileNotFoundError:
        pass

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()


def append_rows(csv_path: str, rows: List[Tuple[datetime, str, str]]) -> int:
    """
    rows: list of (dt, title, link)
    """
    if not rows:
        return 0

    ensure_csv_header(csv_path)

    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        for dt, title, link in rows:
            writer.writerow(
                {
                    "id": COMPANY,
                    "company": COMPANY,
                    "title": title,
                    "link": link,
                    "date": dt.strftime("%Y-%m-%d"),
                }
            )
    return len(rows)


# ----------------------------
# Main
# ----------------------------
def scrape_blog(
    session: requests.Session, existing_links: Set[str]
) -> Optional[List[Tuple[datetime, str, str]]]:
    """
    Legacy source: the WordPress newsroom at blog.beanfield.com.

    Returns the list of new rows, or None when the blog is not available at all
    (as of Oct 2026 the whole blog.beanfield.com host 302-redirects to
    www.beanfield.com and every post is a 404), so the caller can use the fallback.
    """
    new_rows: List[Tuple[datetime, str, str]] = []

    for page in range(1, MAX_LIST_PAGES + 1):
        list_url = LIST_URL_PAGE1 if page == 1 else LIST_URL_PAGED.format(page=page)
        log(f"--- LIST PAGE {page} --- {list_url}")

        try:
            resp = session.get(list_url, timeout=30)
            log(f"HTTP {resp.status_code} final_url={resp.url}")
            if page == 1 and (
                resp.status_code != 200 or not resp.url.startswith(BASE_DOMAIN)
            ):
                log("Legacy blog is unavailable (error status or redirected off blog host).")
                return None
            if resp.status_code == 404:
                log("Stop (404 - no more pages).")
                break
            resp.raise_for_status()
        except Exception as e:
            log(f"Stop (HTTP error): {e}")
            if page == 1:
                return None
            break

        post_links = extract_post_links(resp.text)
        log(f"Found {len(post_links)} candidate post links")
        if page == 1 and not post_links:
            log("Legacy blog list page has no post links; treating as unavailable.")
            return None

        # Track parsed dates on THIS list page (only successful parses)
        parsed_dates_this_page: List[datetime] = []

        for url in post_links:
            url_norm = url.rstrip("/")
            if url_norm in existing_links:
                continue

            try:
                r2 = session.get(url, timeout=30)
                r2.raise_for_status()
            except Exception as e:
                log(f"⚠️ Could not fetch {url}: {e}")
                continue

            title = parse_post_title(r2.text).strip()
            dt = parse_post_date(r2.text, url=url)

            if not dt:
                if DEBUG_SAVE_FAIL_HTML:
                    slug = slug_from_url(url) or "unknown"
                    fn = f"beanfield_fail_{slug}.html"
                    with open(fn, "w", encoding="utf-8") as f:
                        f.write(r2.text)
                    dbg_date(f"{url} saved failing HTML -> {os.path.abspath(fn)}")

                # Try WordPress REST API as fallback
                dt = fetch_wp_date(session, url)

            if not dt:
                log(f"⚠️ Could not parse date for {url}")
                continue

            parsed_dates_this_page.append(dt)

            if dt.year < CUTOFF_YEAR:
                continue

            if not title:
                title = url.rstrip("/").split("/")[-1].replace("-", " ").strip()

            log(f"PR: {dt.strftime('%Y-%m-%d')} | {title}")

            existing_links.add(url_norm)
            new_rows.append((dt, title, url_norm))

        # Stop condition:
        # Only stop if we successfully parsed at least one date on this page
        # AND all parsed dates are older than cutoff.
        if parsed_dates_this_page:
            if all(d.year < CUTOFF_YEAR for d in parsed_dates_this_page):
                log(f"Reached content older than {CUTOFF_YEAR} (parsed dates older) → stop")
                break

    return new_rows


# ----------------------------
# Fallback source: Cartt.ca (Canadian telecom trade press)
# ----------------------------
# Beanfield has no public newsroom anymore and does not use PR Newswire Canada,
# GlobeNewswire or Business Wire (searched Oct 2026). Cartt.ca covers every
# Beanfield announcement and exposes an open WordPress API / RSS feed.
CARTT_API = (
    "https://cartt.ca/wp-json/wp/v2/posts"
    "?search=Beanfield&per_page=50&orderby=date&order=desc"
    "&after={after}T00:00:00&_fields=date,link,title"
)
CARTT_RSS = "https://cartt.ca/search/Beanfield/feed/rss2/"

_STOPWORDS = {
    "beanfield", "the", "a", "an", "and", "or", "of", "to", "for", "in", "on", "as",
    "with", "by", "its", "new", "at", "from", "is", "are", "says", "technologies",
}


def _title_tokens(title: str) -> Set[str]:
    words = re.findall(r"[a-z0-9]+", (title or "").lower())
    return {w for w in words if w not in _STOPWORDS and len(w) > 2}


def load_existing_rows(csv_path: str) -> List[Tuple[Optional[datetime], str]]:
    """(date, title) of existing Beanfield rows; used to avoid duplicating an event."""
    out: List[Tuple[Optional[datetime], str]] = []
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if (row.get("company") or "").strip() != COMPANY:
                    continue
                raw = (row.get("date") or "").strip()
                dt = None
                for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
                    try:
                        dt = datetime.strptime(raw, fmt)
                        break
                    except ValueError:
                        continue
                out.append((dt, (row.get("title") or "").strip()))
    except FileNotFoundError:
        pass
    return out


def _is_same_event(dt: datetime, title: str, existing_rows) -> bool:
    """Same story already stored from another source: dates within 7 days + >=2 shared words."""
    toks = _title_tokens(title)
    for edt, etitle in existing_rows:
        if edt is None or abs((dt - edt).days) > 7:
            continue
        if len(toks & _title_tokens(etitle)) >= 2:
            return True
    return False


def fetch_cartt_items(session: requests.Session) -> List[Tuple[datetime, str, str]]:
    """Returns (date, title, link) for Cartt articles about Beanfield dated >= cutoff."""
    import html as _html
    from email.utils import parsedate_to_datetime

    items: List[Tuple[datetime, str, str]] = []
    cutoff = datetime(CUTOFF_YEAR, 1, 1)

    # 1) WordPress REST API
    try:
        url = CARTT_API.format(after=cutoff.strftime("%Y-%m-%d"))
        r = session.get(url, timeout=30, headers={"Accept": "application/json"})
        log(f"[cartt api] HTTP {r.status_code}")
        r.raise_for_status()
        for p in r.json():
            dt = _try_parse_iso(str(p.get("date") or ""))
            title = _html.unescape(((p.get("title") or {}).get("rendered") or "")).strip()
            link = str(p.get("link") or "").strip().rstrip("/")
            if dt and title and link:
                items.append((dt, title, link))
        log(f"[cartt api] {len(items)} items")
    except Exception as e:
        log(f"[cartt api] failed: {e}")

    # 2) RSS fallback
    if not items:
        try:
            r = session.get(CARTT_RSS, timeout=30, headers={"Accept": "application/rss+xml,*/*"})
            log(f"[cartt rss] HTTP {r.status_code}")
            r.raise_for_status()
            import xml.etree.ElementTree as ET

            root = ET.fromstring(r.content)
            for it in root.iter("item"):
                title = (it.findtext("title") or "").strip()
                link = (it.findtext("link") or "").strip()
                pub = (it.findtext("pubDate") or "").strip()
                if not (title and link and pub):
                    continue
                try:
                    dt = parsedate_to_datetime(pub).replace(tzinfo=None)
                except Exception:
                    continue
                items.append((dt, _html.unescape(title), link.rstrip("/")))
            log(f"[cartt rss] {len(items)} items")
        except Exception as e:
            log(f"[cartt rss] failed: {e}")

    # Keep only articles that actually name Beanfield in the title, >= cutoff
    return [
        (dt, title, link)
        for dt, title, link in items
        if dt >= cutoff and "beanfield" in title.lower()
    ]


def scrape_fallback(
    session: requests.Session, existing_links: Set[str], existing_rows
) -> List[Tuple[datetime, str, str]]:
    log("Using fallback source: Cartt.ca (blog.beanfield.com is gone)")
    out: List[Tuple[datetime, str, str]] = []
    for dt, title, link in fetch_cartt_items(session):
        if link in existing_links:
            continue
        if _is_same_event(dt, title, existing_rows):
            log(f"Skip (same event already in CSV): {dt.strftime('%Y-%m-%d')} | {title}")
            continue
        log(f"PR: {dt.strftime('%Y-%m-%d')} | {title}")
        existing_links.add(link)
        existing_rows.append((dt, title))
        out.append((dt, title, link))
    return out


def scrape_beanfield() -> None:
    log(f"Starting Beanfield scraper for year >= {CUTOFF_YEAR}")

    existing_links = load_existing_links(MASTER_CSV)
    existing_rows = load_existing_rows(MASTER_CSV)
    log(f"Existing Beanfield links in CSV: {len(existing_links)}")

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/141.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-CA,en;q=0.9",
        }
    )

    new_rows = scrape_blog(session, existing_links)
    if new_rows is None:
        new_rows = scrape_fallback(session, existing_links, existing_rows)

    # Sort new rows by date descending
    new_rows.sort(key=lambda x: x[0], reverse=True)

    added = append_rows(MASTER_CSV, new_rows)
    if added:
        log(f"✅ Added {added} rows to {MASTER_CSV}")
    else:
        log("Nothing new.")


if __name__ == "__main__":
    scrape_beanfield()
