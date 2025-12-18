"""
scrape_eastlink.py

Eastlink News Releases scraper (>= Jan 1, 2025).
Listing page: https://www.eastlink.ca/news-release/

Observed DOM (from your screenshot):
  <div class="m-1.5 box-border grid-cols-1 grid">
    <div class="m-4 news-item ..." data-year="2025">
      <a href="/news-release/.../">
        ...
        <small data-years="2025">July 16, 2025</small>

No "load more" indicated; the page is grouped by year via data-year/data-years.

Outputs rows in PressWatch master format:
  A: id (empty)
  B: company
  C: title
  D: link
  E: date (YYYY-MM-DD)

Deps:
  pip install requests beautifulsoup4 python-dateutil
"""

import argparse
import csv
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Set, List, Dict
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from dateutil import parser as date_parser
except Exception:  # pragma: no cover
    date_parser = None


BASE_LIST_URL = "https://www.eastlink.ca/news-release/"
BASE_DOMAIN = "https://www.eastlink.ca"
COMPANY_NAME = "Eastlink"
DEFAULT_MASTER_CSV = "press_releases_master.csv"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class PRItem:
    company: str
    title: str
    link: str
    date: str  # YYYY-MM-DD


def _norm_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    p = urlparse(url)
    return p._replace(fragment="").geturl()


def _safe_text(el) -> str:
    if not el:
        return ""
    return re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()


def _http_get(url: str, timeout: int = 30) -> str:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
    r.raise_for_status()
    return r.text


def _parse_date_any(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip()

    # ISO fast-path
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            pass

    if date_parser:
        try:
            return date_parser.parse(s).date()
        except Exception:
            return None

    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    return None


def _is_news_release_url(href: str) -> bool:
    if not href:
        return False
    full = _norm_url(urljoin(BASE_DOMAIN, href))
    if "eastlink.ca" not in full:
        return False
    if "/news-release/" not in full:
        return False
    if full.rstrip("/") == BASE_LIST_URL.rstrip("/"):
        return False
    return True


def parse_eastlink_listing(html: str, debug: bool = True) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[PRItem] = []

    # Primary selector based on screenshot
    for card in soup.select("div.news-item[data-year]"):
        year_attr = (card.get("data-year") or "").strip()
        # Quick year filter (helps performance)
        if year_attr.isdigit() and int(year_attr) < 2025:
            continue

        a = card.select_one("a[href]")
        if not a:
            continue

        link = _norm_url(urljoin(BASE_DOMAIN, a.get("href", "").strip()))
        if not _is_news_release_url(link):
            continue

        # Title: Eastlink sometimes has h3/h2; be robust
        title_el = a.select_one("h3") or a.select_one("h2") or card.find(["h2", "h3", "h4"])
        title = _safe_text(title_el)
        if not title:
            # fallback: first non-empty text in the anchor
            title = _safe_text(a)

        # Date: <small data-years="2025">July 16, 2025</small>
        date_el = card.select_one("small[data-years]") or card.select_one("small") or card.select_one("time")
        datestr = _safe_text(date_el)
        d = _parse_date_any(datestr)

        if not d:
            if debug:
                print(f"[EASTLINK][SKIP] Date not parsed: '{datestr}' for {link}")
            continue

        items.append(PRItem(company=COMPANY_NAME, title=title, link=link, date=d.isoformat()))

    # Fallback: if class changes, find anchors under /news-release/ and try nearby <small>
    if not items:
        for a in soup.select('a[href*="/news-release/"]'):
            link = _norm_url(urljoin(BASE_DOMAIN, a.get("href", "").strip()))
            if not _is_news_release_url(link):
                continue
            title_el = a.select_one("h3") or a.select_one("h2") or a.find(["h2", "h3", "h4"])
            title = _safe_text(title_el) or _safe_text(a)
            # closest small
            small = a.find_next("small")
            d = _parse_date_any(_safe_text(small)) if small else None
            if title and d:
                items.append(PRItem(company=COMPANY_NAME, title=title, link=link, date=d.isoformat()))

    # Dedup
    seen = set()
    rows: List[Dict[str, str]] = []
    for it in items:
        if it.link in seen:
            continue
        seen.add(it.link)
        rows.append(it.__dict__)
    return rows


def scrape_eastlink(
    since: date = date(2025, 1, 1),
    existing_links: Optional[Set[str]] = None,
    debug: bool = True,
) -> List[Dict[str, str]]:
    existing_links = set(existing_links or set())

    def log(msg: str) -> None:
        if debug:
            print(f"[EASTLINK] {msg}")

    log(f"Starting Eastlink scraper for date >= {since.isoformat()}")
    html = _http_get(BASE_LIST_URL)

    rows = parse_eastlink_listing(html, debug=debug)
    log(f"Found {len(rows)} items on listing page (before filtering/dupes).")

    out: List[Dict[str, str]] = []
    skipped_old = skipped_dup = 0

    for r in rows:
        link = r["link"]
        d = datetime.strptime(r["date"], "%Y-%m-%d").date()

        if link in existing_links:
            skipped_dup += 1
            continue
        if d < since:
            skipped_old += 1
            continue

        existing_links.add(link)
        out.append(r)

    log(f"Done. Added={len(out)} | Skipped dup={skipped_dup} | Skipped old<{since}={skipped_old}")
    return out


# -----------------------------
# Master CSV utilities (A=id blank, B-E data)
# -----------------------------

def load_existing_links_from_master(master_csv_path: str) -> Set[str]:
    links: Set[str] = set()
    if not master_csv_path or not os.path.exists(master_csv_path):
        return links

    with open(master_csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = (row.get("company") or row.get("source") or "").strip()
            url = (row.get("link") or row.get("url") or row.get("URL") or "").strip()
            if company.lower() == COMPANY_NAME.lower() and url:
                links.add(_norm_url(url))
    return links


def append_rows_to_master(master_csv_path: str, rows: List[Dict[str, str]], debug: bool = True) -> int:
    if not rows:
        return 0

    os.makedirs(os.path.dirname(master_csv_path) or ".", exist_ok=True)

    fieldnames = ["id", "company", "title", "link", "date"]

    file_exists = os.path.exists(master_csv_path)
    with open(master_csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()

        for r in rows:
            writer.writerow({
                "id": "",
                "company": r.get("company", ""),
                "title": r.get("title", ""),
                "link": r.get("link", ""),
                "date": r.get("date", ""),
            })

    if debug:
        print(f"[EASTLINK] Appended {len(rows)} rows to: {master_csv_path}")

    return len(rows)


# -----------------------------
# CLI
# -----------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--since", default="2025-01-01", help="Cutoff date YYYY-MM-DD (inclusive). Default 2025-01-01")
    p.add_argument("--master-csv", default=DEFAULT_MASTER_CSV, help=f"Master CSV path (default: {DEFAULT_MASTER_CSV})")
    p.add_argument("--no-write", action="store_true", help="Do not write to master CSV, only print summary.")
    p.add_argument("--debug", action="store_true", help="Enable debug logs.")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    since = datetime.strptime(args.since, "%Y-%m-%d").date()

    master_csv = args.master_csv.strip()
    existing = load_existing_links_from_master(master_csv)
    print(f"[EASTLINK] Loaded {len(existing)} existing Eastlink links from master: {master_csv}")

    rows = scrape_eastlink(since=since, existing_links=existing, debug=True if args.debug or True else False)
    print(f"[EASTLINK] Scraped {len(rows)} rows >= {since}.")

    if args.no_write:
        print("[EASTLINK] --no-write set, nothing was written. First 8 rows:")
        for r in rows[:8]:
            print(r)
    else:
        appended = append_rows_to_master(master_csv, rows, debug=True)
        print(f"[EASTLINK] Master CSV updated. Appended {appended} rows.")
