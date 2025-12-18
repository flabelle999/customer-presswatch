"""
scrape_bruce.py

Bruce Telecom blog scraper (>= Jan 1, 2025).
Listing page: https://brucetelecom.com/about-us/blog/

Observed DOM (from your screenshot):
  <article class="btweb-content__items blog">
    ...
    <div class="btweb-blog__story_home">
      <a href="..."><h2>Title</h2></a>
      <div class="btweb-entry-meta blog-index">
        <span class="posted-on">
          <time class="entry-date published" datetime="2025-10-31T15:00:23-04:00">October 31, 2025</time>
        </span>
      </div>

All 2025 items are on page 1.

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


BASE_LIST_URL = "https://brucetelecom.com/about-us/blog/"
BASE_DOMAIN = "https://brucetelecom.com"
COMPANY_NAME = "Bruce Telecom"
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


def _is_bruce_blog_url(href: str) -> bool:
    if not href:
        return False
    full = _norm_url(urljoin(BASE_DOMAIN, href))
    if "brucetelecom.com" not in full:
        return False
    # Blog posts are generally on the site root with slug; keep permissive but avoid the blog index itself
    if full.rstrip("/") == BASE_LIST_URL.rstrip("/"):
        return False
    return True


def _parse_date_any(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip()

    # ISO date
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


def parse_bruce_listing(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[PRItem] = []

    # Primary: each story block
    for story in soup.select("div.btweb-blog__story_home"):
        a = story.select_one("a[href]")
        if not a:
            continue

        link = _norm_url(urljoin(BASE_DOMAIN, a.get("href", "").strip()))
        if not _is_bruce_blog_url(link):
            continue

        title_el = a.select_one("h2") or story.select_one("h2")
        title = _safe_text(title_el)
        if not title:
            continue

        # time element provides datetime attribute
        t = story.select_one("time[datetime]") or story.select_one("time.entry-date")
        d = None
        if t:
            d = _parse_date_any(t.get("datetime", "") or _safe_text(t))

        if not d:
            continue

        items.append(PRItem(company=COMPANY_NAME, title=title, link=link, date=d.isoformat()))

    # Fallback: any h2 under blog content items
    if not items:
        for a in soup.select("article.btweb-content__items.blog a[href]"):
            h2 = a.select_one("h2")
            if not h2:
                continue
            link = _norm_url(urljoin(BASE_DOMAIN, a.get("href", "").strip()))
            if not _is_bruce_blog_url(link):
                continue
            title = _safe_text(h2)
            t = a.find_next("time", attrs={"datetime": True})
            d = _parse_date_any(t.get("datetime", "")) if t else None
            if title and d:
                items.append(PRItem(company=COMPANY_NAME, title=title, link=link, date=d.isoformat()))

    # Dedup by link
    seen = set()
    rows: List[Dict[str, str]] = []
    for it in items:
        if it.link in seen:
            continue
        seen.add(it.link)
        rows.append(it.__dict__)
    return rows


def scrape_bruce(
    since: date = date(2025, 1, 1),
    existing_links: Optional[Set[str]] = None,
    debug: bool = True,
) -> List[Dict[str, str]]:
    existing_links = set(existing_links or set())

    def log(msg: str) -> None:
        if debug:
            print(f"[BRUCE] {msg}")

    log(f"Starting Bruce Telecom scraper for date >= {since.isoformat()}")
    html = _http_get(BASE_LIST_URL)

    rows = parse_bruce_listing(html)
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
        print(f"[BRUCE] Appended {len(rows)} rows to: {master_csv_path}")

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
    print(f"[BRUCE] Loaded {len(existing)} existing Bruce Telecom links from master: {master_csv}")

    rows = scrape_bruce(since=since, existing_links=existing, debug=True if args.debug or True else False)
    print(f"[BRUCE] Scraped {len(rows)} rows >= {since}.")

    if args.no_write:
        print("[BRUCE] --no-write set, nothing was written. First 8 rows:")
        for r in rows[:8]:
            print(r)
    else:
        appended = append_rows_to_master(master_csv, rows, debug=True)
        print(f"[BRUCE] Master CSV updated. Appended {appended} rows.")
