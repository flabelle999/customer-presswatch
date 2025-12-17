"""
scrape_mnsi.py

MNSi press release scraper (>= Jan 1, 2025).
Listing page: https://www.mnsi.net/articles/press-release

Observed DOM (from your screenshot):
  <h4 class="media-heading">
    <a href="/articles/press-release/...">Title</a>
  </h4>
  <small class="text-muted">Monday 4th of July 2022 09:35 AM</small>

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
from typing import Optional, Set, List, Dict, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from dateutil import parser as date_parser
except Exception:  # pragma: no cover
    date_parser = None


BASE_LIST_URL = "https://www.mnsi.net/articles/press-release"
BASE_DOMAIN = "https://www.mnsi.net"
COMPANY_NAME = "MNSi"
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


# -----------------------------
# Core helpers
# -----------------------------

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


def _is_pr_url(href: str) -> bool:
    if not href:
        return False
    full = _norm_url(urljoin(BASE_DOMAIN, href))
    if "mnsi.net" not in full:
        return False
    if "/articles/press-release" not in full:
        return False
    return True


def _parse_mnsi_date(s: str) -> Optional[date]:
    """
    Parses strings like:
      "Monday 4th of July 2022 09:35 AM"
    We remove ordinal suffixes (st/nd/rd/th) and the word 'of', then parse.
    """
    if not s:
        return None
    s = s.strip()

    # Remove ordinals: 1st -> 1, 2nd -> 2, 3rd -> 3, 4th -> 4, etc.
    s = re.sub(r"(\d{1,2})(st|nd|rd|th)\b", r"\1", s, flags=re.IGNORECASE)

    # Remove " of " (common in this format)
    s = re.sub(r"\bof\b", "", s, flags=re.IGNORECASE)

    # Collapse spaces
    s = re.sub(r"\s+", " ", s).strip()

    # Try dateutil first (best)
    if date_parser:
        try:
            return date_parser.parse(s).date()
        except Exception:
            pass

    # Fallback patterns (day name optional)
    for fmt in ("%A %d %B %Y %I:%M %p", "%d %B %Y %I:%M %p"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    return None


# -----------------------------
# Listing parsing
# -----------------------------

def parse_mnsi_listing(html: str, debug: bool = True) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[PRItem] = []

    # Each entry usually appears as a "media" block; we can be liberal:
    # Find all h4.media-heading a[href]
    for h4 in soup.select("h4.media-heading"):
        a = h4.select_one("a[href]")
        if not a:
            continue

        href = a.get("href", "").strip()
        link = _norm_url(urljoin(BASE_DOMAIN, href))
        if not _is_pr_url(link):
            continue

        title = _safe_text(a)
        if not title:
            continue

        # Date is typically the next <small class="text-muted"> in the same container
        container = h4.find_parent()  # climb up a bit
        date_el = None
        # Search nearby: first inside the same parent
        if container:
            date_el = container.select_one("small.text-muted")
        if not date_el:
            # fallback: next small in document flow
            nxt = h4.find_next("small", class_="text-muted")
            date_el = nxt

        datestr = _safe_text(date_el)
        d = _parse_mnsi_date(datestr)

        if not d:
            if debug:
                print(f"[MNSI][SKIP] Date not parsed: '{datestr}' for {link}")
            continue

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


def scrape_mnsi(
    since: date = date(2025, 1, 1),
    existing_links: Optional[Set[str]] = None,
    debug: bool = True,
) -> List[Dict[str, str]]:
    existing_links = set(existing_links or set())

    def log(msg: str) -> None:
        if debug:
            print(f"[MNSI] {msg}")

    log(f"Starting MNSI scraper for date >= {since.isoformat()}")
    html = _http_get(BASE_LIST_URL)

    rows = parse_mnsi_listing(html, debug=debug)
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
        print(f"[MNSI] Appended {len(rows)} rows to: {master_csv_path}")

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
    print(f"[MNSI] Loaded {len(existing)} existing MNSI links from master: {master_csv}")

    rows = scrape_mnsi(since=since, existing_links=existing, debug=True if args.debug or True else False)
    print(f"[MNSI] Scraped {len(rows)} rows >= {since}.")

    if args.no_write:
        print("[MNSI] --no-write set, nothing was written. First 8 rows:")
        for r in rows[:8]:
            print(r)
    else:
        appended = append_rows_to_master(master_csv, rows, debug=True)
        print(f"[MNSI] Master CSV updated. Appended {appended} rows.")
