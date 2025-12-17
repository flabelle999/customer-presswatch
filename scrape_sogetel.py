"""
scrape_sogetel.py

Sogetel "Salle de presse" scraper (>= Jan 1, 2025).
All items appear on one page (no load more).

Observed DOM (from DevTools screenshot):
  <ul class="c-Articles">
    <li class="c-Articles__item ...">
      <div class="-date">lundi 22 septembre 2025</div>
      <h3 class="h4 -title"> ... </h3>
      <a href="https://sogetel.com/salle-de-presse/..." class="cBth">Lire la suite</a>

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


BASE_LIST_URL = "https://sogetel.com/salle-de-presse"
BASE_DOMAIN = "https://sogetel.com"
COMPANY_NAME = "Sogetel"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

DEFAULT_MASTER_CSV = "press_releases_master.csv"


FR_MONTHS = {
    "janvier": 1,
    "février": 2,
    "fevrier": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "août": 8,
    "aout": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "décembre": 12,
    "decembre": 12,
}


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


def _parse_french_long_date(s: str) -> Optional[date]:
    """
    Parse strings like:
      "lundi 22 septembre 2025"
      "22 septembre 2025"
    """
    if not s:
        return None
    s = s.strip().lower()

    # remove weekday (anything before first digit)
    s = re.sub(r"^[^\d]+", "", s).strip()

    m = re.match(r"(\d{1,2})\s+([a-zàâçéèêëîïôûùüÿœ]+)\s+(\d{4})", s, flags=re.IGNORECASE)
    if not m:
        return None

    day = int(m.group(1))
    month_name = m.group(2).strip().lower()
    year = int(m.group(3))

    month = FR_MONTHS.get(month_name)
    if not month:
        return None

    try:
        return date(year, month, day)
    except ValueError:
        return None


def _parse_date_any(s: str) -> Optional[date]:
    if not s:
        return None

    # ISO date fast path
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            pass

    d = _parse_french_long_date(s)
    if d:
        return d

    if date_parser:
        try:
            return date_parser.parse(s, dayfirst=True).date()
        except Exception:
            return None

    return None


def _is_pr_url(href: str) -> bool:
    if not href:
        return False
    href = _norm_url(urljoin(BASE_DOMAIN, href))
    if "sogetel.com" not in href:
        return False
    if "/salle-de-presse/" not in href:
        return False
    if href.rstrip("/") == BASE_LIST_URL.rstrip("/"):
        return False
    return True


def parse_sogetel_listing(html: str, debug: bool = True) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[PRItem] = []

    # Be forgiving: if site changes, fall back to finding any li with an /salle-de-presse/ link
    lis = soup.select("ul.c-Articles > li.c-Articles__item")
    if not lis:
        lis = soup.select("li.c-Articles__item")
    if not lis:
        lis = soup.select("li")

    for li in lis:
        link_el = li.select_one("a.cBth[href]") or li.select_one("a[href*='/salle-de-presse/']")
        if not link_el:
            continue

        href = link_el.get("href", "").strip()
        link = _norm_url(urljoin(BASE_DOMAIN, href))
        if not _is_pr_url(link):
            continue

        # FIX: your DOM shows class="-date" (single dash), not "--date"
        date_el = li.select_one("div.-date") or li.select_one("div[class='-date']") or li.select_one(".-date")
        datestr = _safe_text(date_el)

        # Title: your DOM shows h3 class "h4 -title"
        title_el = li.select_one("h3.-title") or li.select_one("h3.h4.-title") or li.select_one("h3") or li.find(["h2", "h3"])
        title = _safe_text(title_el)

        if not title:
            # Sometimes title is inside a span
            title_span = li.select_one("h3 span") or li.select_one("h2 span")
            title = _safe_text(title_span)

        d = _parse_date_any(datestr)
        if not d:
            if debug:
                print(f"[SOGETEL][SKIP] Date not parsed: '{datestr}' for {link}")
            continue

        out.append(PRItem(company=COMPANY_NAME, title=title, link=link, date=d.isoformat()))

    # Dedup by link
    seen = set()
    rows: List[Dict[str, str]] = []
    for item in out:
        if item.link in seen:
            continue
        seen.add(item.link)
        rows.append(item.__dict__)
    return rows


def scrape_sogetel(
    since: date = date(2025, 1, 1),
    existing_links: Optional[Set[str]] = None,
    debug: bool = True,
) -> List[Dict[str, str]]:
    existing_links = set(existing_links or set())

    def log(msg: str) -> None:
        if debug:
            print(f"[SOGETEL] {msg}")

    log(f"Starting Sogetel scraper for date >= {since.isoformat()}")
    html = _http_get(BASE_LIST_URL)

    rows = parse_sogetel_listing(html, debug=debug)
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
        print(f"[SOGETEL] Appended {len(rows)} rows to: {master_csv_path}")

    return len(rows)


# -----------------------------
# CLI
# -----------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--since", default="2025-01-01", help="Cutoff date YYYY-MM-DD (inclusive). Default 2025-01-01")
    # DEFAULT changed as requested
    p.add_argument("--master-csv", default=DEFAULT_MASTER_CSV, help=f"Master CSV path (default: {DEFAULT_MASTER_CSV})")
    p.add_argument("--no-write", action="store_true", help="Do not write to master CSV, only print summary.")
    p.add_argument("--debug", action="store_true", help="Enable debug logs.")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    since = datetime.strptime(args.since, "%Y-%m-%d").date()

    master_csv = args.master_csv.strip()
    existing = load_existing_links_from_master(master_csv)
    print(f"[SOGETEL] Loaded {len(existing)} existing Sogetel links from master: {master_csv}")

    rows = scrape_sogetel(since=since, existing_links=existing, debug=True if args.debug or True else False)
    print(f"[SOGETEL] Scraped {len(rows)} rows >= {since}.")

    if args.no_write:
        print("[SOGETEL] --no-write set, nothing was written. First 8 rows:")
        for r in rows[:8]:
            print(r)
    else:
        appended = append_rows_to_master(master_csv, rows, debug=True)
        print(f"[SOGETEL] Master CSV updated. Appended {appended} rows.")
