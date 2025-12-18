"""
scrape_cogeco.py

Cogeco press releases scraper (>= Jan 1, 2025) with paging:
  https://corpo.cogeco.com/cca/en/press-room/press-releases/?ccm_paging_p=1&ccm_order_by=cv.cvDatePublic&ccm_order_by_direction=desc

Observed DOM (from your screenshot):
  <div class="card-horizontal__body">
    <ul class="card-horizontal__meta">
      <li>Cogeco Communications inc.</li>
      <li>December 16, 2025</li>
    </ul>
    <h3 class="card-horizontal__title">
      <a href="https://corpo.cogeco.com/...">Title</a>
    </h3>

Approach:
  - requests+BS4 per page
  - stop when the oldest date on a page is < cutoff
  - dedupe by existing links (from master)

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


BASE_DOMAIN = "https://corpo.cogeco.com"
BASE_LIST_URL = (
    "https://corpo.cogeco.com/cca/en/press-room/press-releases/"
    "?ccm_paging_p={page}&ccm_order_by=cv.cvDatePublic&ccm_order_by_direction=desc"
)
COMPANY_NAME = "Cogeco"
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


def _is_press_release_url(href: str) -> bool:
    if not href:
        return False
    full = _norm_url(urljoin(BASE_DOMAIN, href))
    if "corpo.cogeco.com" not in full:
        return False
    # Typical pattern includes /press-room/press-releases/
    if "/press-room/press-releases/" not in full:
        return False
    # Exclude the listing itself (with query)
    if "/press-room/press-releases/?" in full:
        return False
    return True


def parse_cogeco_page(html: str, debug: bool = False) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[PRItem] = []

    # Each card
    for card in soup.select("div.card-horizontal__body"):
        a = card.select_one("h3.card-horizontal__title a[href]") or card.select_one("a[href*='/press-room/press-releases/']")
        if not a:
            continue

        link = _norm_url(urljoin(BASE_DOMAIN, a.get("href", "").strip()))
        if not _is_press_release_url(link):
            continue

        title = _safe_text(a)
        if not title:
            continue

        # Date appears as the 2nd <li> in .card-horizontal__meta
        d = None
        meta_lis = card.select("ul.card-horizontal__meta > li")
        if meta_lis and len(meta_lis) >= 2:
            d = _parse_date_any(_safe_text(meta_lis[1]))
        else:
            # fallback: find any li containing a month name
            for li in card.select("ul.card-horizontal__meta > li"):
                dd = _parse_date_any(_safe_text(li))
                if dd:
                    d = dd
                    break

        if not d:
            if debug:
                print(f"[COGECO][SKIP] No date parsed for {link}")
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


def scrape_cogeco(
    since: date = date(2025, 1, 1),
    existing_links: Optional[Set[str]] = None,
    debug: bool = True,
    max_pages: int = 200,
) -> List[Dict[str, str]]:
    existing_links = set(existing_links or set())

    def log(msg: str) -> None:
        if debug:
            print(f"[COGECO] {msg}")

    log(f"Starting Cogeco scraper for date >= {since.isoformat()}")
    out: List[Dict[str, str]] = []

    added = skipped_dup = skipped_old = 0

    for page in range(1, max_pages + 1):
        url = BASE_LIST_URL.format(page=page)
        log(f"--- PAGE {page} --- {url}")

        html = _http_get(url)
        rows = parse_cogeco_page(html, debug=False)
        log(f"Found {len(rows)} items on page.")

        if not rows:
            log("No items found on page; stopping.")
            break

        # Track oldest date on this page to support early stop
        page_dates: List[date] = []
        for r in rows:
            try:
                page_dates.append(datetime.strptime(r["date"], "%Y-%m-%d").date())
            except Exception:
                pass
        oldest_on_page = min(page_dates) if page_dates else None

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
            added += 1

        if oldest_on_page and oldest_on_page < since:
            log(f"Oldest on page {oldest_on_page} < cutoff {since}; stopping pagination.")
            break

    log(f"Done. Added={added} | Skipped dup={skipped_dup} | Skipped old<{since}={skipped_old}")
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
        print(f"[COGECO] Appended {len(rows)} rows to: {master_csv_path}")

    return len(rows)


# -----------------------------
# CLI
# -----------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--since", default="2025-01-01", help="Cutoff date YYYY-MM-DD (inclusive). Default 2025-01-01")
    p.add_argument("--master-csv", default=DEFAULT_MASTER_CSV, help=f"Master CSV path (default: {DEFAULT_MASTER_CSV})")
    p.add_argument("--max-pages", type=int, default=200, help="Safety cap on paging (default 200).")
    p.add_argument("--no-write", action="store_true", help="Do not write to master CSV, only print summary.")
    p.add_argument("--debug", action="store_true", help="Enable debug logs.")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    since = datetime.strptime(args.since, "%Y-%m-%d").date()

    master_csv = args.master_csv.strip()
    existing = load_existing_links_from_master(master_csv)
    print(f"[COGECO] Loaded {len(existing)} existing Cogeco links from master: {master_csv}")

    rows = scrape_cogeco(
        since=since,
        existing_links=existing,
        debug=True if args.debug or True else False,
        max_pages=args.max_pages,
    )
    print(f"[COGECO] Scraped {len(rows)} rows >= {since}.")

    if args.no_write:
        print("[COGECO] --no-write set, nothing was written. First 8 rows:")
        for r in rows[:8]:
            print(r)
    else:
        appended = append_rows_to_master(master_csv, rows, debug=True)
        print(f"[COGECO] Master CSV updated. Appended {appended} rows.")
