"""
scrape_xplore.py

Xplore news scraper (>= Jan 1, 2025).
Listing page: https://www.xplore.ca/about/news/

Observed DOM (from your screenshot):
- Each card contains:
    <p class="mb-none text-white">
        <span class="block mb-xs">Sep 12, 2025</span>
        "Xplore invests..."
    </p>
    <a ... href="https://www.xplore.ca/about/news/xplore-invests-200-million-to-bring-fibre-internet-to-rural-newfoundland/">
- So date + link + title are all on the listing page (simple).

Approach:
  - requests + BeautifulSoup
  - Extract cards (anchor href contains /about/news/)
  - Title: anchor text if present, otherwise use URL slug -> title
  - Date: parse from the <span class="block mb-xs">...</span>
  - Filter >= 2025-01-01
  - Append to master CSV (press_releases_master.csv by default)
    with columns: id(blank), company, title, link, date

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
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup

try:
    from dateutil import parser as date_parser
except Exception:  # pragma: no cover
    date_parser = None


BASE_LIST_URL = "https://www.xplore.ca/about/news/"
BASE_DOMAIN = "https://www.xplore.ca"
COMPANY_NAME = "Xplore"
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

def _http_get_requests(url: str, timeout: int = 30) -> str:
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-CA,en;q=0.9,fr-CA;q=0.8,fr;q=0.7",
        "Referer": "https://www.xplore.ca/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


def _http_get_selenium(url: str) -> str:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service as ChromeService
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.support.ui import WebDriverWait

    opts = Options()
    # headless est souvent bloqué; commence headful
    opts.add_argument("--window-size=1400,1000")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(f"--user-agent={UA}")

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
    try:
        driver.get(url)
        WebDriverWait(driver, 25).until(lambda d: d.execute_script("return document.readyState") == "complete")
        return driver.page_source
    finally:
        driver.quit()


def _http_get(url: str, timeout: int = 30, debug: bool = False) -> str:
    try:
        return _http_get_requests(url, timeout=timeout)
    except requests.exceptions.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        if code in (401, 403):
            if debug:
                print(f"[XPLORE][FETCH] requests blocked ({code}) -> Selenium fallback: {url}")
            return _http_get_selenium(url)
        raise

def _parse_date_any(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip()

    # Typical: "Sep 12, 2025"
    if date_parser:
        try:
            return date_parser.parse(s, fuzzy=True).date()
        except Exception:
            return None

    for fmt in ("%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _is_xplore_news_url(href: str) -> bool:
    if not href:
        return False
    full = _norm_url(urljoin(BASE_DOMAIN, href))
    if "xplore.ca" not in full:
        return False
    if "/about/news/" not in full:
        return False
    if full.rstrip("/") == BASE_LIST_URL.rstrip("/"):
        return False
    return True


def _title_from_slug(url: str) -> str:
    p = urlparse(url)
    slug = p.path.rstrip("/").split("/")[-1]
    slug = unquote(slug)
    slug = slug.replace("-", " ").strip()
    if not slug:
        return ""
    return slug[:1].upper() + slug[1:]


def parse_xplore_listing(html: str, debug: bool = True) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[PRItem] = []

    # Strategy:
    # 1) Find all links to /about/news/... (excluding the listing itself)
    # 2) For each link, find nearest preceding/ancestor block that has a date span "block mb-xs"
    for a in soup.select('a[href*="/about/news/"]'):
        href = (a.get("href") or "").strip()
        link = _norm_url(urljoin(BASE_DOMAIN, href))
        if not _is_xplore_news_url(link):
            continue

        # Title: anchor text if any; else from slug
        title = _extract_title_for_link(a)
        if not title:
            title = _title_from_slug(link)
        if not title:
            continue

        # Date: try to find within same card container
        card = a.find_parent()
        date_el = None
        # Walk up a few levels to find the date span
        for _ in range(6):
            if not card:
                break
            date_el = card.select_one("span.block.mb-xs") or card.select_one("span.block")
            if date_el and _parse_date_any(_safe_text(date_el)):
                break
            card = card.find_parent()

        # Fallback: search nearby in DOM
        if not date_el:
            candidate = a.find_previous("span", class_=lambda c: c and "mb-xs" in c.split())
            date_el = candidate

        datestr = _safe_text(date_el)
        d = _parse_date_any(datestr)

        if not d:
            if debug:
                print(f"[XPLORE][SKIP] Date not parsed near link: {link} (got '{datestr}')")
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

def _extract_title_for_link(a) -> str:
    """
    Try to get the real card title (usually in a nearby <h2>),
    fallback to anchor text, then URL slug.
    """
    # Best: closest heading above the link
    h = a.find_previous(["h1", "h2", "h3", "h4"])
    if h:
        t = _safe_text(h)
        if t and "read more" not in t.lower():
            return t

    # Alternate: a heading inside the same card container
    card = a
    for _ in range(6):
        if not card:
            break
        hh = card.select_one("h1, h2, h3, h4")
        if hh:
            t = _safe_text(hh)
            if t and "read more" not in t.lower():
                return t
        card = card.find_parent()

    # Fallback: anchor text (may be generic)
    t = _safe_text(a)
    if t and "read more" not in t.lower():
        return t

    return ""

def scrape_xplore(
    since: date = date(2025, 1, 1),
    existing_links: Optional[Set[str]] = None,
    debug: bool = True,
) -> List[Dict[str, str]]:
    existing_links = set(existing_links or set())

    def log(msg: str) -> None:
        if debug:
            print(f"[XPLORE] {msg}")

    log(f"Starting Xplore scraper for date >= {since.isoformat()}")
    html = _http_get(BASE_LIST_URL, debug=debug)

    rows = parse_xplore_listing(html, debug=debug)
    log(f"Found {len(rows)} items on listing page (before filtering/dupes).")

    out: List[Dict[str, str]] = []
    skipped_dup = skipped_old = 0

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
        print(f"[XPLORE] Appended {len(rows)} rows to: {master_csv_path}")

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
    print(f"[XPLORE] Loaded {len(existing)} existing Xplore links from master: {master_csv}")

    rows = scrape_xplore(since=since, existing_links=existing, debug=True if args.debug or True else False)
    print(f"[XPLORE] Scraped {len(rows)} rows >= {since}.")

    if args.no_write:
        print("[XPLORE] --no-write set, nothing was written. First 8 rows:")
        for r in rows[:8]:
            print(r)
    else:
        appended = append_rows_to_master(master_csv, rows, debug=True)
        print(f"[XPLORE] Master CSV updated. Appended {appended} rows.")
