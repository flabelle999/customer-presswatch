"""
scrape_videotron.py

Videotron press room scraper (>= Jan 1, 2025) with Selenium "Voir plus" load-more loop.
Outputs rows in PressWatch master format:
  A: id (empty)
  B: company
  C: title
  D: link
  E: date (YYYY-MM-DD)

Deps:
  pip install requests beautifulsoup4 python-dateutil selenium webdriver-manager
"""

import argparse
import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Set, List, Tuple, Dict
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    from dateutil import parser as date_parser
except Exception:  # pragma: no cover
    date_parser = None


BASE_LIST_URL = "https://corpo.videotron.com/en/press-room"
BASE_DOMAIN = "https://corpo.videotron.com"
COMPANY_NAME = "Videotron"

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


def _parse_date_any(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip()

    # ISO date fast-path
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

    # minimal fallback
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue

    return None


def _http_get(url: str, timeout: int = 30) -> str:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
    r.raise_for_status()
    return r.text


# -----------------------------
# Article date fallback
# -----------------------------

def _extract_date_from_article_html(html: str) -> Optional[date]:
    soup = BeautifulSoup(html, "html.parser")

    # Meta tags
    for sel, attr in [
        ('meta[property="article:published_time"]', "content"),
        ('meta[name="date"]', "content"),
        ('meta[name="publish-date"]', "content"),
        ('meta[itemprop="datePublished"]', "content"),
    ]:
        node = soup.select_one(sel)
        if node and node.get(attr):
            d = _parse_date_any(node.get(attr, ""))
            if d:
                return d

    # JSON-LD
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue

        objs = data if isinstance(data, list) else [data]
        for obj in objs:
            if isinstance(obj, dict):
                val = obj.get("datePublished") or obj.get("dateCreated") or obj.get("dateModified")
                if isinstance(val, str):
                    d = _parse_date_any(val)
                    if d:
                        return d

    # <time datetime="...">
    t = soup.select_one("time[datetime]")
    if t and t.get("datetime"):
        d = _parse_date_any(t.get("datetime", ""))
        if d:
            return d

    # last ditch visible scan
    text = soup.get_text(" ", strip=True)
    m = re.search(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\w*\s+\d{1,2},\s+\d{4}\b", text)
    if m:
        return _parse_date_any(m.group(0))

    return None


# -----------------------------
# Listing parse (expanded HTML)
# -----------------------------

def _is_pressroom_item_url(href: str) -> bool:
    """
    From your DOM, links look like:
      /en/pressroom/videotron-... (or /fr/salle-de-presse/...)
    We'll accept:
      - same domain
      - contains '/pressroom/' OR '/salle-de-presse/' (to be safe)
    """
    if not href:
        return False
    href = _norm_url(urljoin(BASE_DOMAIN, href))
    if "corpo.videotron.com" not in href:
        return False
    return ("/pressroom/" in href) or ("/salle-de-presse/" in href)


def _parse_listing_page(html: str) -> List[Tuple[str, str, Optional[date]]]:
    """
    Returns [(title, link, date_or_none)] from the expanded press-room listing.

    Your DOM:
      <a href=".../en/pressroom/...">
        ...
        <h3 class="card-title lh-3"><span>Title ...</span></h3>
        ...
        <p class="mb-0">November 20, 2025</p>
      </a>
    """
    soup = BeautifulSoup(html, "html.parser")
    out: List[Tuple[str, str, Optional[date]]] = []

    # Primary: cards as anchors
    # We keep it robust: look for anchors with /pressroom/ in href
    anchors = soup.select('a[href*="/pressroom/"], a[href*="/salle-de-presse/"]')

    for a in anchors:
        href = a.get("href", "").strip()
        link = _norm_url(urljoin(BASE_DOMAIN, href))
        if not _is_pressroom_item_url(link):
            continue

        # Title: prefer the card title span you showed
        title_el = a.select_one("h3.card-title span") or a.select_one("h3.card-title") or a.find(["h2", "h3"])
        title = _safe_text(title_el)
        if not title:
            continue

        # Date: your example is p.mb-0
        date_el = a.select_one("p.mb-0") or a.select_one("time") or a.select_one(".date")
        d = None
        if date_el:
            d = _parse_date_any(date_el.get("datetime", "") or _safe_text(date_el))

        out.append((title, link, d))

    # Dedup by link
    seen = set()
    deduped: List[Tuple[str, str, Optional[date]]] = []
    for t, l, d in out:
        if not l or l in seen:
            continue
        seen.add(l)
        deduped.append((t, l, d))

    return deduped


# -----------------------------
# Selenium "Voir plus" loader
# -----------------------------

def _get_listing_html_with_selenium_load_more(
    url: str,
    cutoff: date,
    max_clicks: int = 60,
    debug: bool = True,
) -> str:
    """
    Clicks "Voir plus" / "Load more" repeatedly.
    Stops when:
      - button disappears OR
      - no new items appear OR
      - oldest visible date appears < cutoff (best-effort)
    """
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service as ChromeService
    import time

    def log(msg: str) -> None:
        if debug:
            print(f"[VIDEOTRON][SEL] {msg}")

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,1000")
    opts.add_argument(f"--user-agent={UA}")

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
    wait = WebDriverWait(driver, 20)

    try:
        log(f"Opening {url}")
        driver.get(url)

        # Wait for cards to appear (anchor pattern from DOM)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/pressroom/"], a[href*="/salle-de-presse/"]')))

        last_count = 0

        for i in range(max_clicks):
            cards = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/pressroom/"], a[href*="/salle-de-presse/"]')
            count = len(cards)
            log(f"Iteration {i+1}/{max_clicks} - visible pressroom links: {count}")

            if i > 0 and count <= last_count:
                log("No increase detected; stopping.")
                break
            last_count = count

            # Best-effort cutoff detection by reading visible date nodes
            oldest = None
            date_els = driver.find_elements(By.CSS_SELECTOR, "p.mb-0, time")
            for el in date_els:
                txt = (el.get_attribute("datetime") or el.text or "").strip()
                d = _parse_date_any(txt)
                if d:
                    oldest = d if oldest is None else min(oldest, d)

            if oldest and oldest < cutoff:
                log(f"Oldest visible date {oldest} < cutoff {cutoff}; stopping clicks.")
                break

            # Find button by text (FR/EN)
            load_more = None
            for b in driver.find_elements(By.TAG_NAME, "button"):
                txt = (b.text or "").strip().lower()
                if ("voir plus" in txt) or ("load more" in txt) or ("see more" in txt):
                    load_more = b
                    break

            if load_more is None:
                # sometimes it's a link styled as button
                for a in driver.find_elements(By.TAG_NAME, "a"):
                    txt = (a.text or "").strip().lower()
                    if ("voir plus" in txt) or ("load more" in txt) or ("see more" in txt):
                        load_more = a
                        break

            if load_more is None:
                log("No 'Voir plus' / 'Load more' control found; done.")
                break

            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", load_more)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", load_more)
            log("Clicked load more.")
            time.sleep(1.5)

        return driver.page_source

    finally:
        driver.quit()


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

    # IMPORTANT: A=id blank, B-E are company/title/link/date
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
        print(f"[VIDEOTRON] Appended {len(rows)} rows to: {master_csv_path}")

    return len(rows)


# -----------------------------
# Main scraper
# -----------------------------

def scrape_videotron(
    since: date = date(2025, 1, 1),
    existing_links: Optional[Set[str]] = None,
    debug: bool = True,
    use_selenium_load_more: bool = True,
    selenium_max_clicks: int = 60,
) -> List[Dict[str, str]]:
    existing_links = set(existing_links or set())
    results: List[PRItem] = []

    def log(msg: str) -> None:
        if debug:
            print(f"[VIDEOTRON] {msg}")

    log(f"Starting Videotron scraper for date >= {since.isoformat()}")
    log(f"Existing Videotron links provided: {len(existing_links)}")

    # 1) Listing HTML (expanded)
    if use_selenium_load_more:
        try:
            list_html = _get_listing_html_with_selenium_load_more(
                BASE_LIST_URL, cutoff=since, max_clicks=selenium_max_clicks, debug=debug
            )
        except Exception as e:
            log(f"Selenium listing loader failed ({e}); falling back to requests.")
            list_html = _http_get(BASE_LIST_URL)
    else:
        list_html = _http_get(BASE_LIST_URL)

    # 2) Parse cards
    items = _parse_listing_page(list_html)
    log(f"Found {len(items)} candidate items on listing page (after expansion).")

    added = skipped_dup = skipped_old = skipped_no_date = 0

    # 3) Resolve dates (card first, then article fallback), filter cutoff, dedupe
    for title, link, d in items:
        if not link:
            continue

        if link in existing_links:
            skipped_dup += 1
            continue

        if d is None:
            try:
                art_html = _http_get(link)
                d = _extract_date_from_article_html(art_html)
            except Exception as e:
                log(f"[DATE] Could not fetch/parse date for {link}: {e}")
                d = None

        if d is None:
            skipped_no_date += 1
            if debug:
                log(f"[SKIP] No date found: {link}")
            continue

        if d < since:
            skipped_old += 1
            continue

        existing_links.add(link)
        results.append(PRItem(company=COMPANY_NAME, title=title, link=link, date=d.isoformat()))
        added += 1

    log(f"Done. Added={added} | Skipped dup={skipped_dup} | Skipped old<{since}={skipped_old} | Skipped no-date={skipped_no_date}")
    return [r.__dict__ for r in results]


# -----------------------------
# CLI
# -----------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--since", default="2025-01-01", help="Cutoff date YYYY-MM-DD (inclusive). Default 2025-01-01")
    p.add_argument("--master-csv", default="press_releases_master.csv", help="If provided, append results to this master CSV path.")
    p.add_argument("--no-selenium", action="store_true", help="Disable Selenium load-more expansion.")
    p.add_argument("--max-clicks", type=int, default=60, help="Max 'Voir plus' clicks (default 60).")
    p.add_argument("--debug", action="store_true", help="Enable debug logs.")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    since = datetime.strptime(args.since, "%Y-%m-%d").date()

    master_csv = args.master_csv.strip()
    if master_csv:
        existing = load_existing_links_from_master(master_csv)
        print(f"[VIDEOTRON] Loaded {len(existing)} existing Videotron links from master: {master_csv}")
    else:
        existing = set()

    rows = scrape_videotron(
        since=since,
        existing_links=existing,
        debug=True if args.debug or True else False,
        use_selenium_load_more=not args.no_selenium,
        selenium_max_clicks=args.max_clicks,
    )

    print(f"[VIDEOTRON] Scraped {len(rows)} rows >= {since}.")

    if master_csv:
        appended = append_rows_to_master(master_csv, rows, debug=True)
        print(f"[VIDEOTRON] Master CSV updated. Appended {appended} rows.")
    else:
        print("[VIDEOTRON] No --master-csv provided, so nothing was written. Showing first 8 rows:")
        for r in rows[:8]:
            print(r)
