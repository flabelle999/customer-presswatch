"""
scrape_rogers.py

Rogers "News & Ideas" scraper (>= Jan 1, 2025) with Selenium "Load more".
Can be:
  A) imported by orchestrator: scrape_rogers(...)
  B) run standalone to append to master CSV:
        python scrape_rogers.py --master-csv "Press Release Masters/press_releases_master.csv"

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


BASE_LIST_URL = "https://about.rogers.com/news-ideas/"
COMPANY_NAME = "Rogers"

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


def _is_article_url(href: str) -> bool:
    if not href:
        return False
    href = _norm_url(href)

    if "about.rogers.com" not in href:
        return False
    if "/news-ideas/" not in href:
        return False

    bad_parts = ["/category/", "/tag/", "/page/", "?s="]
    return not any(b in href for b in bad_parts)


def _parse_date_any(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip()

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

    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y/%m/%d"):
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
# Article date extraction
# -----------------------------

def _extract_date_from_article_html(html: str) -> Optional[date]:
    soup = BeautifulSoup(html, "html.parser")

    meta_selectors = [
        ('meta[property="article:published_time"]', "content"),
        ('meta[name="article:published_time"]', "content"),
        ('meta[name="date"]', "content"),
        ('meta[name="publish-date"]', "content"),
        ('meta[name="pubdate"]', "content"),
        ('meta[itemprop="datePublished"]', "content"),
    ]
    for sel, attr in meta_selectors:
        node = soup.select_one(sel)
        if node and node.get(attr):
            d = _parse_date_any(node.get(attr, ""))
            if d:
                return d

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
            if not isinstance(obj, dict):
                continue
            val = obj.get("datePublished") or obj.get("dateCreated") or obj.get("dateModified")
            if isinstance(val, str):
                d = _parse_date_any(val)
                if d:
                    return d

    t = soup.select_one("time[datetime]")
    if t and t.get("datetime"):
        d = _parse_date_any(t.get("datetime", ""))
        if d:
            return d

    text = soup.get_text(" ", strip=True)
    m = re.search(
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\w*\s+\d{1,2},\s+\d{4}\b",
        text,
    )
    if m:
        return _parse_date_any(m.group(0))

    return None


# -----------------------------
# Listing parsing
# -----------------------------

def _parse_listing_page(html: str) -> List[Tuple[str, str, Optional[date]]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Tuple[str, str, Optional[date]]] = []

    # Featured (per your screenshot)
    featured_date = None
    fd = soup.select_one("span.featured-post__date")
    if fd:
        featured_date = _parse_date_any(_safe_text(fd))

    ft = soup.select_one("a.featured-post__title")
    if ft and ft.get("href"):
        link = _norm_url(urljoin(BASE_LIST_URL, ft["href"]))
        title = _safe_text(ft)
        if title and _is_article_url(link):
            out.append((title, link, featured_date))

    # Other posts
    posts = soup.select("#posts .news__article, #posts article, #posts .news__article-card")
    if not posts:
        posts = soup.select("section.news__posts #posts a[href]")

    for node in posts:
        a = node if getattr(node, "name", "") == "a" else node.select_one("a[href]")
        if not a:
            continue

        link = _norm_url(urljoin(BASE_LIST_URL, a.get("href", "")))
        if not _is_article_url(link):
            continue

        title_el = node.select_one(".news__title, .news__article-title, h3, h2, .title, .headline")
        title = _safe_text(title_el) if title_el else _safe_text(a)
        if not title or title.lower() in ("learn more", "read more"):
            heading = node.find(["h2", "h3", "h4"])
            title = _safe_text(heading) or title

        date_el = node.select_one(".news__date, .news__article-date, .date, time, .news__meta time, .meta time")
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
# Selenium "Load more"
# -----------------------------

def _get_listing_html_with_selenium_load_more(
    url: str,
    cutoff: date,
    max_clicks: int = 60,
    debug: bool = True,
) -> str:
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
            print(f"[ROGERS][SEL] {msg}")

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

        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#posts, section.news__posts, .news__posts-container")))

        last_count = 0

        for i in range(max_clicks):
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/news-ideas/']")
            count = len(links)
            log(f"Iteration {i+1}/{max_clicks} - visible /news-ideas/ links: {count}")

            if i > 0 and count <= last_count:
                log("No increase in links detected; stopping.")
                break
            last_count = count

            # Best-effort cutoff check
            oldest = None
            date_els = driver.find_elements(By.CSS_SELECTOR, "span.featured-post__date, .news__date, .date, time")
            for el in date_els:
                txt = (el.get_attribute("datetime") or el.text or "").strip()
                d = _parse_date_any(txt)
                if d:
                    oldest = d if oldest is None else min(oldest, d)

            if oldest and oldest < cutoff:
                log(f"Oldest visible date {oldest} < cutoff {cutoff}; stopping clicks.")
                break

            # Find "Load more"
            load_more = None
            for b in driver.find_elements(By.TAG_NAME, "button"):
                if "load more" in (b.text or "").strip().lower():
                    load_more = b
                    break
            if load_more is None:
                for a in driver.find_elements(By.TAG_NAME, "a"):
                    if "load more" in (a.text or "").strip().lower():
                        load_more = a
                        break

            if load_more is None:
                log("No 'Load more' control found; done.")
                break

            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", load_more)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", load_more)
            log("Clicked 'Load more'.")
            time.sleep(1.5)

        return driver.page_source

    finally:
        driver.quit()


# -----------------------------
# Master CSV utilities
# -----------------------------

def load_existing_links_from_master(master_csv_path: str) -> Set[str]:
    links: Set[str] = set()
    if not master_csv_path or not os.path.exists(master_csv_path):
        return links

    with open(master_csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("link") or row.get("url") or row.get("URL") or "").strip()
            company = (row.get("company") or row.get("source") or "").strip()
            if company.lower() == COMPANY_NAME.lower() and url:
                links.add(_norm_url(url))
    return links


def append_rows_to_master(master_csv_path: str, rows: List[Dict[str, str]], debug: bool = True) -> int:
    """
    Append rows to master CSV. Creates it if missing (with standard headers).
    """
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
        print(f"[ROGERS] Appended {len(rows)} rows to: {master_csv_path}")

    return len(rows)

# -----------------------------
# Main scraper
# -----------------------------

def scrape_rogers(
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
            print(f"[ROGERS] {msg}")

    log(f"Starting Rogers scraper for date >= {since.isoformat()}")
    log(f"Existing Rogers links provided: {len(existing_links)}")

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

    items = _parse_listing_page(list_html)
    log(f"Found {len(items)} candidate items on listing page (after expansion).")

    added = skipped_dup = skipped_old = skipped_no_date = 0

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

    log(
        f"Done. Added={added} | Skipped dup={skipped_dup} | Skipped old<{since}={skipped_old} | Skipped no-date={skipped_no_date}"
    )

    return [r.__dict__ for r in results]


# -----------------------------
# CLI
# -----------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--since", default="2025-01-01", help="Cutoff date YYYY-MM-DD (inclusive). Default 2025-01-01")
    p.add_argument("--master-csv", default="press_releases_master.csv", help="If provided, append results to this master CSV path.")
    p.add_argument("--no-selenium", action="store_true", help="Disable Selenium load-more expansion.")
    p.add_argument("--max-clicks", type=int, default=60, help="Max Load more clicks (default 60).")
    p.add_argument("--debug", action="store_true", help="Enable debug logs.")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    since = datetime.strptime(args.since, "%Y-%m-%d").date()

    master_csv = args.master_csv.strip()
    if master_csv:
        existing = load_existing_links_from_master(master_csv)
        print(f"[ROGERS] Loaded {len(existing)} existing Rogers links from master: {master_csv}")
    else:
        existing = set()

    rows = scrape_rogers(
        since=since,
        existing_links=existing,
        debug=True if args.debug or True else False,  # default on when run standalone
        use_selenium_load_more=not args.no_selenium,
        selenium_max_clicks=args.max_clicks,
    )

    print(f"[ROGERS] Scraped {len(rows)} rows >= {since}.")

    if master_csv:
        appended = append_rows_to_master(master_csv, rows, debug=True)
        print(f"[ROGERS] Master CSV updated. Appended {appended} rows.")
    else:
        print("[ROGERS] No --master-csv provided, so nothing was written. Showing first 8 rows:")
        for r in rows[:8]:
            print(r)
