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
import time
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

# Current desktop Chrome UA (other sites in this project reject Chrome < 130).
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/141.0.0.0 Safari/537.36"
)

BROWSER_HEADERS = {
    "User-Agent": UA,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
    "Sec-CH-UA": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

# Markers of a Cloudflare / bot-protection interstitial instead of the real page.
BLOCK_MARKERS = (
    "just a moment", "cf-chl", "challenge-platform", "attention required",
    "access denied", "enable javascript and cookies", "cf-browser-verification",
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


def _elog(msg: str) -> None:
    print(f"[EASTLINK] {msg}", flush=True)


def _looks_blocked(html: str) -> bool:
    low = (html or "")[:20000].lower()
    return any(m in low for m in BLOCK_MARKERS)


def _http_get(url: str, timeout: int = 30, attempts: int = 3) -> str:
    """Plain requests fetch with browser-like headers; logs status for diagnosis.

    Raises on final failure (caller decides whether to fall back to a browser).
    """
    last_exc: Optional[Exception] = None
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)
    for i in range(1, attempts + 1):
        try:
            r = session.get(url, timeout=timeout)
            server = r.headers.get("Server", "?")
            cf_ray = r.headers.get("CF-RAY", "-")
            _elog(
                f"[requests] attempt {i}/{attempts} GET {url} -> HTTP {r.status_code} "
                f"final_url={r.url} bytes={len(r.content)} server={server} cf-ray={cf_ray}"
            )
            html = r.content.decode("utf-8", errors="replace")
            if r.status_code == 200 and not _looks_blocked(html):
                return html
            if r.status_code == 200:
                _elog("[requests] HTTP 200 but body looks like a bot-protection challenge page.")
                return html  # let the parser find 0 items -> browser fallback
            last_exc = requests.HTTPError(f"HTTP {r.status_code}")
            if r.status_code in (403, 429, 503):
                _elog(f"[requests] blocked/rate-limited. body[:150]={html[:150]!r}")
        except requests.RequestException as e:
            last_exc = e
            _elog(f"[requests] attempt {i}/{attempts} failed: {type(e).__name__}: {e}")
        if i < attempts:
            time.sleep(2 * i)
    raise last_exc or RuntimeError("request failed")


def _get_html_playwright(url: str, timeout_ms: int = 60000) -> str:
    """Headless Chromium via Playwright (imported lazily)."""
    from playwright.sync_api import sync_playwright  # lazy: optional dependency

    with sync_playwright() as p:
        browser = None
        for kwargs in ({"headless": True}, {"headless": True, "channel": "chrome"}):
            try:
                browser = p.chromium.launch(
                    args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
                    **kwargs,
                )
                break
            except Exception as e:  # try next launcher
                _elog(f"[playwright] launch {kwargs} failed: {type(e).__name__}: {str(e)[:150]}")
        if browser is None:
            raise RuntimeError("could not launch any Playwright browser")
        try:
            ctx = browser.new_context(
                user_agent=UA,
                locale="en-CA",
                timezone_id="America/Halifax",
                viewport={"width": 1366, "height": 900},
                extra_http_headers={"Accept-Language": BROWSER_HEADERS["Accept-Language"]},
            )
            ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            )
            page = ctx.new_page()
            resp = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            _elog(f"[playwright] goto -> HTTP {resp.status if resp else '?'} url={page.url}")
            # A Cloudflare JS challenge usually resolves by itself within a few seconds.
            try:
                page.wait_for_selector("div.news-item, a[href*='/news-release/']", timeout=30000)
            except Exception:
                _elog(f"[playwright] news items did not appear; title={page.title()!r}")
            html = page.content()
            return html
        finally:
            browser.close()


def _get_html_selenium(url: str, timeout_s: int = 60) -> str:
    """Headless Chrome via Selenium (imported lazily)."""
    from selenium import webdriver  # lazy: optional dependency
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,900")
    opts.add_argument("--lang=en-CA")
    opts.add_argument(f"--user-agent={UA}")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])

    driver = None
    try:
        driver = webdriver.Chrome(options=opts)  # Selenium Manager resolves the driver
    except Exception as e:
        _elog(f"[selenium] default driver start failed ({type(e).__name__}); trying webdriver-manager")
        from selenium.webdriver.chrome.service import Service
        from webdriver_manager.chrome import ChromeDriverManager

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    try:
        driver.set_page_load_timeout(timeout_s)
        driver.get(url)
        _elog(f"[selenium] loaded url={driver.current_url} title={driver.title!r}")
        try:
            WebDriverWait(driver, 30).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, "div.news-item, a[href*='/news-release/']")
            )
        except Exception:
            _elog("[selenium] news items did not appear within 30s")
        return driver.page_source
    finally:
        try:
            driver.quit()
        except Exception:
            pass


def _get_html_browser(url: str) -> str:
    """Try Playwright, then Selenium. Returns '' if neither could produce HTML."""
    for name, fn in (("playwright", _get_html_playwright), ("selenium", _get_html_selenium)):
        try:
            html = fn(url)
            n = len(BeautifulSoup(html, "html.parser").select("div.news-item[data-year]"))
            _elog(f"[{name}] returned {len(html)} bytes, news-item cards={n}")
            if html and (n > 0 or "/news-release/" in html):
                return html
        except ImportError as e:
            _elog(f"[{name}] not installed: {e}")
        except Exception as e:
            _elog(f"[{name}] failed: {type(e).__name__}: {str(e)[:200]}")
    return ""


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


_DATE_PREFIX_RE = re.compile(
    r"^\s*(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},?\s+\d{4}\s*"
)
_DATE_ANYWHERE_RE = re.compile(
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},?\s+\d{4}"
)


def _card_title(a, card, datestr: str) -> str:
    """Headline only: no date prefix and no sub-headline.

    The card anchor text is "<date> <headline>\\n<sub-headline>". Older rows in the
    master CSV were stored with the date prefix and sub-headline included; dedup is by
    link only (see load_existing_links_from_master), so cleaner titles are safe.
    """
    title_el = a.select_one("h3") or a.select_one("h2") or card.find(["h2", "h3", "h4"])
    title = _safe_text(title_el)
    if title:
        return _DATE_PREFIX_RE.sub("", title).strip()

    # Lines of visible text inside the anchor; drop the date line, keep the first headline line.
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in a.get_text("\n").split("\n")]
    lines = [ln for ln in lines if ln and ln != datestr]
    if lines:
        return _DATE_PREFIX_RE.sub("", lines[0]).strip()
    return _DATE_PREFIX_RE.sub("", _safe_text(a)).strip()


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

        # Date: <small data-years="2025">July 16, 2025</small>
        date_el = card.select_one("small[data-years]") or card.select_one("small") or card.select_one("time")
        datestr = _safe_text(date_el)
        d = _parse_date_any(datestr)

        title = _card_title(a, card, datestr)

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
            small = a.find_next("small")
            datestr = _safe_text(small) if small else ""
            m = _DATE_ANYWHERE_RE.search(_safe_text(a))
            if m:  # date rendered inside the anchor text
                datestr = m.group(0)
            title = _card_title(a, a, datestr)
            d = _parse_date_any(datestr)
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

    # 1) Plain HTTP (fast path)
    html = ""
    rows: List[Dict[str, str]] = []
    try:
        html = _http_get(BASE_LIST_URL)
        rows = parse_eastlink_listing(html, debug=debug)
        log(f"[requests] parsed {len(rows)} items (html bytes={len(html)}, "
            f"news-item cards={len(BeautifulSoup(html, 'html.parser').select('div.news-item'))})")
        if not rows:
            snippet = re.sub(r"\s+", " ", BeautifulSoup(html, "html.parser").get_text(" ", strip=True))[:200]
            log(f"[requests] 0 items; page text starts: {snippet!r}")
    except Exception as e:
        log(f"[requests] failed: {type(e).__name__}: {e}")

    # 2) Headless-browser fallback (blocked / challenge page / layout needs JS)
    if not rows:
        log("Plain requests gave 0 items -> trying headless browser fallback (Playwright, then Selenium)")
        bhtml = _get_html_browser(BASE_LIST_URL)
        if bhtml:
            rows = parse_eastlink_listing(bhtml, debug=debug)
            log(f"[browser] parsed {len(rows)} items")
        else:
            log("[browser] no HTML obtained from any headless browser")

    if not rows:
        log("WARNING: 0 items from every method. The site is probably blocking this IP "
            "(Cloudflare) or its layout changed. Nothing will be added.")
    log(f"Found {len(rows)} items on listing page (before filtering/dupes).")
    if rows:
        newest = max(r["date"] for r in rows)
        log(f"Newest item on page: {newest}")

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
