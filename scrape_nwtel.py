import csv
import os
import re
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE_URL = "https://www.nwtel.ca/media"
COMPANY = "Northwestel"
MASTER_CSV = "press_releases_master.csv"
CUTOFF_YEAR = 2025


def log(msg: str) -> None:
    print(f"[NWTEL] {msg}")


# ---------- CSV helpers ----------

def load_existing_links(path: str, company: str) -> set:
    links = set()
    if not os.path.exists(path):
        return links
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("company") == company and row.get("link"):
                links.add(row["link"])
    return links


def append_rows(path: str, rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    file_exists = os.path.exists(path)
    fieldnames = [
        "id",
        "company",
        "title",
        "link",
        "date",
        "fetched_at",
        "summary_ai",
        "impact_for_zhone",
    ]
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ---------- Selenium setup ----------

def make_driver() -> webdriver.Chrome:
    options = Options()
    # commente cette ligne si tu veux voir le navigateur
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    driver.set_window_size(1400, 3000)
    return driver


# ---------- Date parsing ----------

DATE_RE = re.compile(
    r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|"
    r"Nov(?:ember)?|Dec(?:ember)?)\.?\s+"
    r"(\d{1,2})(?:st|nd|rd|th)?"
    r",\s+(\d{4})"
)

MONTH_MAP = {
    "Jan": 1, "January": 1,
    "Feb": 2, "February": 2,
    "Mar": 3, "March": 3,
    "Apr": 4, "April": 4,
    "May": 5,
    "Jun": 6, "June": 6,
    "Jul": 7, "July": 7,
    "Aug": 8, "August": 8,
    "Sep": 9, "Sept": 9, "September": 9,
    "Oct": 10, "October": 10,
    "Nov": 11, "November": 11,
    "Dec": 12, "December": 12,
}


def parse_nwtel_date(text: str) -> Optional[datetime]:
    """
    Extrait une date style 'Whitehorse, YT, Dec. 29, 2025'
    ou 'Whitehorse, YT, May 5th, 2025'.
    """
    if not text:
        return None

    # Normaliser les variantes de Septembre
    norm = text.replace("Sept.", "Sep").replace("Sept", "Sep")

    m = DATE_RE.search(norm)
    if not m:
        return None

    month_str, day_str, year_str = m.groups()
    month_key = month_str.replace(".", "")
    month = MONTH_MAP.get(month_key)
    if not month:
        return None

    day = int(day_str)
    year = int(year_str)

    try:
        return datetime(year, month, day)
    except ValueError:
        return None


# ---------- HTML extraction ----------

READ_MORE_RE = re.compile(r"^\s*read\s+more\s*$", re.IGNORECASE)


def extract_items(html: str):
    """
    Return a list of (datetime, title, absolute_href) for every release entry
    in every year tab. Entries whose date cannot be parsed are skipped.
    """
    soup = BeautifulSoup(html, "html.parser")
    items = []

    for body in soup.select("div.component-tab-slide__body"):
        for p in body.find_all("p"):
            a = p.find("a", href=True)
            if not a:
                continue

            title = re.sub(r"\s+", " ", a.get_text(" ", strip=True)).strip()
            # Skip the "Read more" links; they are not titles
            if not title or READ_MORE_RE.match(title):
                continue

            # The first non-empty <p> after the title paragraph holds the dateline
            date_text = ""
            sib = p
            for _ in range(3):
                sib = sib.find_next_sibling("p")
                if sib is None:
                    break
                txt = sib.get_text(" ", strip=True)
                if txt:
                    date_text = txt
                    break

            # Dateline is at the start of the paragraph; don't scan the whole teaser
            dt = parse_nwtel_date(date_text[:100])
            if not dt:
                continue

            href = urljoin(BASE_URL, a["href"].strip()).split("#")[0]
            items.append((dt, title, href))

    return items


# ---------- Scraping logic ----------

def scrape_nwtel() -> None:
    log(f"Starting Northwestel scraper for year >= {CUTOFF_YEAR}")

    existing_links = load_existing_links(MASTER_CSV, COMPANY)
    log(f"Existing Northwestel links in CSV: {len(existing_links)}")

    driver = make_driver()
    new_rows: List[Dict[str, str]] = []

    try:
        driver.get(BASE_URL)

        # Attendre que le contenu soit là
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.component-tab-slide__body")
            )
        )

        # The page has one tab per year (2026, 2025, 2024, ...). Only the active
        # tab is visible, so Selenium's element.text is empty for the other tabs
        # (that is why 2025 items were being missed). Parse the full page source
        # with BeautifulSoup instead, which also sees hidden tabs.
        #
        # Each release is structured as:
        #   <p><a href="..."><strong>Title</strong></a></p>
        #   <p><strong>Whitehorse, YT, Dec. 29, 2025</strong> - teaser...</p>
        #   <p><a href="..."><strong>Read more</strong></a></p>
        items = extract_items(driver.page_source)
        log(f"Found {len(items)} release entries (all years) on page")

        seen_links = set()
        for dt, title, href in items:
            if dt.year < CUTOFF_YEAR:
                continue

            if href in seen_links:
                continue
            seen_links.add(href)

            log(f"Candidate PR: {dt.strftime('%Y-%m-%d')} | {title} | {href}")

            if href in existing_links:
                log("  → Skipped (already in CSV)")
                continue
            else:
                log("  → NEW (will be added)")

            row = {
                "id": f"{COMPANY}_{dt.strftime('%Y%m%d')}_{abs(hash(title)) & 0xffffffff}",
                "company": COMPANY,
                "title": title,
                "link": href,
                "date": dt.strftime("%Y-%m-%d"),
                "fetched_at": datetime.utcnow().isoformat(timespec="seconds"),
                "summary_ai": "",
                "impact_for_zhone": "",
            }
            new_rows.append(row)

        log(f"Total Northwestel PRs >= {CUTOFF_YEAR} found on page (new only): {len(new_rows)}")

        if new_rows:
            append_rows(MASTER_CSV, new_rows)
            log(f"✅ Added {len(new_rows)} rows to {MASTER_CSV}")
        else:
            log("Nothing new.")

    finally:
        driver.quit()


if __name__ == "__main__":
    scrape_nwtel()
