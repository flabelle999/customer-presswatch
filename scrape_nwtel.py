import csv
import os
import re
from datetime import datetime
from typing import List, Dict, Optional

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

        # Sur la page 2025, chaque communiqué est structuré ainsi :
        # <p><a><strong>Titre...</strong></a></p>
        # <p><strong>Whitehorse, YT, June 25, 2025</strong> – blabla...</p>
        # On cible donc les <a> dont l'enfant direct est un <strong>
        anchors = driver.find_elements(
            By.XPATH,
            "//div[contains(@class,'component-tab-slide__body')]//p/a[strong]"
        )

        log(f"Found {len(anchors)} title anchors")

        for a_el in anchors:
            title = a_el.text.strip()
            href = a_el.get_attribute("href") or ""

            if not title or not href:
                continue

            if href.startswith("/"):
                href = "https://www.nwtel.ca" + href

            # Le <a> est dans un <p>. Le <p> suivant contient la date en <strong>
            try:
                parent_p = a_el.find_element(By.XPATH, "./parent::p")
                date_strong = parent_p.find_element(
                    By.XPATH, "./following-sibling::p[1]/strong"
                )
                date_text = date_strong.text.strip()
            except Exception:
                # petit fallback : chercher le premier strong "2025" après le <a>
                try:
                    date_strong = a_el.find_element(
                        By.XPATH,
                        "./ancestor::div[contains(@class,'component-tab-slide__body')][1]"
                        "//strong[re:match(., '20\\d{2}')]"
                    )
                    date_text = date_strong.text.strip()
                except Exception:
                    preview = title if len(title) < 60 else title[:57] + "..."
                    log(f"⚠️ Could not find date for '{preview}' – skipping.")
                    continue

            dt = parse_nwtel_date(date_text)
            if not dt:
                preview = date_text if len(date_text) < 60 else date_text[:57] + "..."
                log(f"⚠️ Could not parse date '{preview}' – skipping.")
                continue

            if dt.year < CUTOFF_YEAR:
                log(f"Skip {dt.date()} (year < {CUTOFF_YEAR})")
                continue

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
