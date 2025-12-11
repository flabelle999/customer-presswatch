#!/usr/bin/env python
import csv
import os
import re
import uuid
from datetime import datetime, timezone

from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException

BASE_URL = "https://www.telus.com/en/about/newsroom"
COMPANY = "TELUS"
MASTER_CSV = "press_releases_master.csv"
CUTOFF = datetime(2025, 1, 1, tzinfo=timezone.utc)

MONTH_PATTERN = r"(January|February|March|April|May|June|July|August|September|October|November|December)"
DATE_RE = re.compile(rf"{MONTH_PATTERN}\s+\d{{1,2}},\s+\d{{4}}")


def log(msg: str):
    print(f"[TELUS] {msg}", flush=True)


def parse_date(date_str: str) -> datetime:
    dt = datetime.strptime(date_str.strip(), "%B %d, %Y")
    return dt.replace(tzinfo=timezone.utc)


def load_master(path: str):
    if not os.path.exists(path):
        return [], set()

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    existing_keys = {(r["company"], r["title"], r["date"]) for r in rows}
    return rows, existing_keys


def append_rows(path: str, new_rows):
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
        writer.writerows(new_rows)


def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def load_all_2025_cards(driver: webdriver.Chrome):
    """
    Clique sur "Show more news" tant qu'on voit encore des dates 2025.
    Utilise scroll + fallback JS click pour éviter ElementClickInterceptedException.
    """
    driver.get(BASE_URL)
    wait = WebDriverWait(driver, 20)

    # attendre la première tuile
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[data-testid^='col-']")))

    while True:
        cards = driver.find_elements(By.CSS_SELECTOR, "div[data-testid^='col-']")
        log(f"Currently {len(cards)} cards loaded")

        # vérifier les dernières cartes pour voir la plus vieille année
        oldest_year = 9999
        for card in cards[-30:]:  # on regarde juste le bas de la liste
            text = card.text
            m = DATE_RE.search(text)
            if m:
                dt = parse_date(m.group(0))
                if dt.year < oldest_year:
                    oldest_year = dt.year

        log(f"Oldest year seen so far: {oldest_year}")

        # si on a déjà atteint 2024, on arrête de cliquer
        if oldest_year < 2025:
            log("Reached cards older than 2025 → stop loading more.")
            break

        # essayer de trouver le bouton "Show more news"
        try:
            btn = wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//button[contains(., 'Show more news')]")
                )
            )
        except TimeoutException:
            log("No more 'Show more news' button → stop.")
            break

        # scroll le bouton au centre de l'écran
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)

        # petite pause implicite via wait: on s'assure qu'il soit cliquable
        try:
            wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(., 'Show more news')]")
            ))
            prev_count = len(cards)
            try:
                btn.click()
            except ElementClickInterceptedException:
                # fallback : clic forcé en JS
                log("Click intercepted, using JS click fallback")
                driver.execute_script("arguments[0].click();", btn)
        except TimeoutException:
            log("Button not clickable → stop.")
            break

        # attendre que de nouvelles cartes soient ajoutées
        try:
            wait.until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, "div[data-testid^='col-']")) > prev_count
            )
        except TimeoutException:
            log("Timed out waiting for more cards → stop.")
            break


def extract_cards_from_dom(driver: webdriver.Chrome):
    cards = driver.find_elements(By.CSS_SELECTOR, "div[data-testid^='col-']")
    log(f"Total cards in DOM: {len(cards)}")

    items = []

    for card in cards:
        text = card.text
        m = DATE_RE.search(text)
        if not m:
            continue

        dt = parse_date(m.group(0))
        if dt < CUTOFF:
            continue

        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        title = ""
        date_seen = False
        for ln in lines:
            if not date_seen and DATE_RE.search(ln):
                date_seen = True
                continue
            if date_seen:
                title = ln
                break

        if not title:
            continue

        link_elts = card.find_elements(By.CSS_SELECTOR, "a[href*='/about/news']")
        if link_elts:
            href = link_elts[0].get_attribute("href")
        else:
            href = BASE_URL

        items.append((dt, title, href))

    return items


def scrape_telus():
    log("Starting TELUS Selenium scraper")
    _, existing_keys = load_master(MASTER_CSV)

    driver = create_driver()
    try:
        load_all_2025_cards(driver)
        items = extract_cards_from_dom(driver)
    finally:
        driver.quit()

    log(f"Found {len(items)} TELUS PRs with date >= 2025-01-01")

    # dédup par (title, date)
    unique = {}
    for dt, title, link in items:
        key = (title, dt.date().isoformat())
        if key not in unique:
            unique[key] = (dt, title, link)

    log(f"Unique TELUS rows >= cutoff after dedup: {len(unique)}")

    fetched_at = datetime.now(timezone.utc).isoformat()
    new_rows = []
    for dt, title, link in unique.values():
        date_str = dt.date().isoformat()
        key = (COMPANY, title, date_str)
        if key in existing_keys:
            continue

        new_rows.append(
            {
                "id": str(uuid.uuid4()),
                "company": COMPANY,
                "title": title,
                "link": link,
                "date": date_str,
                "fetched_at": fetched_at,
                "summary_ai": "",
                "impact_for_zhone": "",
            }
        )

    log(f"Total new TELUS rows to append: {len(new_rows)}")
    if new_rows:
        append_rows(MASTER_CSV, new_rows)
        log(f"Added {len(new_rows)} rows to {MASTER_CSV}")
    else:
        log("All TELUS 2025 rows were already present (or none matched).")


if __name__ == "__main__":
    scrape_telus()
