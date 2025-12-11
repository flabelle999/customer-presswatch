#!/usr/bin/env python
import csv
import os
import uuid
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

COMPANY = "SaskTel"
MASTER_CSV = "press_releases_master.csv"
CUTOFF_YEAR = 2025

BASE_URL = "https://www.sasktel.com"
START_URL = "https://www.sasktel.com/about-us/news/news-archives?archive=/content/home/about-sasktel/news/2025&tab=tab-2025"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


def load_master(path: str):
    if not os.path.exists(path):
        return [], set()

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    existing = {(r["company"], r["title"], r["date"]) for r in rows}
    return rows, existing


def append_rows(path: str, new_rows):
    file_exists = os.path.exists(path)
    fieldnames = [
        "id", "company", "title", "link", "date",
        "fetched_at", "summary_ai", "impact_for_zhone"
    ]

    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)


def parse_sasktel_date(text: str) -> datetime:
    """Parse both FR (12 novembre 2025) and EN (November 12, 2025)."""

    text = text.strip()

    # --- English format (e.g., 'November 12, 2025')
    try:
        return datetime.strptime(text, "%B %d, %Y").replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # --- French format (e.g., '12 novembre 2025')
    MONTHS_FR = {
        "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
        "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
        "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12
    }

    parts = text.split()
    if len(parts) != 3:
        raise ValueError(f"Unrecognized SaskTel date format: {text}")

    day = int(parts[0])
    month = MONTHS_FR[parts[1].lower()]
    year = int(parts[2])

    return datetime(year, month, day, tzinfo=timezone.utc)

def scrape_sasktel():
    print("[SASKTEL] Starting SaskTel scraper...")
    rows_master, existing = load_master(MASTER_CSV)

    resp = requests.get(START_URL, headers=HEADERS)
    if resp.status_code != 200:
        print(f"[SASKTEL] ERROR: status {resp.status_code}")
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    articles = soup.select("article.pt-30")

    results = []
    for art in articles:
        # Date
        label = art.find("label")
        if not label:
            continue
        date_text = label.get_text(strip=True)
        dt = parse_sasktel_date(date_text)

        # Filtrer les années
        if dt.year < CUTOFF_YEAR:
            continue

        # Title + link
        a = art.find("a")
        if not a:
            continue

        title = a.get_text(strip=True)
        href = a.get("href")
        link = BASE_URL + href

        results.append((dt, title, link))

    print(f"[SASKTEL] Found {len(results)} SaskTel PRs for 2025+")

    # Remove duplicates (if ever)
    unique = {}
    for dt, title, link in results:
        key = (title, dt.date())
        unique[key] = (dt, title, link)

    fetched_at = datetime.now(timezone.utc).isoformat()
    new_rows = []
    for dt, title, link in unique.values():
        key = (COMPANY, title, dt.date().isoformat())
        if key in existing:
            continue
        new_rows.append({
            "id": str(uuid.uuid4()),
            "company": COMPANY,
            "title": title,
            "link": link,
            "date": dt.date().isoformat(),
            "fetched_at": fetched_at,
            "summary_ai": "",
            "impact_for_zhone": "",
        })

    print(f"[SASKTEL] New SaskTel rows to add: {len(new_rows)}")
    if new_rows:
        append_rows(MASTER_CSV, new_rows)
        print("[SASKTEL] Added to master CSV.")
    else:
        print("[SASKTEL] Nothing new.")


if __name__ == "__main__":
    scrape_sasktel()
