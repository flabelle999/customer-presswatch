#!/usr/bin/env python
import csv
import os
import uuid
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

COMPANY = "SaskTel"

# If you want it to write next to this script (recommended), uncomment the next line
# MASTER_CSV = os.path.join(os.path.dirname(__file__), "press_releases_master.csv")
MASTER_CSV = "press_releases_master.csv"

# Static start boundary: keep everything from Jan 1, 2025 onward
CUTOFF_YEAR = 2025

BASE_URL = "https://www.sasktel.com"

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

    # CSV may have been created by other scrapers using utf-8-sig
    try:
        f = open(path, newline="", encoding="utf-8")
        reader = csv.DictReader(f)
        rows = list(reader)
        f.close()
    except UnicodeDecodeError:
        with open(path, newline="", encoding="latin-1") as f2:
            reader = csv.DictReader(f2)
            rows = list(reader)

    existing = {(r.get("company", ""), r.get("title", ""), r.get("date", "")) for r in rows}
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
    text = (text or "").strip()

    # --- English format (e.g., 'November 12, 2025')
    try:
        return datetime.strptime(text, "%B %d, %Y").replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # --- French format (e.g., '12 novembre 2025')
    months_fr = {
        "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
        "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
        "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12
    }

    parts = text.split()
    if len(parts) != 3:
        raise ValueError(f"Unrecognized SaskTel date format: {text}")

    day = int(parts[0])
    month = months_fr[parts[1].lower()]
    year = int(parts[2])

    return datetime(year, month, day, tzinfo=timezone.utc)


def build_archive_url(year: int) -> str:
    # SaskTel uses: ?archive=/content/home/about-sasktel/news/YYYY&tab=tab-YYYY
    return (
        f"{BASE_URL}/about-us/news/news-archives"
        f"?archive=/content/home/about-sasktel/news/{year}&tab=tab-{year}"
    )


def scrape_archive_year(year: int):
    """Return list of tuples: (dt, title, link) for a given archive year page."""
    url = build_archive_url(year)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"[SASKTEL] WARNING: status {resp.status_code} for {url}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    articles = soup.select("article.pt-30")
    out = []

    for art in articles:
        # Date
        label = art.find("label")
        if not label:
            continue
        date_text = label.get_text(strip=True)

        try:
            dt = parse_sasktel_date(date_text)
        except Exception:
            continue

        # Static cutoff: keep 2025+
        if dt.year < CUTOFF_YEAR:
            continue

        # Title + link
        a = art.find("a")
        if not a or not a.get("href"):
            continue

        title = a.get_text(strip=True) or "(No title)"
        link = urljoin(BASE_URL, a["href"])

        out.append((dt, title, link))

    return out


def scrape_sasktel():
    print("[SASKTEL] Starting SaskTel scraper...")
    _, existing = load_master(MASTER_CSV)

    # Auto-include current year down to cutoff year so 2026+ never gets missed.
    now_year = datetime.now(timezone.utc).year
    archive_years = list(range(now_year, CUTOFF_YEAR - 1, -1))

    results = []
    for y in archive_years:
        year_rows = scrape_archive_year(y)
        print(f"[SASKTEL] Year {y}: found {len(year_rows)} PRs meeting cutoff ({CUTOFF_YEAR}+).")
        results.extend(year_rows)

    # De-dupe by (title, date)
    unique = {}
    for dt, title, link in results:
        key = (title, dt.date().isoformat())
        unique[key] = (dt, title, link)

    fetched_at = datetime.now(timezone.utc).isoformat()
    new_rows = []

    for dt, title, link in unique.values():
        date_iso = dt.date().isoformat()
        key = (COMPANY, title, date_iso)
        if key in existing:
            continue

        new_rows.append({
            "id": str(uuid.uuid4()),
            "company": COMPANY,
            "title": title,
            "link": link,
            "date": date_iso,
            "fetched_at": fetched_at,
            "summary_ai": "",
            "impact_for_zhone": "",
        })

    print(f"[SASKTEL] Unique PRs kept (>= {CUTOFF_YEAR}): {len(unique)}")
    print(f"[SASKTEL] New SaskTel rows to add: {len(new_rows)}")

    if new_rows:
        append_rows(MASTER_CSV, new_rows)
        print("[SASKTEL] Added to master CSV.")
    else:
        print("[SASKTEL] Nothing new.")


if __name__ == "__main__":
    scrape_sasktel()
