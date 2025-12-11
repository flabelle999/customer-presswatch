#!/usr/bin/env python
import csv
import os
import re
import uuid
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.bce.ca/news-and-media/newsroom"
COMPANY = "Bell"
MASTER_CSV = "press_releases_master.csv"
CUTOFF_YEAR = 2025   # on prend tous les PR de 2025 (et plus récents)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

MONTH_PATTERN = r"(January|February|March|April|May|June|July|August|September|October|November|December)"
DATE_RE = re.compile(rf"^{MONTH_PATTERN}\s+\d{{1,2}},\s+\d{{4}}$")


def slugify(title: str) -> str:
    """Slug BCE : conserve les majuscules, enlève les accents, remplace les séparateurs par des tirets."""
    import unicodedata
    import re

    # Ne pas lower() → on garde Bell / BCE / CRTC en majuscules
    s = title.strip()

    # Normaliser et enlever les accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))

    # Tout ce qui n'est pas lettre/chiffre → tiret
    s = re.sub(r"[^A-Za-z0-9]+", "-", s)

    # Réduire les tirets multiples, enlever en début/fin
    s = re.sub(r"-+", "-", s).strip("-")

    return s


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%B %d, %Y").replace(tzinfo=timezone.utc)


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


def normalize_bce_link(link: str) -> str:
    """
    BCE utilise le format ?article=slug.
    Si on a encore un lien avec un #slug, on le convertit.
    """
    if "news-and-media/newsroom#" in link:
        base, frag = link.split("#", 1)
        return f"{base}?article={frag}"
    return link


def extract_latest_news(soup: BeautifulSoup):
    """Bloc 'Latest news' (cartes du haut)."""
    rows = []
    # span avec id="heading-..."
    for span in soup.select("span[id^='heading-']"):
        title = span.get_text(strip=True)

        # la date est dans le h2 précédent dans la même carte
        date_el = span.find_parent().find_previous("h2")
        if not date_el:
            continue

        date_text = date_el.get_text(strip=True)
        if not DATE_RE.match(date_text):
            continue

        dt = parse_date(date_text)
        if dt.year < CUTOFF_YEAR:
            continue

        # slug basé sur le titre
        slug = slugify(title)

        # format officiel BCE
        link = f"{BASE_URL}?article={slug}"
        link = normalize_bce_link(link)

        rows.append((dt, title, link))

    return rows


def extract_news_archive(soup: BeautifulSoup, page_index: int):
    """
    Extrait le bloc 'News archive' en lisant le texte de la page.

    Retourne:
      - rows: list[(dt, title, link)]
      - hit_older: True si on a rencontré une date < CUTOFF_YEAR
                   (ce qui veut dire qu'on a fini tous les PR 2025).
    """
    rows = []
    hit_older = False

    text = soup.get_text("\n")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # On démarre à partir de "News archive"
    try:
        start = lines.index("News archive") + 1
    except ValueError:
        return rows, hit_older

    i = start
    while i < len(lines) - 1:
        line = lines[i]
        if DATE_RE.match(line):
            date_text = line
            title = lines[i + 1].strip()
            dt = parse_date(date_text)

            if dt.year >= CUTOFF_YEAR:
                slug = slugify(title)
                # ancien format: f"{BASE_URL}#{slug}"
                #if page_index == 1:
                #    link = f"{BASE_URL}?article={slug}"
                #else:
                link = f"{BASE_URL}?page={page_index}&article={slug}"
                link = normalize_bce_link(link)
                rows.append((dt, title, link))
                i += 2
                continue
            else:
                # On vient de tomber dans 2024 → on arrête complètement
                hit_older = True
                break
        i += 1

    return rows, hit_older


def scrape_bce():
    print("[BCE] >>> __main__ block reached")
    _, existing_keys = load_master(MASTER_CSV)

    all_rows = []
    page = 1
    while True:
        if page == 1:
            url = BASE_URL
        else:
            url = f"{BASE_URL}?page={page}"

        print(f"[BCE] --- PAGE {page} --- URL: {url}")
        resp = requests.get(url, headers=HEADERS, timeout=30)
        print(f"[BCE] Status code: {resp.status_code}")
        if resp.status_code != 200:
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        latest_rows = extract_latest_news(soup) if page == 1 else []
        archive_rows, hit_older = extract_news_archive(soup, page)

        page_rows = latest_rows + archive_rows
        if not page_rows:
            print("[BCE] No rows on this page → stop.")
            break

        oldest_on_page = min(r[0] for r in page_rows)
        print(f"[BCE] Found {len(page_rows)} rows on this page")
        print(f"[BCE] Oldest on page {page}: {oldest_on_page.date()}")

        all_rows.extend(page_rows)

        if hit_older:
            print(f"[BCE] Hit first archive older than {CUTOFF_YEAR} on page {page} → stop.")
            break

        page += 1

    # déduplication intra-scraper (certaines news reviennent sur plusieurs pages)
    unique = {}
    for dt, title, link in all_rows:
        key = (title, dt.date())
        if key not in unique:
            unique[key] = (dt, title, link)

    print(f"[BCE] Unique rows for {CUTOFF_YEAR}+ after dedup: {len(unique)}")

    fetched_at = datetime.now(timezone.utc).isoformat()
    new_rows = []
    for dt, title, link in unique.values():
        key = (COMPANY, title, dt.date().isoformat())
        if key in existing_keys:
            continue
        row = {
            "id": str(uuid.uuid4()),
            "company": COMPANY,
            "title": title,
            "link": link,
            "date": dt.date().isoformat(),
            "fetched_at": fetched_at,
            "summary_ai": "",
            "impact_for_zhone": "",
        }
        new_rows.append(row)

    print(f"[BCE] Total rows >= cutoff not in master: {len(new_rows)}")
    if new_rows:
        append_rows(MASTER_CSV, new_rows)
        print(f"[BCE] Added to CSV: {len(new_rows)}")
    else:
        print("[BCE] All rows already in master, nothing new.")


if __name__ == "__main__":
    print(f"[BCE] Start scraper for {COMPANY} with cutoff year {CUTOFF_YEAR}")
    scrape_bce()
