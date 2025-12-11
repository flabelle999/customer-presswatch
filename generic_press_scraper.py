# generic_press_scraper.py
# ------------------------------------------------------------
# Scraper générique pour portails "Press Releases" :
# - Stratégies : RSS/Atom -> JSON-LD (NewsArticle) -> Heuristiques HTML
# - Pagination générique (?page=, ?Page=, ?p=, ?start=)
# - Filtrage par date (>= cutoff, par défaut 2025-01-01)
# - Fallback Selenium pour sites JS (Huawei, ZTE)
# - Profil Selenium spécial Calix (Load more) avec arrêt dès < 2025
#
# Dépendances à ajouter dans requirements.txt :
#   requests
#   beautifulsoup4
#   pandas
#   feedparser
#   dateparser
#   selenium==4.25.0
#   webdriver-manager
#
# Sur GitHub Actions : installer Google Chrome (étape apt-get).
# ------------------------------------------------------------

import os, re, json, time, uuid
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
from datetime import datetime

# Optionnels mais recommandés pour une meilleure robustesse
try:
    import feedparser   # pip install feedparser
except Exception:
    feedparser = None
try:
    import dateparser   # pip install dateparser
except Exception:
    dateparser = None

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

MASTER_FILE = os.path.join(os.path.dirname(__file__), "press_releases_master.csv")

# -------------------- CSV HELPERS --------------------

def load_master():
    if os.path.exists(MASTER_FILE):
        try:
            return pd.read_csv(MASTER_FILE, encoding="utf-8")
        except UnicodeDecodeError:
            return pd.read_csv(MASTER_FILE, encoding="latin-1")
    return pd.DataFrame(columns=["id","company","title","link","date","fetched_at"])

def save_to_master(rows, company):
    """rows: list[{'title','link','date'}] → append uniques (company,title)."""
    df_new = pd.DataFrame(
        [{
            "id": str(uuid.uuid4()),
            "company": company,
            "title": r.get("title","").strip(),
            "link": r.get("link","").strip(),
            "date": r.get("date","").strip(),
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        } for r in rows if r.get("title") and r.get("link")],
        columns=["id","company","title","link","date","fetched_at"]
    )
    if df_new.empty:
        print(f"ℹ️ No new rows for {company}.")
        return 0

    master = load_master()
    if master.empty:
        out = df_new
    else:
        merged = pd.merge(df_new, master[["company","title"]],
                          on=["company","title"], how="left", indicator=True)
        only_new = merged[merged["_merge"]=="left_only"].drop(columns="_merge")
        if only_new.empty:
            print(f"ℹ️ No unique rows to add for {company}.")
            return 0
        out = pd.concat([master, only_new[df_new.columns]], ignore_index=True)

    out.to_csv(MASTER_FILE, index=False, encoding="utf-8-sig")
    print(f"✅ Added {len(df_new)} {company} press releases to {MASTER_FILE}")
    return len(df_new)

# -------------------- HTTP + DATES --------------------

def http_get(url, timeout=25, headers=None):
    h = {"User-Agent": UA, "Accept-Language": "en,fr;q=0.9", "Accept": "*/*"}
    if headers: h.update(headers)
    for i in range(3):
        r = requests.get(url, headers=h, timeout=timeout)
        if r.status_code == 200:
            return r
        time.sleep(1.5 * (i+1))
    r.raise_for_status()

def norm_date(s):
    s = (s or "").strip()
    # Formats fréquents
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d %B %Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    # Fallback
    if dateparser:
        dt = dateparser.parse(s, languages=["en","fr"], settings={"DATE_ORDER":"DMY"})
        if dt:
            return dt.strftime("%Y-%m-%d")
    return ""

# -------------------- STRATEGIES (RSS / JSON-LD / HTML) --------------------

def from_rss_or_atom(list_url, soup=None):
    if not feedparser:
        return []
    if soup is None:
        soup = BeautifulSoup(http_get(list_url).text, "html.parser")
    out = []
    for lk in soup.select("link[rel='alternate'][type*='rss'], link[rel='alternate'][type*='atom']"):
        feed_url = urljoin(list_url, lk.get("href"))
        d = feedparser.parse(feed_url)
        for e in d.entries:
            title = (e.get("title") or "").strip()
            link  = e.get("link")
            date  = ""
            for k in ("published","updated","pubDate"):
                if getattr(e, k, None):
                    date = norm_date(str(getattr(e, k)))
                    break
            out.append({"title": title, "link": link, "date": date})
        if out:
            break
    return out

def from_jsonld(list_url, html=None):
    if html is None:
        html = http_get(list_url).text
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string)
        except Exception:
            continue
        stack = data if isinstance(data, list) else [data]
        for obj in stack:
            if not isinstance(obj, dict):
                continue
            typ = obj.get("@type")
            if typ in ("NewsArticle","Article","Report"):
                title = (obj.get("headline") or obj.get("name") or "").strip()
                link  = obj.get("url") or obj.get("mainEntityOfPage")
                date  = obj.get("datePublished") or obj.get("dateModified") or ""
                items.append({"title": title, "link": urljoin(list_url, str(link) if link else ""), "date": norm_date(date)})
            # collections possibles
            for k in ("itemListElement","hasPart","about","mainEntity"):
                v = obj.get(k)
                if isinstance(v, list):
                    for it in v:
                        if isinstance(it, dict) and it.get("@type") in ("NewsArticle","Article"):
                            title = (it.get("headline") or it.get("name") or "").strip()
                            link  = it.get("url") or it.get("mainEntityOfPage")
                            date  = it.get("datePublished") or ""
                            items.append({"title": title, "link": urljoin(list_url, str(link) if link else ""), "date": norm_date(date)})
    # dédup
    uniq = {}
    for i in items:
        k = (i["title"], i["link"])
        if k not in uniq:
            uniq[k] = i
    return list(uniq.values())

CONTAINER_CANDIDATES = [
    "article",
    "div.card",
    "div.cmp-card",
    "div.cmp-card__dynamic",
    "div.cmp-listing__item",
    "div.news-item",
    "div.gt-listing-item",
    "div.sppb-article-info-wrap",
    "div.sppb-addon-articles",
    "div.views-row",
    "dd.item-txt",                       # for some ZTE-like layouts
    "div.mc-list-item-wrapper",
    "div.pp_block-item-container",
    "div.bg-white.rounded-2xl",         # Bell / BCE newsroom cards
]
TITLE_CANDIDATES = [
    "h3.title",
    "h2.title",
    "h3.cmp-card__title",
    "h1",
    "h2",
    "h3",
    "h4",
    '[id^="heading-"]',     # Bell: <span id="heading-...">
    '[role="heading"]',
    "a[title]",
    "a",
]
DATE_CANDIDATES = [
    "time[datetime]", "time", "span.date", "div.date", "span.sppb-meta-date",
    "div.gt-listing-item-date", "span.pp-item-date-city-wrapper", "div.dates"
]
LINK_CANDIDATES = [
    "a.cmp-card__link", "a.card-link", "a.link-wrap",
    "a.gt-listing-item-overlay-link", "a"
]

def text_or_none(el):
    return el.get_text(" ", strip=True) if el else ""

def first(sel_list, root):
    for s in sel_list:
        el = root.select_one(s)
        if el:
            return el
    return None

def from_html_list(list_url, html=None):
    if html is None:
        html = http_get(list_url).text
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for cont_sel in CONTAINER_CANDIDATES:
        for item in soup.select(cont_sel):
            # Skip obvious navigation / footer / breadcrumb elements
            if item.find_parent(["nav", "header", "footer"]):
                continue
            classes = " ".join(item.get("class", [])).lower()
            if any(k in classes for k in ("nav", "menu", "breadcrumb", "footer", "header")):
                continue

            a = first(LINK_CANDIDATES, item)
            if not a:
                continue
            href = a.get("href") or ""
            link = urljoin(list_url, href)

            h = first(TITLE_CANDIDATES, item) or a
            title = text_or_none(h)
            if not title or title.lower() in ("learn more", "read more"):
                title = a.get("title") or title
            title = (title or "").strip()
            if not title:
                continue

            d = first(DATE_CANDIDATES, item)
            date = norm_date(text_or_none(d))

            items.append({"title": title, "link": link, "date": date})
        if items:
            break
    # dédup + filtrage
    clean, seen = [], set()
    for it in items:
        if len(it["title"]) < 4:
            continue
        # Drop single-word nav links like "Accessibility"
        if len(it["title"].split()) == 1:
            continue
        k = (it["title"], it["link"])
        if k in seen:
            continue
        seen.add(k)
        clean.append(it)
    return clean

# -------------------- PAGINATION --------------------

def next_page_url(list_url, page, mode_hint=None):
    """
    Essaie des schémas de pagination :
    ?page=2, ?Page=2, ?p=2, ?start=16 (offset), etc.
    """
    u = urlparse(list_url)
    q = parse_qs(u.query)

    if "start" in q:
        step = int(q["start"][0] or 0) or 16
        q["start"] = [str(page * step)]
    elif any(k in q for k in ("page","Page","p")):
        key = "page" if "page" in q else ("Page" if "Page" in q else "p")
        q[key] = [str(page)]
    else:
        q["page"] = [str(page)]
    new_q = urlencode({k: v[0] for k, v in q.items()})
    return urlunparse(u._replace(query=new_q))

# -------------------- SELENIUM (FALLBACK) --------------------

# Imports Selenium (protégés pour les environnements sans Selenium)
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        _USE_WDM = True
    except Exception:
        _USE_WDM = False
    _SEL_OK = True
except Exception:
    _SEL_OK = False

def setup_driver():
    chrome_opts = Options()
    chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--disable-blink-features=AutomationControlled")
    chrome_opts.add_argument("--window-size=1920,1080")
    chrome_opts.add_argument("--lang=en-US")
    chrome_opts.add_argument("user-agent=" + UA)
    if _USE_WDM:
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_opts)
    else:
        driver = webdriver.Chrome(options=chrome_opts)
    return driver

# -------- Profil Calix (Load more) --------

_CALIX_MONTH_RE = re.compile(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}", re.I)

def _calix_extract_from_dom(base_url: str, html: str):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for card in soup.select("div.cmp-card, div.cmp-card.cmp-card--dynamic"):
        a = card.select_one("span.cmp-card__title a, a[href*='/press-release/']")
        if not a:
            continue
        link = urljoin(base_url, a.get("href",""))

        h = card.select_one("span.cmp-card__title, h3, h2")
        title = (h.get_text(" ", strip=True) if h else a.get_text(" ", strip=True)).strip()
        if not title or title.lower() in ("learn more","read more"):
            title = a.get("title") or title
        title = (title or "").strip()
        if not title:
            continue

        date_txt = ""
        info = card.select_one(".cmp-card__info, .cmp-card__footer, .cmp-card__meta")
        if info:
            m = _CALIX_MONTH_RE.search(info.get_text(" ", strip=True))
            if m:
                date_txt = m.group(0)

        items.append({"title": title, "link": link, "date": norm_date(date_txt)})
    return items

def _calix_fill_missing_dates_with_article(driver, rows):
    out = []
    for r in rows:
        if r.get("date"):
            out.append(r)
            continue
        try:
            driver.get(r["link"])
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            soup = BeautifulSoup(driver.page_source, "html.parser")
            body_txt = soup.get_text(" ", strip=True)
            m = _CALIX_MONTH_RE.search(body_txt)
            if m:
                r["date"] = norm_date(m.group(0))
        except Exception:
            pass
        out.append(r)
    return out

def _scrape_calix_load_more(driver, list_url: str, cutoff="2025-01-01", max_clicks=80):
    cutoff_dt = datetime.strptime(cutoff, "%Y-%m-%d")
    driver.get(list_url)
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

    all_items, clicks, stop = [], 0, False

    while not stop and clicks <= max_clicks:
        html = driver.page_source
        rows = _calix_extract_from_dom(list_url, html)
        need_fill = [r for r in rows if not r.get("date")]
        if need_fill:
            rows = _calix_fill_missing_dates_with_article(driver, rows)

        # Arrêt si on croise < 2025 via URL ou via date
        for it in rows:
            link_year = None
            m = re.search(r"/(20\d{2})/", it["link"])
            if m:
                try: link_year = int(m.group(1))
                except Exception: link_year = None
            if link_year and link_year < 2025:
                stop = True
                break
            if it.get("date"):
                try:
                    if datetime.strptime(it["date"], "%Y-%m-%d") < cutoff_dt:
                        stop = True
                        break
                except Exception:
                    pass

        all_items.extend(rows)
        ded = {}
        for i in all_items:
            k = (i["title"], i["link"])
            if k not in ded or (not ded[k].get("date") and i.get("date")):
                ded[k] = i
        all_items = list(ded.values())

        if stop:
            break

        # Clic "Load more"
        try:
            btn = WebDriverWait(driver, 6).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//button[contains(., 'Load More') or contains(., 'Load more') or contains(., 'Load more articles')]"
                    " | //a[contains(., 'Load More') or contains(., 'Load more')]"
                ))
            )
            driver.execute_script("arguments[0].click();", btn)
            clicks += 1
            time.sleep(1.2)
        except Exception:
            break

    # Garder uniquement >= cutoff
    kept, seen = [], set()
    for it in all_items:
        if it.get("date"):
            try:
                if datetime.strptime(it["date"], "%Y-%m-%d") < cutoff_dt:
                    continue
            except Exception:
                pass
        k = (it["title"], it["link"])
        if k in seen:
            continue
        seen.add(k)
        kept.append(it)
    return kept

# -------- Raccourcis Huawei / ZTE sur DOM rendu --------

def _from_html_known_js_sites(base_url, html):
    soup = BeautifulSoup(html, "html.parser")
    items = []
    netloc = urlparse(base_url).netloc.lower()

    # Huawei news listing
    if "huawei.com" in netloc:
        for it in soup.select("div.video-list-item"):
            a = it.select_one("a.c-box[href], a[href]")
            t = it.select_one("h4.js-text-dot-en") or a
            d = it.select_one("div.time")
            if not a or not t:
                continue
            title = text_or_none(t)
            if not title:
                continue
            link = urljoin(base_url, a.get("href") or "")
            date = norm_date(text_or_none(d))
            items.append({"title": title, "link": link, "date": date})

    # ZTE listing
    if "zte.com" in netloc:
        for it in soup.select("dd.item-txt"):
            a = it.find_parent("a")
            t = it.select_one("h4.ellipsis-3") or (a if a else None)
            d = it.select_one("span.date")
            if not a or not t:
                continue
            title = text_or_none(t)
            if not title:
                continue
            link = urljoin(base_url, a.get("href") or "")
            date = norm_date(text_or_none(d))
            items.append({"title": title, "link": link, "date": date})

    return items

def scrape_with_selenium(list_url, cutoff="2025-01-01", max_pages=10):
    if not _SEL_OK:
        return []

    cutoff_dt = datetime.strptime(cutoff, "%Y-%m-%d")
    out = []
    driver = setup_driver()
    try:
        netloc = urlparse(list_url).netloc.lower()
        # Profil Calix (Load more) : on traite la page unique et on retourne
        if "calix.com" in netloc:
            try:
                return _scrape_calix_load_more(driver, list_url, cutoff=cutoff, max_clicks=80)
            finally:
                pass

        for p in range(1, max_pages + 1):
            url = list_url if p == 1 else next_page_url(list_url, p)
            driver.get(url)
            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            except Exception:
                pass
            html = driver.page_source

            # Heuristique générique HTML sur DOM rendu
            items = from_html_list(url, html)
            # Profils rapides Huawei/ZTE
            if not items:
                items = _from_html_known_js_sites(url, html)

            if not items:
                break

            # garder >= cutoff
            keep = []
            for it in items:
                dt = None
                if it.get("date"):
                    try: dt = datetime.strptime(it["date"], "%Y-%m-%d")
                    except: dt = None
                if dt and dt < cutoff_dt:
                    continue
                keep.append(it)
            out.extend(keep)

            # stop si on a croisé une date < cutoff
            dated = [i["date"] for i in items if i.get("date")]
            if dated:
                try:
                    if min(datetime.strptime(d, "%Y-%m-%d") for d in dated) < cutoff_dt:
                        break
                except Exception:
                    pass

            time.sleep(0.6)
    finally:
        driver.quit()

    # dédup
    dedup = {}
    for i in out:
        k = (i["title"], i["link"])
        if k not in dedup:
            dedup[k] = i
        elif not dedup[k].get("date") and i.get("date"):
            dedup[k] = i
    return list(dedup.values())

# -------------------- ORCHESTRATION --------------------

def scrape_press_releases(list_url, cutoff="2025-01-01", max_pages=10):
    """
    Retourne list[{title, date, link}] pour list_url, avec pagination et cutoff.
    Ordre : RSS/Atom -> JSON-LD -> Heuristiques HTML -> (fallback) Selenium.
    """
    cutoff_dt = datetime.strptime(cutoff, "%Y-%m-%d")
    out = []
    any_found = False

    def keep_only_newer(items):
        kept = []
        for it in items:
            dt = None
            if it.get("date"):
                try: dt = datetime.strptime(it["date"], "%Y-%m-%d")
                except: dt = None
            if dt and dt < cutoff_dt:
                continue
            kept.append(it)
        return kept

    for p in range(1, max_pages+1):
        url = list_url if p == 1 else next_page_url(list_url, p)
        try:
            r = http_get(url)
        except Exception:
            break
        html = r.text
        soup = BeautifulSoup(html, "html.parser")

        items = from_rss_or_atom(url, soup) or []
        if not items:
            items = from_jsonld(url, html) or []
        if not items:
            items = from_html_list(url, html) or []

        if not items:
            # si page 1 est vide → on tentera Selenium après la boucle
            if p == 1:
                break
            else:
                break

        any_found = True
        items = keep_only_newer(items)
        out.extend(items)

        alldates = [i.get("date") for i in items if i.get("date")]
        if alldates:
            try:
                oldest = min(datetime.strptime(d, "%Y-%m-%d") for d in alldates)
                if oldest < cutoff_dt:
                    break
            except Exception:
                pass
        time.sleep(0.6)

    if not any_found:
        # Fallback Selenium (sites JS)
        try:
            sel_rows = scrape_with_selenium(list_url, cutoff=cutoff, max_pages=max_pages)
            return sel_rows
        except Exception:
            return []

    # dédup finale
    dedup = {}
    for i in out:
        k = (i["title"], i["link"])
        if k not in dedup:
            dedup[k] = i
        elif not dedup[k].get("date") and i.get("date"):
            dedup[k] = i
    return list(dedup.values())

# -------------------- EXEMPLE D’UTILISATION --------------------

if __name__ == "__main__":
    # Ajoute/retire des sources librement
    sources = {
        "Bell":        "https://www.bce.ca/news-and-media/newsroom",
        "TELUS":       "https://www.telus.com/en/about/newsroom",
        # Sites JS → Selenium fallback auto :
    }

    CUTOFF = "2025-01-01"

    for company, url in sources.items():
        print(f"\n==== {company}: {url} ====")
        rows = scrape_press_releases(url, cutoff=CUTOFF, max_pages=8)
        for r in rows[:5]:
            print(" -", r.get("date",""), r.get("title",""), "→", r.get("link",""))
        print(f"Total found (>= {CUTOFF}): {len(rows)}")
        added = save_to_master(rows, company)
        print(f"Added to CSV: {added}")
