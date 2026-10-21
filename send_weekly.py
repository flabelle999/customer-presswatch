# send_weekly.py
# Builds the weekly digest, generates an AI summary (GROQ),
# pulls recipients from Google Sheets, and (optionally) sends via Microsoft Graph.

import csv
import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from dotenv import load_dotenv
import logging

import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from email_templates import weekly_digest


# === Config (env-driven) =====================================================

CSV_FILE = os.getenv("PR_CSV_PATH", "press_releases_master.csv")
SHEET_NAME = os.getenv("SUBSCRIBERS_SHEET_NAME", "Customer PressWatch Subscribers")
SHEET_WORKSHEET = os.getenv("SUBSCRIBERS_WORKSHEET", "Subscribers")
SENDER_UPN = os.getenv("MS_SENDER_UPN", "presswatch.ai@zhone.com")

# Azure AD App for Microsoft Graph
load_dotenv()
MS_TENANT_ID = os.getenv("MS_TENANT_ID")
MS_CLIENT_ID = os.getenv("MS_CLIENT_ID")
MS_CLIENT_SECRET = os.getenv("MS_CLIENT_SECRET")

# --- UNIVERSAL SECRET LOADER (LOCAL + GITHUB ACTIONS) ---
def load_google_service_json():
    # Try st.secrets first
    try:
        import streamlit as st
        path = st.secrets["google"]["service_json_path"]
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        pass

    # Fallback for GitHub Actions
    service_json_env = os.getenv("GCP_SERVICE_ACCOUNT_JSON")
    if not service_json_env:
        raise RuntimeError("GCP_SERVICE_ACCOUNT_JSON not set in GitHub Actions.")
    return json.loads(service_json_env)

# ✅ GLOBAL VARIABLE — used everywhere in your script
GOOGLE_SERVICE_JSON = load_google_service_json()

# GROQ (OpenAI-compatible)
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
# llama-3.3-70b-versatile was retired by Groq on 2026-08-16 — see console.groq.com/docs/deprecations
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
GROQ_URL = os.getenv("GROQ_URL", "https://api.groq.com/openai/v1/chat/completions")

# Options
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "7"))  # BRING BACK TO 7
REQUIRE_2025_PLUS = os.getenv("REQUIRE_2025_PLUS", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes")


# === Helpers =================================================================

def _parse_date_any(s: str):
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S", "%b %d, %Y"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except Exception:
            pass
    try:
        from dateutil import parser
        return parser.parse(s)
    except Exception:
        return None


def load_recent_press_releases(csv_path: str, days: int) -> List[Dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    out = []
    if not os.path.exists(csv_path):
        return out

    with open(csv_path, newline="", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            title = row.get("title") or row.get("headline") or row.get("Title")
            url = row.get("url") or row.get("link") or row.get("URL")
            source = row.get("source") or row.get("company") or row.get("site") or ""
            datestr = row.get("date") or row.get("published") or row.get("published_date") or row.get("Date")
            if not (title and url and datestr):
                continue
            dt = _parse_date_any(datestr)
            if not dt:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if REQUIRE_2025_PLUS and dt.year < 2025:
                continue
            if dt >= cutoff:
                out.append({"title": title.strip(), "url": url.strip(), "source": source.strip(), "date": dt})
    out.sort(key=lambda x: x["date"], reverse=True)
    return out


def _limit_sentences(text: str, max_sentences: int = 2) -> str:
    """Safety net in case the model ignores the length limit: keep the first N sentences."""
    import re
    flat = " ".join(text.split())  # collapse line breaks / extra paragraphs
    sentences = re.split(r"(?<=[.!?])\s+", flat)
    return " ".join(sentences[:max_sentences]).strip()


def groq_ai_summary(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "No new customer press releases were detected in the last week."

    if not GROQ_API_KEY:
        return "(AI summary unavailable — GROQ_API_KEY not configured in environment.)"
    
    try:
        messages = [
            {"role": "system", "content": (
            "You are summarizing press releases from Canadian telecom and broadband service providers "
            "(Zhone Technologies customers) for a weekly email digest. "
            "Write in a friendly, executive tone, as if explaining the news over a coffee break. "
            "Highlight only the most meaningful developments and avoid corporate jargon. "
            "Keep it digestible, engaging, and easy to skim. "
            "Focus on what happened and why it matters. "
            "LENGTH LIMIT: at most 2 sentences and 45 words in total, written as a single paragraph. "
            "Do not open with a greeting or filler such as 'Exciting week in telecom' — start directly with the news."
            )},
            {"role": "user", "content": "\n".join(
                [f"- {i['source']} — {i['title']} ({i['date'].strftime('%Y-%m-%d')}): {i['url']}" for i in items[:20]]
            )},
        ]
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        # gpt-oss is a reasoning model: reasoning tokens count against max_tokens,
        # so keep reasoning low and leave headroom for the visible answer.
        payload = {"model": GROQ_MODEL, "messages": messages, "temperature": 0.3, "max_tokens": 1500}
        if GROQ_MODEL.startswith("openai/gpt-oss"):
            payload["reasoning_effort"] = "low"
        r = requests.post(GROQ_URL, headers=headers, data=json.dumps(payload), timeout=60)
        if r.status_code != 200:
            print(f"::warning::Groq API error {r.status_code} (model={GROQ_MODEL}): {r.text[:500]}")
            r.raise_for_status()
        data = r.json()
        content = (data["choices"][0]["message"].get("content") or "").strip()
        if not content:
            print(f"::warning::Groq returned an empty summary (model={GROQ_MODEL}, "
                  f"finish_reason={data['choices'][0].get('finish_reason')})")
            return "AI summary unavailable"
        return _limit_sentences(content, 2)
    except Exception as e:
        print(f"::warning::AI summary failed: {e}")
        return "AI summary unavailable"


# --- MICROSOFT GRAPH ---------------------------------------------------------

def get_graph_token() -> str:
    """Acquire Graph access token using the same logic as testZhoneEmailApi.py."""
    logging.info("Acquiring access token from send_weekly.py...")

    token_url = f"https://login.microsoftonline.com/{MS_TENANT_ID}/oauth2/v2.0/token"

    data = {
        "grant_type": "client_credentials",
        "client_id": MS_CLIENT_ID,
        "client_secret": MS_CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
    }

    # 👇 Debug pour être sûr que les valeurs sont correctes
    print("DEBUG MS_TENANT_ID:", repr(MS_TENANT_ID))
    print("DEBUG MS_CLIENT_ID:", repr(MS_CLIENT_ID))
    print("DEBUG MS_CLIENT_SECRET length:", len(MS_CLIENT_SECRET))

    r = requests.post(token_url, data=data)  # pas de timeout pour le moment

    if r.status_code != 200:
        print("TOKEN ERROR STATUS:", r.status_code)
        print("TOKEN ERROR BODY:", r.text)     # ⬅ très important pour voir le vrai message Graph
        r.raise_for_status()

    token_info = r.json()
    access_token = token_info.get("access_token")
    if not access_token:
        raise RuntimeError(f"No access_token in response: {token_info}")

    logging.info("Access token acquired successfully in send_weekly.py")
    return access_token


def send_graph_html(subject: str, html: str, recipients: List[str], save_to_sent: bool = False):
    if DRY_RUN:
        print("🔸 Dry-run active — skipping Microsoft Graph send.")
        print(f"Would send to {len(recipients)} recipients")
        return

    if not recipients:
        print("No recipients to send to; skipping email.")
        return

    token = get_graph_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    to_recipients = [{"emailAddress": {"address": addr}} for addr in recipients]
    message = {
        "message": {
            "subject": subject,
            "body": {"contentType": "HTML", "content": html},
            "toRecipients": to_recipients,
        },
        "saveToSentItems": "true" if save_to_sent else "false",
    }

    url = f"https://graph.microsoft.com/v1.0/users/{SENDER_UPN}/sendMail"
    r = requests.post(url, headers=headers, data=json.dumps(message), timeout=60)

    if r.status_code in (429, 500, 502, 503, 504):
        time.sleep(3)
        r = requests.post(url, headers=headers, data=json.dumps(message), timeout=60)

    if r.status_code not in (202, 200):
        raise RuntimeError(f"Graph sendMail failed: {r.status_code} {r.text}")


# --- GOOGLE SHEETS (MATCHING REGISTRATION.PY!) -------------------------------

def get_subscriber_emails() -> List[str]:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(GOOGLE_SERVICE_JSON, scope)
    gc = gspread.authorize(creds)

    sh = gc.open(SHEET_NAME)

    try:
        ws = sh.worksheet(SHEET_WORKSHEET)
    except:
        ws = sh.sheet1

    rows = ws.get_all_records()

    emails = []
    for r in rows:
        email = r.get("email") or r.get("Email")
        status_raw = str(r.get("active") or r.get("Active") or "true").strip().lower()
        active = status_raw in ("1", "true", "yes", "active")
        if email and active:
            emails.append(email.strip())

    return sorted(set(emails))


# --- MAIN --------------------------------------------------------------------

def main():
    prs = load_recent_press_releases(CSV_FILE, WINDOW_DAYS)
    summary = groq_ai_summary(prs)
    print(f"{len(prs)} press release(s) in the last {WINDOW_DAYS} days. AI summary: {summary}")

    week_label = f"{(datetime.utcnow() - timedelta(days=WINDOW_DAYS)).strftime('%b %d')}–{datetime.utcnow().strftime('%b %d, %Y')}"
    items = [{"title": p["title"], "url": p["url"], "source": p["source"], "date": p["date"]} for p in prs]
    html = weekly_digest(items, summary, week_label)

    recipients = get_subscriber_emails()

    subject = f"Customer PressWatch Weekly Digest — {week_label}"
    send_graph_html(subject, html, recipients, save_to_sent=False)

    print(f"{'Would send' if DRY_RUN else 'Sent'} to {len(recipients)} subscribers.")


if __name__ == "__main__":
    main()
