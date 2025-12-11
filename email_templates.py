# email_templates.py
# HTML email templates for PressWatch (Zhone branding, unsubscribe link, proper title links)

import os
from datetime import datetime

# --- Branding & defaults -----------------------------------------------------

PRIMARY = "#0b2b6b"   # deep Zhone blue
ACCENT = "#1a73e8"    # link/button blue
BG_LIGHT = "#f6f8fb"  # background
TEXT = "#1f2937"      # dark text
MUTED = "#6b7280"     # muted gray

LOGO_URL = "https://zhone.com/wp-content/themes/dzs/dist/images/logo/logo.png"
PRESSWATCH_URL = os.getenv("DUMMY_URL", "https://customer-presswatch.streamlit.app/")
UNSUBSCRIBE_URL = os.getenv("DUMMY_URL", "https://customer-presswatch.streamlit.app/Unsubscribe")


# --- Styling helper ----------------------------------------------------------

def _base_css():
    return f"""
      body {{
        margin:0; padding:0; background:{BG_LIGHT}; color:{TEXT};
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      }}
      a {{ color:{ACCENT}; text-decoration:none; }}
      .container {{
        width:100%; max-width:700px; margin:0 auto; background:#ffffff;
        border-radius:10px; overflow:hidden; box-shadow:0 2px 10px rgba(0,0,0,.06);
      }}
      .header {{
        background:{PRIMARY}; color:#fff; padding:20px 28px; display:flex; align-items:center;
      }}
      .header h1 {{ margin:0; font-size:20px; font-weight:600; }}
      .logo {{ max-height:30px; vertical-align:middle; margin-right:14px; }}
      .content {{ padding:24px 28px; }}
      .pill {{ display:inline-block; padding:6px 10px; background:{BG_LIGHT}; border-radius:999px; color:{MUTED}; font-size:12px; }}
      .cta {{ display:inline-block; padding:12px 18px; background:{ACCENT}; color:#fff; border-radius:8px; font-weight:600; }}
      .list {{ margin:0; padding:0; list-style:none; }}
      .item {{ padding:14px 0; border-bottom:1px solid #eee; }}
      .item:last-child {{ border-bottom:none; }}
      .footer {{ color:{MUTED}; font-size:12px; padding:20px 28px 28px; text-align:center; }}
      .unsubscribe {{ display:inline-block; margin-top:14px; padding:10px 16px; background:#d9534f; color:#fff; border-radius:6px; font-weight:600; font-size:13px; }}
    """


def _header_html(title_left: str = "PressWatch Weekly"):
    return f"""
      <tr><td class="header">
        <img class="logo" src="{LOGO_URL}" alt="Zhone logo" width="120"
             style="display:block; max-width:120px; margin-right:14px;">
        <h1>{title_left}</h1>
      </td></tr>
    """


# --- Templates ---------------------------------------------------------------

def registration_thank_you(name: str, manage_link: str):
    css = _base_css()
    header = _header_html("Thanks for Subscribing")
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1"/><style>{css}</style></head>
      <body>
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
          <tr><td align="center" style="padding:24px;">
            <table class="container" role="presentation" cellspacing="0" cellpadding="0" border="0">
              {header}
              <tr><td class="content">
                <p class="pill">Welcome</p>
                <h2>Hello, {name}!</h2>
                <p>You’ll now receive weekly digests of competitor press releases curated by <strong>PressWatch.ai</strong>.</p>
                <p style="margin:18px 0;">
                  <a class="cta" href="{PRESSWATCH_URL}">Open Customer PressWatch</a>
                </p>
                <p style="font-size:13px; color:{MUTED};">Manage your preferences here:
                  <a href="{manage_link}">{manage_link}</a></p>
              </td></tr>
              <tr><td class="footer">
                Sent automatically by PressWatch.ai • {datetime.utcnow().strftime("%B %d, %Y")} UTC
              </td></tr>
            </table>
          </td></tr>
        </table>
      </body></html>
    """


def weekly_digest(company_updates: list, ai_summary: str, week_label: str):
    css = _base_css()
    header = _header_html(f"Customer Weekly Digest — {week_label}")

    # --- Build list of PRs ---
    if not company_updates:
        list_html = "<p>No new press releases this week.</p>"
    else:
        items_html = []
        for pr in company_updates:
            title = pr.get("title", "Untitled")
            url = pr.get("url", "#")
            source = pr.get("source", "")
            date_str = pr.get("date")
            if hasattr(date_str, "strftime"):
                date_str = date_str.strftime("%Y-%m-%d")
            meta = " • ".join(filter(None, [source, date_str]))
            items_html.append(
                f"""
                <li class="item">
                  <div style="font-weight:600; line-height:1.3;">
                    <a href="{url}" target="_blank">{title}</a>
                  </div>
                  <div style="color:{MUTED}; font-size:12px; margin-top:4px;">{meta}</div>
                </li>
                """
            )
        list_html = "\n".join(items_html)

    # --- Full HTML ---
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1"/><style>{css}</style></head>
      <body>
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0">
          <tr><td align="center" style="padding:24px;">
            <table class="container" role="presentation" cellspacing="0" cellpadding="0" border="0">
              {header}
              <tr><td class="content">
                <p class="pill">AI Summary</p>
                <div style="margin-top:10px; line-height:1.6;">{ai_summary}</div>
                <p style="margin:18px 0 6px;">
                  <a class="cta" href="{PRESSWATCH_URL}">Open Customer PressWatch</a>
                </p>
                <hr style="border:none; border-top:1px solid #eee; margin:24px 0;">
                <p class="pill">New Press Releases</p>
                <ul class="list" style="margin-top:10px;">
                  {list_html}
                </ul>
              </td></tr>
              <tr><td class="footer">
                Sent automatically by PressWatch.ai • {datetime.utcnow().strftime("%B %d, %Y")} UTC<br>
                <a class="unsubscribe" href="{UNSUBSCRIBE_URL}">Unsubscribe from Customer PressWatch</a>
              </td></tr>
            </table>
          </td></tr>
        </table>
      </body></html>
    """
