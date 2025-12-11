import os
import pandas as pd
import plotly.express as px
import streamlit as st
import chardet
import gspread
import json
from streamlit_plotly_events import plotly_events
from datetime import date
from oauth2client.service_account import ServiceAccountCredentials

# --- Google Sheets Setup

SHEET_NAME = "PressWatch Subscribers"

def get_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(st.secrets["google"], scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).sheet1
    return sheet

# --- Handle unsubscribe page ---
query = st.query_params  # new Streamlit syntax
page = query.get("page")
email = query.get("email")

if page == "unsubscribe":
    st.title("Unsubscribe from PressWatch Weekly Digest")

    if not email:
        st.error("No email address provided in the link.")
    else:
        st.write(f"Email detected: **{email}**")
        sheet = get_sheet()
        all_emails = [row[2] for row in sheet.get_all_values()]  # assuming col C = email

        if email in all_emails:
            cell = sheet.find(email)
            sheet.delete_rows(cell.row)
            st.success("✅ You’ve been successfully unsubscribed.")
        else:
            st.warning("⚠️ This email was not found in the subscriber list.")

    st.markdown(
        "If this was a mistake, you can [resubscribe here](https://presswatch.streamlit.app/)"
    )
    st.stop()

# -------------------------------
# Page configuration
# -------------------------------
st.set_page_config(
    page_title="Customer Press Release Radar",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -------------------------------
# CSS Styling
# -------------------------------
st.markdown("""
<style>
body { background-color: #fafafa; }
.sidebar .block-container { width: 260px !important; }  /* narrower sidebar */

.metric-container {
    display: flex;
    justify-content: space-between;
    gap: 0.8rem;
    margin-top: 0.5rem;
    margin-bottom: 1rem;
}
.metric-card {
    flex: 1;
    background: linear-gradient(180deg, #ffffff 0%, #f8f9fb 100%);
    border: 1px solid rgba(0,0,0,0.05);
    border-radius: 12px;
    padding: 10px 12px;
    box-shadow: 0 2px 6px rgba(0,0,0,0.05);
    text-align: center;
}
.metric-title { font-size: 0.8rem; color: #666; margin-bottom: 4px; font-weight: 500; }
.metric-value { font-size: 1.2rem; font-weight: 600; color: #222; }

/* Detail card */
.detail-card {
    padding: 22px;
    border-radius: 14px;
    color: white;
    margin-top: 25px;
    animation: fadeIn 0.7s ease-in-out;
    box-shadow: 0 4px 12px rgba(0,0,0,0.2);
}
@keyframes fadeIn {
    from {opacity: 0; transform: translateY(10px);}
    to {opacity: 1; transform: translateY(0);}
}
.detail-card h3 { margin-top: 0; margin-bottom: 10px; font-weight: 700; }
.detail-card p { line-height: 1.5em; }
.detail-card a { color: white; text-decoration: underline; }
</style>
""", unsafe_allow_html=True)

# -------------------------------
# Load CSV
# -------------------------------
@st.cache_data(show_spinner=False)
def load_data():
    csv_path = os.path.join(os.path.dirname(__file__), "press_releases_master.csv")

    if not os.path.exists(csv_path):
        st.error(f"CSV file not found: {csv_path}")
        st.stop()

    # --- Auto-detect encoding to avoid UTF-8 decode errors ---
    try:
        with open(csv_path, "rb") as f:
            raw_data = f.read(50000)
            enc = chardet.detect(raw_data)["encoding"] or "utf-8"
        df = pd.read_csv(csv_path, encoding=enc)
    except Exception as e:
        st.error(f"Error reading CSV: {e}")
        st.stop()

    #st.caption(f"✅ Loaded CSV with encoding: {enc}")

    # --- Quick sanity check ---
    if df.empty:
        st.error("CSV file is empty.")
        st.stop()

    # --- Normalize string fields ---
    if "company" not in df.columns:
        st.error("No 'company' column found in CSV.")
        st.stop()

    df["company"] = df["company"].astype(str).str.strip()
    if "title" in df.columns:
        df["title"] = df["title"].astype(str).str.strip()

    # --- Ensure date column exists ---
    if "date" not in df.columns:
        st.error("No 'date' column found in CSV.")
        st.stop()

    # --- Robust date normalization (multi-format merge) ---
    df["date"] = df["date"].astype(str).str.strip()

    possible_formats = [
        "%Y-%m-%d",           # 2025-03-05
        "%Y/%m/%d",           # 2025/03/05
        "%b %d, %Y",          # Mar 5, 2025
        "%B %d, %Y",          # March 5, 2025
        "%m/%d/%Y",           # 03/05/2025
        "%d-%b-%Y",           # 05-Mar-2025
        "%Y-%m-%dT%H:%M:%SZ", # 2025-03-05T00:00:00Z
    ]

    # Try all formats and combine valid results
    parsed_final = pd.Series(pd.NaT, index=df.index)

    for fmt in possible_formats:
        try:
            parsed = pd.to_datetime(df["date"], format=fmt, errors="coerce")
            parsed_final = parsed_final.combine_first(parsed)
        except Exception:
            continue

    df["date"] = parsed_final.dt.date

    # --- Report ---
    invalid_dates = df["date"].isna().sum()
    print(f"✅ Loaded {len(df)} rows; {invalid_dates} rows still unparsed (kept).")


    # --- Clean up NaN text fields ---
    for c in ["summary_ai", "impact_for_zhone"]:
        if c in df.columns:
            df[c] = df[c].astype(str).replace({"nan": "", "NaN": ""}).fillna("")

    # --- Final assurance log ---
    print("✅ load_data() executed successfully, returning dataframe.")
    print(f"Companies found: {df['company'].unique()}")

    # ✅ Always return df
    return df

df = load_data()

# -------------------------------
# Sidebar filters
# -------------------------------
with st.sidebar:
    st.header("🔎 Filters")

    companies = sorted(df["company"].dropna().unique())
    selected_companies = st.multiselect("Companies", companies, default=companies)

    valid_dates = df["date"].dropna()
    min_d, max_d = valid_dates.min(), valid_dates.max()
    selected_range = st.date_input("Date range", value=(min_d, max_d), min_value=min_d, max_value=max_d)

    query = st.text_input("Search text (title, summary, impact)", "")

# -------------------------------
# Filtering
# -------------------------------
def filter_df(df, companies, date_range, query):
    f = df.copy()
    if companies:
        f = f[f["company"].isin(companies)]
    if date_range and all(date_range):
        start, end = date_range
        f = f[(f["date"] >= start) & (f["date"] <= end)]
    if query:
        q = query.lower()
        f = f[
            f["title"].str.lower().str.contains(q, na=False)
            | f.get("summary_ai", "").str.lower().str.contains(q, na=False)
            | f.get("impact_for_zhone", "").str.lower().str.contains(q, na=False)
        ]
    return f.sort_values("date", ascending=False)

filtered = filter_df(df, selected_companies, selected_range, query)

# -------------------------------
# Header
# -------------------------------
st.title("🛰️ Press Release Radar")
st.caption("Click a point on the timeline below to view details.")

by_co = filtered.groupby("company")["id"].count().sort_values(ascending=False)
top_co = by_co.index[0] if not by_co.empty else "—"
earliest = filtered["date"].min()
latest = filtered["date"].max()

st.markdown(f"""
<div class="metric-container">
  <div class="metric-card"><div class="metric-title">Total Releases</div><div class="metric-value">{len(filtered):,}</div></div>
  <div class="metric-card"><div class="metric-title">Top Company</div><div class="metric-value">{top_co}</div></div>
  <div class="metric-card"><div class="metric-title">Earliest</div><div class="metric-value">{earliest if pd.notna(earliest) else '—'}</div></div>
  <div class="metric-card"><div class="metric-title">Latest</div><div class="metric-value">{latest if pd.notna(latest) else '—'}</div></div>
</div>
""", unsafe_allow_html=True)

# ==============================
# 📈 PRESS RELEASE TIMELINE — Accurate Clicks & Hover Tooltips
# ==============================
st.subheader("🗓 Press Release Timeline (clickable)")

# Ensure we have a unique ID for persistent mapping
tl = filtered.copy()
if "id" not in tl.columns:
    tl["id"] = tl.index.astype(str)
tl = tl.reset_index(drop=True)

COLOR_MAP = {
    "Bell": "#7c66ff",  # orange
    "TELUS": "#b95fa3",              # violet
}
company_order = [c for c in ["BCE", "TELUS"]
#company_order = [c for c in ["Zhone Technologies", "Adtran", "Nokia", "Calix","Ciena"]
                 if c in tl["company"].unique()]

@st.cache_data(show_spinner=False)
def build_timeline(data):
    fig = px.scatter(
        data,
        x="date",
        y="company",
        color="company",
        hover_name="title",
        custom_data=["id"],  # real persistent ID
        color_discrete_map=COLOR_MAP,
        category_orders={"company": company_order},
        template="plotly_white",
    )

    fig.update_traces(
        marker=dict(size=12, line=dict(width=0.5, color="rgba(0,0,0,.3)")),
        hovertemplate="<span style='font-size:16px; font-weight:bold;'>%{hovertext}</span><br>"
                      "<span style='font-size:14px;'>📅 %{x|%b %d, %Y}</span><extra></extra>",
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Company",
        showlegend=False,
        xaxis=dict(tickfont=dict(size=14)),
        yaxis=dict(tickfont=dict(size=14)),
        margin=dict(l=30, r=30, t=10, b=30),
        height=460,
        hoverlabel=dict(
            bgcolor="rgba(0,0,0,0.85)",
            font_size=15,
            font_color="white",
            bordercolor="white"
        ),
    )
    return fig

# Build figure once (cached)
fig = build_timeline(tl)

# Capture hover for tooltip display, click for selection
events = plotly_events(
    fig,
    click_event=True,
    #hover_event=True,
    override_height=460,
    key="timeline_plot"
)

#st.write("🔍 Debug event payload:", events)

# =====================================
# 🗂 DETAILED INFO — compatible with your event payload
# =====================================
if "selected_row" not in st.session_state:
    st.session_state.selected_row = None

clicked_row = None
if events and isinstance(events, list) and len(events) > 0:
    e = events[0]
    clicked_date = str(e.get("x"))
    clicked_company = e.get("y")

    # Match by date + company
    match = tl[
        (tl["company"] == clicked_company)
        & (tl["date"].astype(str) == clicked_date)
    ]
    if not match.empty:
        clicked_row = match.iloc[0]
        st.session_state.selected_row = clicked_row

row = st.session_state.selected_row

# =====================================
# 🪧 DISPLAY SELECTED CARD
# =====================================
if row is not None:
    company = row["company"]
    title = row["title"]
    date_val = row["date"]
    link = row.get("link", "#")
    ai = (row.get("summary_ai", "") or "").strip() or "Not available."
    impact = (row.get("impact_for_zhone", "") or "").strip() or "Not available."
    bg = COLOR_MAP.get(company, "#444")

    st.markdown(
        f"""
        <div style="
            background-color:{bg};
            padding:20px;border-radius:12px;color:white;
            box-shadow:0 4px 10px rgba(0,0,0,.2); margin-top:16px;
        ">
          <h4 style="color:white;">📄 <b>{company} — {title}</b></h4>
          <p><b>Date:</b> {date_val}</p>
          <p><b>AI Summary:</b> {ai}</p>
          <p><b>Impact for Zhone Technologies:</b> {impact}</p>
          <a href="{link}" target="_blank" style="color:white;text-decoration:underline;">
            🔗 Read full press release
          </a>
        </div>
        """,
        unsafe_allow_html=True,
    )
else:
    st.info("Click a bubble on the timeline to view its details below.")
