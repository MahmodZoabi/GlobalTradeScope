"""
utils/styles.py — Premium CSS injection for GlobalTradeScope dashboard.

Call inject_css() once per page, immediately after st.set_page_config().
"""

import streamlit as st

_ACCENT = "#0F52BA"
_SIDEBAR_TOP = "#0D1B2E"
_SIDEBAR_BTM = "#162542"

_CSS = f"""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">

<style>
/* ── Global typography ─────────────────────────────────────────────────── */
html, body, [class*="css"] {{
    font-family: 'Inter', sans-serif;
}}
h1 {{
    font-size: 1.75rem !important;
    font-weight: 700 !important;
    color: #0F172A !important;
    letter-spacing: -0.02em;
}}
h2 {{
    font-size: 1.25rem !important;
    font-weight: 600 !important;
    color: #1E293B !important;
}}
h3 {{
    font-size: 1rem !important;
    font-weight: 600 !important;
    color: #1E293B !important;
    padding-left: 0.65rem;
    border-left: 3px solid {_ACCENT};
    margin-top: 1.25rem !important;
}}

/* ── Layout / spacing ──────────────────────────────────────────────────── */
#MainMenu, footer, [data-testid="stHeader"] {{
    display: none !important;
}}
.block-container {{
    padding-top: 1.5rem !important;
    padding-bottom: 2rem !important;
    max-width: 1280px;
}}

/* ── Sidebar ───────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {{
    background: linear-gradient(160deg, {_SIDEBAR_TOP} 0%, {_SIDEBAR_BTM} 100%) !important;
}}
[data-testid="stSidebar"] * {{
    color: #CBD5E1 !important;
}}
[data-testid="stSidebar"] h1,
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3,
[data-testid="stSidebar"] .stMarkdown strong {{
    color: #F1F5F9 !important;
}}
[data-testid="stSidebar"] a {{
    color: #93C5FD !important;
    text-decoration: none;
}}
[data-testid="stSidebar"] a:hover {{
    color: #BFDBFE !important;
    text-decoration: underline;
}}
[data-testid="stSidebar"] [data-testid="stSelectbox"] label,
[data-testid="stSidebar"] [data-testid="stSlider"] label,
[data-testid="stSidebar"] [data-testid="stRadio"] label,
[data-testid="stSidebar"] [data-testid="stMultiSelect"] label {{
    color: #94A3B8 !important;
    font-size: 0.75rem !important;
    font-weight: 500 !important;
    text-transform: uppercase;
    letter-spacing: 0.06em;
}}
[data-testid="stSidebar"] hr {{
    border-color: rgba(255,255,255,0.12) !important;
}}

/* ── Metric cards ──────────────────────────────────────────────────────── */
[data-testid="stMetric"] {{
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-top: 3px solid {_ACCENT};
    border-radius: 8px;
    padding: 1rem 1.25rem 0.85rem !important;
    box-shadow: 0 1px 4px rgba(15,82,186,0.07);
}}
[data-testid="stMetric"] label {{
    font-size: 0.72rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.07em;
    color: #64748B !important;
}}
[data-testid="stMetric"] [data-testid="stMetricValue"] {{
    font-size: 1.6rem !important;
    font-weight: 700 !important;
    color: #0F172A !important;
}}
[data-testid="stMetric"] [data-testid="stMetricDelta"] {{
    font-size: 0.8rem !important;
    font-weight: 500 !important;
}}

/* ── Plotly chart containers ───────────────────────────────────────────── */
[data-testid="stPlotlyChart"] {{
    border: 1px solid #E2E8F0;
    border-radius: 8px;
    box-shadow: 0 1px 6px rgba(0,0,0,0.05);
    overflow: hidden;
    background: #FFFFFF;
}}

/* ── Dataframe / table containers ─────────────────────────────────────── */
[data-testid="stDataFrame"],
[data-testid="stTable"] {{
    border: 1px solid #E2E8F0 !important;
    border-radius: 8px !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04) !important;
    overflow: hidden !important;
}}

/* ── Expander ──────────────────────────────────────────────────────────── */
[data-testid="stExpander"] {{
    border: 1px solid #E2E8F0 !important;
    border-radius: 8px !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.03) !important;
}}
[data-testid="stExpander"] summary {{
    font-weight: 600 !important;
    font-size: 0.875rem !important;
    color: #1E293B !important;
}}

/* ── Tabs ──────────────────────────────────────────────────────────────── */
[data-baseweb="tab-list"] {{
    border-bottom: 2px solid #E2E8F0 !important;
    gap: 0 !important;
}}
[data-baseweb="tab"] {{
    font-size: 0.875rem !important;
    font-weight: 500 !important;
    color: #64748B !important;
    padding: 0.5rem 1rem !important;
}}
[data-baseweb="tab-highlight"] {{
    background-color: {_ACCENT} !important;
    height: 2px !important;
}}

/* ── Info / warning / success callouts ────────────────────────────────── */
[data-testid="stAlert"] {{
    border-radius: 8px !important;
    border-width: 1px !important;
    font-size: 0.875rem !important;
}}

/* ── Buttons ───────────────────────────────────────────────────────────── */
[data-testid="baseButton-secondary"] {{
    border-radius: 6px !important;
    font-weight: 500 !important;
    font-size: 0.875rem !important;
}}

/* ── Top navigation bar ────────────────────────────────────────────────── */
/* Non-active links */
[data-testid="stPageLink"] a {{
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    color: #475569 !important;
    text-decoration: none !important;
    display: block !important;
    text-align: center !important;
    padding: 0.35rem 0.25rem !important;
    border-radius: 6px !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
}}
[data-testid="stPageLink"] a:hover {{
    background: #F1F5F9 !important;
    color: {_ACCENT} !important;
}}
/* Active / current page — Streamlit renders disabled page_link as a <p> */
[data-testid="stPageLink"] p {{
    font-size: 0.85rem !important;
    font-weight: 700 !important;
    color: {_ACCENT} !important;
    text-align: center !important;
    border-bottom: 2px solid {_ACCENT};
    padding-bottom: 2px !important;
    cursor: default !important;
    margin: 0 auto !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
}}

/* ── Mobile responsive (≤ 768 px) ──────────────────────────────────────── */
@media (max-width: 768px) {{
    /* Hide sidebar; top nav is the only navigation on small screens */
    [data-testid="stSidebar"],
    [data-testid="collapsedControl"] {{
        display: none !important;
    }}
    /* Tighter page padding */
    .block-container {{
        padding-left: 0.75rem !important;
        padding-right: 0.75rem !important;
        padding-top: 1rem !important;
    }}
    /* Scale down headings */
    h1 {{ font-size: 1.3rem !important; }}
    h2 {{ font-size: 1.1rem !important; }}
    h3 {{ font-size: 0.9rem !important; }}
    /* Metric value size */
    [data-testid="stMetric"] [data-testid="stMetricValue"] {{
        font-size: 1.2rem !important;
    }}
    /* Allow metric cards to wrap into a 2-column grid */
    [data-testid="stHorizontalBlock"] {{
        flex-wrap: wrap !important;
        gap: 0.5rem !important;
    }}
    [data-testid="column"] {{
        min-width: 45% !important;
        flex: 1 1 45% !important;
    }}
    /* Nav bar: allow horizontal scroll if labels don't fit */
    [data-testid="stHorizontalBlock"]:has([data-testid="stPageLink"]) {{
        flex-wrap: nowrap !important;
        overflow-x: auto !important;
    }}
}}
</style>
"""


def inject_css() -> None:
    """Inject premium CSS into the current Streamlit page."""
    st.markdown(_CSS, unsafe_allow_html=True)
