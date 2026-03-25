"""
app.py — GlobalTradeScope Streamlit entry point (Home page)
"""

import streamlit as st

from utils.constants import APP_TITLE, fmt_usd
from utils.db import query
from utils.styles import inject_css

st.set_page_config(
    page_title=APP_TITLE,
    page_icon=None,
    layout="wide",
)
inject_css()

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("GlobalTradeScope")
    st.caption("Israel bilateral trade intelligence · 2014 – 2024")
    st.divider()
    st.caption("Built by Mahmod Zoubi")
    st.markdown("[GitHub](https://github.com/MahmodZoabi) · [LinkedIn](https://www.linkedin.com/in/mahmod-zoabi/)")

# ---------------------------------------------------------------------------
# Hero
# ---------------------------------------------------------------------------

st.title("GlobalTradeScope")
st.markdown(
    """
    A decade of Israel's bilateral trade (2014 – 2024) — explored through
    UN Comtrade flows, partner mirror statistics, and World Bank macro-indicators.

    Use the **pages** in the sidebar to explore trade patterns, partner concentration,
    commodity breakdowns, and mirror-data discrepancies.
    """
)
st.divider()

# ---------------------------------------------------------------------------
# Quick-stat metrics
# ---------------------------------------------------------------------------

st.subheader("Dataset summary")

try:
    stats_df = query("""
        SELECT
            COUNT(*)                                  AS trade_records,
            COUNT(DISTINCT partner_id)                AS trading_partners,
            MIN(year)                                 AS year_min,
            MAX(year)                                 AS year_max,
            ROUND(SUM(trade_value_usd) / 1e9, 1)     AS total_trade_b
        FROM fact_trade
        WHERE trade_value_usd > 0
    """)

    chap_df = query("SELECT COUNT(*) AS chapters FROM dim_commodity")

    row = stats_df.iloc[0]
    chapters = int(chap_df.iloc[0]["chapters"])

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        label="Trade records",
        value=f"{int(row['trade_records']):,}",
        help="Unique (year, partner, HS chapter, flow) observations in fact_trade",
    )
    c2.metric(
        label="Trading partners",
        value=f"{int(row['trading_partners']):,}",
        help="Distinct countries Israel traded with across all years",
    )
    c3.metric(
        label="Year range",
        value=f"{int(row['year_min'])} – {int(row['year_max'])}",
        help="Annual data from UN Comtrade",
    )
    c4.metric(
        label="HS commodity chapters",
        value=f"{chapters}",
        help="2-digit Harmonized System chapters loaded into dim_commodity",
    )

    st.caption(
        f"Total trade value across all years: **{fmt_usd(float(row['total_trade_b']) * 1e9)}**"
    )

except FileNotFoundError:
    st.info(
        "Database not found. Run the pipeline first:\n\n"
        "```\npython pipeline/01_ingest.py\n"
        "python pipeline/02_clean.py\n"
        "python pipeline/03_load_db.py\n```",
    )
except Exception as exc:
    st.warning(f"Could not load database statistics: {exc}")
