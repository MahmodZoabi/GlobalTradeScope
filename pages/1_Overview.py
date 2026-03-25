"""
pages/1_Overview.py — Trade Overview dashboard
Bilateral trade flows for Israel: balance over time, top partners, commodity mix.
"""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.constants import APP_TITLE, COLORS, PLOTLY_TEMPLATE, SECTION_COLORS, fmt_usd, fmt_pct
from utils.db import query, query_uncached
from utils.styles import inject_css

st.set_page_config(
    page_title=f"Trade Overview | {APP_TITLE}",
    page_icon=None,
    layout="wide",
)
inject_css()

# ---------------------------------------------------------------------------
# World Bank region → colour (for treemap)
# ---------------------------------------------------------------------------

REGION_COLORS: dict[str, str] = {
    "East Asia & Pacific":         "#0EA5E9",
    "Europe & Central Asia":       "#6366F1",
    "Latin America & Caribbean":   "#F97316",
    "Middle East & North Africa":  "#EAB308",
    "North America":               "#22C55E",
    "South Asia":                  "#A855F7",
    "Sub-Saharan Africa":          "#F43F5E",
    "High income":                 "#64748B",  # fallback for unclassified
}


def _flow_clause(flow_option: str) -> str:
    """Return a SQL WHERE fragment for the chosen flow radio option."""
    if flow_option == "Imports":
        return "AND ft.flow_direction IN ('Import', 'Re-import')"
    if flow_option == "Exports":
        return "AND ft.flow_direction IN ('Export', 'Re-export')"
    return ""  # Both


# ---------------------------------------------------------------------------
# Sidebar controls
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("GlobalTradeScope")
    st.caption("Israel bilateral trade intelligence · 2014 – 2024")
    st.divider()

    st.header("Filters")

    # Year range — derive bounds from DB
    try:
        yr_bounds = query("SELECT MIN(year) AS lo, MAX(year) AS hi FROM dim_time")
        yr_lo = int(yr_bounds.iloc[0]["lo"])
        yr_hi = int(yr_bounds.iloc[0]["hi"])
    except Exception:
        yr_lo, yr_hi = 2014, 2024

    year_range: tuple[int, int] = st.slider(
        "Year range",
        min_value=yr_lo,
        max_value=yr_hi,
        value=(yr_lo, yr_hi),
        step=1,
    )
    yr_min, yr_max = year_range

    flow_option: str = st.radio(
        "Trade flow",
        options=["Both", "Imports", "Exports"],
        horizontal=True,
    )

    st.caption(f"Showing {yr_min} – {yr_max} · {flow_option}")
    st.divider()
    st.caption("Built by Mahmod Zoubi")
    st.markdown("[GitHub](https://github.com/MahmodZoabi) · [LinkedIn](https://www.linkedin.com/in/mahmod-zoabi/)")

# ---------------------------------------------------------------------------
# Page title
# ---------------------------------------------------------------------------

st.title("Trade Overview")
st.caption(f"Israel bilateral trade · {yr_min}–{yr_max} · {flow_option}")

# ---------------------------------------------------------------------------
# Guard: ensure DB is reachable
# ---------------------------------------------------------------------------

try:
    _probe = query("SELECT 1 FROM fact_trade LIMIT 1")
    if _probe.empty:
        st.info("The database is empty. Run the pipeline to load data.")
        st.stop()
except Exception as exc:
    st.info(
        "Database not found. Run the pipeline first:\n\n"
        "```\npython pipeline/01_ingest.py\n"
        "python pipeline/02_clean.py\n"
        "python pipeline/03_load_db.py\n```",
    )
    st.stop()

# ---------------------------------------------------------------------------
# Annual aggregates (used by KPIs + area chart)
# ---------------------------------------------------------------------------

annual_df = query("""
    SELECT
        year,
        SUM(CASE WHEN flow_direction IN ('Import', 'Re-import')
                 THEN trade_value_usd ELSE 0 END)           AS imports_usd,
        SUM(CASE WHEN flow_direction IN ('Export', 'Re-export')
                 THEN trade_value_usd ELSE 0 END)           AS exports_usd,
        COUNT(DISTINCT partner_id)                          AS active_partners
    FROM fact_trade
    WHERE trade_value_usd > 0
    GROUP BY year
    ORDER BY year
""")

# Filtered to selected range
range_df  = annual_df[(annual_df.year >= yr_min) & (annual_df.year <= yr_max)]
curr_yr   = annual_df[annual_df.year == yr_max]
prev_yr   = annual_df[annual_df.year == yr_max - 1]

def _sum(col: str, df=range_df) -> float:
    return float(df[col].sum()) if not df.empty else 0.0

def _val(col: str, df=curr_yr) -> float | None:
    return float(df[col].iloc[0]) if not df.empty else None

def _yoy(col: str) -> str | None:
    c = _val(col, curr_yr)
    p = _val(col, prev_yr)
    if c is None or p is None or p == 0:
        return None
    return fmt_pct((c - p) / abs(p), scale=100)


total_imports  = _sum("imports_usd")
total_exports  = _sum("exports_usd")
trade_balance  = total_exports - total_imports
total_partners = int(range_df["active_partners"].max()) if not range_df.empty else 0

# ---------------------------------------------------------------------------
# KPI Cards
# ---------------------------------------------------------------------------

st.subheader("Key indicators")
k1, k2, k3, k4 = st.columns(4)

k1.metric(
    label="Total Imports",
    value=fmt_usd(total_imports),
    delta=_yoy("imports_usd"),
    delta_color="inverse",          # higher imports → red (more spending)
    help=f"Sum of all import values {yr_min}–{yr_max}. "
         "Delta = last year vs prior year.",
)
k2.metric(
    label="Total Exports",
    value=fmt_usd(total_exports),
    delta=_yoy("exports_usd"),
    help=f"Sum of all export values {yr_min}–{yr_max}.",
)
k3.metric(
    label="Trade Balance",
    value=fmt_usd(trade_balance),
    delta=None,
    delta_color="normal",
    help="Exports minus Imports across selected period. Negative = deficit.",
)
k4.metric(
    label="Active Partners",
    value=f"{total_partners:,}",
    delta=_yoy("active_partners"),
    help="Peak distinct partner count in a single year within selected range.",
)

st.divider()

# ---------------------------------------------------------------------------
# Chart 1 — Trade balance area chart
# ---------------------------------------------------------------------------

st.subheader("Trade balance over time")

balance_df = range_df.copy()
balance_df["balance_b"]  = (balance_df["exports_usd"] - balance_df["imports_usd"]) / 1e9
balance_df["imports_b"]  = balance_df["imports_usd"] / 1e9
balance_df["exports_b"]  = balance_df["exports_usd"] / 1e9

fig_balance = go.Figure()

fig_balance.add_trace(go.Scatter(
    x=balance_df["year"],
    y=balance_df["imports_b"],
    name="Imports",
    mode="lines+markers",
    fill="tozeroy",
    line=dict(color=COLORS["import"], width=2.5),
    fillcolor="rgba(37, 99, 235, 0.12)",
    marker=dict(size=6),
    hovertemplate="%{y:.2f}B<extra>Imports</extra>",
))
fig_balance.add_trace(go.Scatter(
    x=balance_df["year"],
    y=balance_df["exports_b"],
    name="Exports",
    mode="lines+markers",
    fill="tozeroy",
    line=dict(color=COLORS["export"], width=2.5),
    fillcolor="rgba(22, 163, 74, 0.12)",
    marker=dict(size=6),
    hovertemplate="%{y:.2f}B<extra>Exports</extra>",
))
fig_balance.add_trace(go.Scatter(
    x=balance_df["year"],
    y=balance_df["balance_b"],
    name="Balance",
    mode="lines",
    line=dict(
        color=COLORS["deficit"] if trade_balance < 0 else COLORS["surplus"],
        width=1.8,
        dash="dot",
    ),
    hovertemplate="%{y:.2f}B<extra>Balance</extra>",
))
fig_balance.add_hline(y=0, line_color="#94A3B8", line_width=1, line_dash="dash")

fig_balance.update_layout(
    template=PLOTLY_TEMPLATE,
    xaxis=dict(title="Year", dtick=1, tickformat="d"),
    yaxis=dict(title="USD Billions"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    hovermode="x unified",
    margin=dict(t=40, b=40),
    height=380,
)

st.plotly_chart(fig_balance, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Charts 2 & 3 — Partners treemap  |  Commodity sector bar
# ---------------------------------------------------------------------------

col_tree, col_bar = st.columns([6, 5], gap="large")

# ── 2. Top-20 trading partners treemap ──────────────────────────────────────

with col_tree:
    st.subheader("Top 20 trading partners")

    flow_clause = _flow_clause(flow_option)
    partners_df = query_uncached(f"""
        SELECT
            dc.country_name,
            COALESCE(dc.region, 'Unknown') AS region,
            ROUND(SUM(ft.trade_value_usd) / 1e9, 3) AS trade_value_b
        FROM fact_trade ft
        JOIN dim_country dc ON ft.partner_id = dc.country_id
        WHERE ft.trade_value_usd > 0
          AND ft.year BETWEEN {yr_min} AND {yr_max}
          {flow_clause}
        GROUP BY dc.country_name, dc.region
        ORDER BY trade_value_b DESC
        LIMIT 20
    """)

    if partners_df.empty:
        st.info("No partner data for the selected filters.")
    else:
        # Build color_discrete_map from known regions
        unique_regions = partners_df["region"].unique().tolist()
        color_map = {r: REGION_COLORS.get(r, "#94A3B8") for r in unique_regions}
        color_map["(?)"] = "#94A3B8"

        fig_tree = px.treemap(
            partners_df,
            path=[px.Constant("All partners"), "region", "country_name"],
            values="trade_value_b",
            color="region",
            color_discrete_map=color_map,
            hover_data={"trade_value_b": ":.2f"},
        )
        fig_tree.update_traces(
            textinfo="label+value",
            hovertemplate="<b>%{label}</b><br>%{value:.2f}B USD<extra></extra>",
        )
        fig_tree.update_layout(
            template=PLOTLY_TEMPLATE,
            margin=dict(t=10, b=10, l=10, r=10),
            height=440,
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_tree, use_container_width=True)

# ── 3. Trade by commodity sector — horizontal grouped bar ───────────────────

with col_bar:
    st.subheader("Trade by HS section")

    sector_df = query_uncached(f"""
        SELECT
            COALESCE(dco.section_name, 'Unknown') AS section_name,
            ROUND(SUM(CASE WHEN ft.flow_direction IN ('Import', 'Re-import')
                      THEN ft.trade_value_usd ELSE 0 END) / 1e9, 3) AS imports_b,
            ROUND(SUM(CASE WHEN ft.flow_direction IN ('Export', 'Re-export')
                      THEN ft.trade_value_usd ELSE 0 END) / 1e9, 3) AS exports_b
        FROM fact_trade ft
        JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
        WHERE ft.trade_value_usd > 0
          AND ft.year BETWEEN {yr_min} AND {yr_max}
        GROUP BY dco.section_name
        ORDER BY (imports_b + exports_b) DESC
        LIMIT 15
    """)

    if sector_df.empty:
        st.info("No commodity data for the selected filters.")
    else:
        # Sort ascending so largest bar is at top of horizontal chart
        sector_df = sector_df.sort_values("imports_b", ascending=True)

        fig_bar = go.Figure()

        if flow_option in ("Both", "Imports"):
            fig_bar.add_trace(go.Bar(
                y=sector_df["section_name"],
                x=sector_df["imports_b"],
                name="Imports",
                orientation="h",
                marker_color=COLORS["import"],
                hovertemplate="%{x:.2f}B<extra>Imports</extra>",
            ))
        if flow_option in ("Both", "Exports"):
            fig_bar.add_trace(go.Bar(
                y=sector_df["section_name"],
                x=sector_df["exports_b"],
                name="Exports",
                orientation="h",
                marker_color=COLORS["export"],
                hovertemplate="%{x:.2f}B<extra>Exports</extra>",
            ))

        fig_bar.update_layout(
            template=PLOTLY_TEMPLATE,
            barmode="group",
            xaxis=dict(title="USD Billions"),
            yaxis=dict(title=""),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            margin=dict(t=30, b=40, l=0, r=10),
            height=440,
        )
        st.plotly_chart(fig_bar, use_container_width=True)
