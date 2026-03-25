"""
pages/2_Dependency_Risk.py — Supply-chain concentration & dependency risk

Demonstrates:
  • Multi-CTE SQL pipelines for HHI calculation
  • Window functions (ROW_NUMBER, ARG_MAX) for dominant-supplier detection
  • Custom Plotly heatmap with a three-zone colorscale
  • Streamlit column_config for rich table formatting
"""

import plotly.graph_objects as go
import streamlit as st

from utils.constants import (
    APP_TITLE, COLORS, HHI_HIGH, HHI_MODERATE, PLOTLY_TEMPLATE,
    SECTION_COLORS, fmt_pct, fmt_usd,
)
from utils.db import query, query_uncached
from utils.styles import inject_css

st.set_page_config(
    page_title=f"Dependency Risk | {APP_TITLE}",
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

    st.header("Filters")

    try:
        yr_bounds = query("SELECT MIN(year) AS lo, MAX(year) AS hi FROM dim_time")
        yr_lo = int(yr_bounds.iloc[0]["lo"])
        yr_hi = int(yr_bounds.iloc[0]["hi"])
    except Exception:
        yr_lo, yr_hi = 2014, 2024

    year_range = st.slider(
        "Year range",
        min_value=yr_lo, max_value=yr_hi,
        value=(yr_lo, yr_hi), step=1,
    )
    yr_min, yr_max = year_range

    flow_option = st.radio(
        "Trade flow",
        options=["Imports", "Exports", "Both"],
        index=0,            # default: Imports — most relevant for supply-chain risk
        horizontal=True,
    )

    st.divider()
    st.caption(
        f"HHI thresholds: **Moderate ≥ {HHI_MODERATE:,}** · "
        f"**High ≥ {HHI_HIGH:,}**"
    )
    st.divider()
    st.caption("Built by Mahmod Zoubi")
    st.markdown("[GitHub](https://github.com/MahmodZoabi) · [LinkedIn](https://www.linkedin.com/in/mahmod-zoabi/)")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flow_clause(opt: str) -> str:
    if opt == "Imports":
        return "AND ft.flow_direction IN ('Import', 'Re-import')"
    if opt == "Exports":
        return "AND ft.flow_direction IN ('Export', 'Re-export')"
    return ""


# ---------------------------------------------------------------------------
# DB guard
# ---------------------------------------------------------------------------

try:
    if query("SELECT 1 FROM fact_trade LIMIT 1").empty:
        st.info("Database is empty. Run the pipeline first.")
        st.stop()
except Exception:
    st.info(
        "Database not found.\n\n"
        "```\npython pipeline/01_ingest.py\n"
        "python pipeline/02_clean.py\n"
        "python pipeline/03_load_db.py\n```",
    )
    st.stop()

flow_clause = _flow_clause(flow_option)

# ---------------------------------------------------------------------------
# Page title
# ---------------------------------------------------------------------------

st.title("Dependency Risk Analysis")
st.caption(
    f"Herfindahl-Hirschman Index (HHI) · Israel {flow_option.lower()} · "
    f"{yr_min}–{yr_max}"
)

# ---------------------------------------------------------------------------
# Explainer
# ---------------------------------------------------------------------------

with st.expander("What is HHI and how to read this page?", expanded=False):
    st.markdown(f"""
**Herfindahl-Hirschman Index (HHI)** measures how concentrated a market is.
For each HS commodity section, we calculate the share each partner country holds, then sum the squared shares:

> **HHI = Σ (partner_share %)²** — ranges from near 0 (many equal suppliers) to 10,000 (single monopoly supplier)

| HHI Range | Interpretation | Risk Level |
|---|---|---|
| < {HHI_MODERATE:,} | Diversified suppliers — resilient to disruption | Low |
| {HHI_MODERATE:,} – {HHI_HIGH:,} | Moderate concentration — monitor closely | Moderate |
| ≥ {HHI_HIGH:,} | Highly concentrated — single-point-of-failure risk | High |

The **Single-Source Alerts** table flags any commodity chapter where one country
supplies more than 50% of the total import value.
""")

st.divider()

# ---------------------------------------------------------------------------
# SQL 1 — HHI per section per year (powers heatmap + trend chart)
# ---------------------------------------------------------------------------

HHI_SQL = f"""
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  HHI Calculation — partner concentration per HS section per year       │
-- └─────────────────────────────────────────────────────────────────────────┘

WITH

-- Step 1: aggregate bilateral trade value to (year, section, partner) grain
section_partner AS (
    SELECT
        ft.year,
        dco.section_name,
        ft.partner_id,
        dc.country_name                     AS partner_name,
        SUM(ft.trade_value_usd)             AS trade_value
    FROM fact_trade         ft
    JOIN dim_commodity      dco ON ft.commodity_id = dco.commodity_id
    JOIN dim_country        dc  ON ft.partner_id   = dc.country_id
    WHERE ft.trade_value_usd > 0
      {flow_clause}
      AND ft.year BETWEEN {yr_min} AND {yr_max}
      AND dco.section_name IS NOT NULL
    GROUP BY ft.year, dco.section_name, ft.partner_id, dc.country_name
),

-- Step 2: section-year totals (denominator for market-share calculation)
section_totals AS (
    SELECT
        year,
        section_name,
        SUM(trade_value) AS total_value
    FROM section_partner
    GROUP BY year, section_name
),

-- Step 3: per-partner market shares (percentage, 0-100 scale)
market_shares AS (
    SELECT
        sp.year,
        sp.section_name,
        sp.partner_name,
        sp.trade_value,
        st.total_value,
        sp.trade_value / st.total_value * 100.0   AS share_pct
    FROM section_partner  sp
    JOIN section_totals   st
      ON sp.year = st.year AND sp.section_name = st.section_name
),

-- Step 4: HHI = sum of squared market shares; also expose top supplier via ARG_MAX
hhi_results AS (
    SELECT
        year,
        section_name,
        ROUND(SUM(share_pct * share_pct), 0)     AS hhi,
        COUNT(DISTINCT partner_name)             AS supplier_count,
        ROUND(MAX(share_pct), 1)                 AS top_share_pct,
        ARG_MAX(partner_name, share_pct)         AS top_supplier
    FROM market_shares
    GROUP BY year, section_name
)

SELECT * FROM hhi_results
ORDER BY section_name, year
"""

hhi_df = query_uncached(HHI_SQL)

# ---------------------------------------------------------------------------
# Chart 1 — HHI Supplier Concentration Heatmap
# ---------------------------------------------------------------------------

st.subheader("HHI Supplier Concentration Heatmap")
st.caption("Rows = HS sections · Columns = years · Colour = HHI (green = diversified, red = concentrated)")

if hhi_df.empty:
    st.info("No data for the selected filters.")
else:
    # Pivot: section_name (rows) × year (cols) → HHI value
    hhi_pivot = (
        hhi_df
        .pivot(index="section_name", columns="year", values="hhi")
        .sort_index()
    )
    years     = [int(c) for c in hhi_pivot.columns]
    sections  = list(hhi_pivot.index)
    z_values  = hhi_pivot.values.tolist()

    # Build hover text matrix: "Section · Year\nHHI: X\nTop supplier: Y (Z%)"
    hover_df = hhi_df.set_index(["section_name", "year"])
    hover_text = []
    for sec in sections:
        row_hover = []
        for yr in years:
            try:
                r = hover_df.loc[(sec, yr)]
                row_hover.append(
                    f"<b>{sec}</b> · {yr}<br>"
                    f"HHI: <b>{int(r['hhi']):,}</b><br>"
                    f"Suppliers: {int(r['supplier_count'])}<br>"
                    f"Top: {r['top_supplier']} ({r['top_share_pct']:.1f}%)"
                )
            except KeyError:
                row_hover.append("No data")
        hover_text.append(row_hover)

    # Three-zone colorscale: green → yellow → red (capped at 10,000)
    _zmax = 10_000
    colorscale = [
        [0.0,                        "#16A34A"],  # 0      — diversified
        [HHI_MODERATE / _zmax,       "#84CC16"],  # ~1 500 — low-moderate boundary
        [HHI_MODERATE / _zmax + 0.001, "#F59E0B"],  # step → yellow
        [HHI_HIGH     / _zmax,       "#F97316"],  # ~2 500 — moderate-high boundary
        [HHI_HIGH     / _zmax + 0.001, "#DC2626"],  # step → red
        [1.0,                        "#7F1D1D"],  # 10 000 — monopoly
    ]

    fig_heat = go.Figure(go.Heatmap(
        x=years,
        y=sections,
        z=z_values,
        text=hover_text,
        hovertemplate="%{text}<extra></extra>",
        colorscale=colorscale,
        zmin=0,
        zmax=_zmax,
        colorbar=dict(
            title=dict(text="HHI", side="right"),
            tickvals=[0, HHI_MODERATE, HHI_HIGH, 5000, _zmax],
            ticktext=["0", f"{HHI_MODERATE:,}", f"{HHI_HIGH:,}", "5,000", "10,000"],
            len=0.8,
        ),
        xgap=2,
        ygap=2,
    ))
    fig_heat.update_layout(
        template=PLOTLY_TEMPLATE,
        xaxis=dict(title="Year", dtick=1, tickformat="d", side="bottom"),
        yaxis=dict(title="", autorange="reversed"),
        margin=dict(t=20, b=60, l=10, r=80),
        height=max(420, len(sections) * 26),
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    # Risk summary row
    latest_yr = hhi_df["year"].max()
    latest = hhi_df[hhi_df["year"] == latest_yr]
    n_high = (latest["hhi"] >= HHI_HIGH).sum()
    n_mod  = ((latest["hhi"] >= HHI_MODERATE) & (latest["hhi"] < HHI_HIGH)).sum()
    n_low  = (latest["hhi"] <  HHI_MODERATE).sum()

    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("High-risk sections",     f"{n_high}",
               help=f"HHI ≥ {HHI_HIGH:,} in {latest_yr}")
    mc2.metric("Moderate-risk sections", f"{n_mod}",
               help=f"HHI {HHI_MODERATE:,}–{HHI_HIGH:,} in {latest_yr}")
    mc3.metric("Diversified sections",   f"{n_low}",
               help=f"HHI < {HHI_MODERATE:,} in {latest_yr}")

st.divider()

# ---------------------------------------------------------------------------
# Chart 2 — Concentration Trend (average HHI over time)
# ---------------------------------------------------------------------------

TREND_SQL = f"""
-- Average HHI across all sections per year — shows whether Israel is
-- diversifying or concentrating its supply chains over time

WITH

section_partner AS (
    SELECT
        ft.year,
        dco.section_name,
        ft.partner_id,
        SUM(ft.trade_value_usd) AS trade_value
    FROM fact_trade    ft
    JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
    WHERE ft.trade_value_usd > 0
      {flow_clause}
      AND ft.year BETWEEN {yr_min} AND {yr_max}
      AND dco.section_name IS NOT NULL
    GROUP BY ft.year, dco.section_name, ft.partner_id
),

section_totals AS (
    SELECT year, section_name, SUM(trade_value) AS total_value
    FROM section_partner
    GROUP BY year, section_name
),

section_hhi AS (
    SELECT
        sp.year,
        sp.section_name,
        SUM(POWER(sp.trade_value / st.total_value * 100.0, 2)) AS hhi
    FROM section_partner sp
    JOIN section_totals  st
      ON sp.year = st.year AND sp.section_name = st.section_name
    GROUP BY sp.year, sp.section_name
)

SELECT
    year,
    ROUND(AVG(hhi),  0) AS avg_hhi,
    ROUND(MIN(hhi),  0) AS min_hhi,
    ROUND(MAX(hhi),  0) AS max_hhi,
    COUNT(*)            AS section_count,
    SUM(CASE WHEN hhi >= {HHI_HIGH}                            THEN 1 ELSE 0 END) AS n_high,
    SUM(CASE WHEN hhi >= {HHI_MODERATE} AND hhi < {HHI_HIGH}  THEN 1 ELSE 0 END) AS n_moderate,
    SUM(CASE WHEN hhi <  {HHI_MODERATE}                        THEN 1 ELSE 0 END) AS n_low
FROM section_hhi
GROUP BY year
ORDER BY year
"""

trend_df = query_uncached(TREND_SQL)

col_trend, col_counts = st.columns([6, 4], gap="large")

with col_trend:
    st.subheader("Average HHI Trend")
    st.caption("Mean across all 21 HS sections — rising = supply chains concentrating")

    if not trend_df.empty:
        fig_trend = go.Figure()

        # Shaded confidence band (min–max range)
        fig_trend.add_trace(go.Scatter(
            x=list(trend_df["year"]) + list(trend_df["year"])[::-1],
            y=list(trend_df["max_hhi"]) + list(trend_df["min_hhi"])[::-1],
            fill="toself",
            fillcolor="rgba(37, 99, 235, 0.08)",
            line=dict(color="rgba(0,0,0,0)"),
            name="Min–Max range",
            hoverinfo="skip",
        ))

        # Average HHI line
        fig_trend.add_trace(go.Scatter(
            x=trend_df["year"],
            y=trend_df["avg_hhi"],
            name="Avg HHI",
            mode="lines+markers",
            line=dict(color=COLORS["import"], width=2.5),
            marker=dict(size=7),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Avg HHI: %{y:,.0f}<br>"
                "<extra></extra>"
            ),
        ))

        # Threshold reference lines
        fig_trend.add_hline(
            y=HHI_MODERATE, line_color=COLORS["moderate_risk"],
            line_dash="dot", line_width=1.5,
            annotation_text=f"Moderate ≥{HHI_MODERATE:,}",
            annotation_position="right",
        )
        fig_trend.add_hline(
            y=HHI_HIGH, line_color=COLORS["high_risk"],
            line_dash="dot", line_width=1.5,
            annotation_text=f"High ≥{HHI_HIGH:,}",
            annotation_position="right",
        )

        fig_trend.update_layout(
            template=PLOTLY_TEMPLATE,
            xaxis=dict(title="Year", dtick=1, tickformat="d"),
            yaxis=dict(title="HHI"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=30, b=50, r=120),
            height=360,
            hovermode="x unified",
        )
        st.plotly_chart(fig_trend, use_container_width=True)

with col_counts:
    st.subheader("Risk Distribution by Year")
    st.caption("Number of sections in each HHI risk band")

    if not trend_df.empty:
        fig_stack = go.Figure()
        fig_stack.add_trace(go.Bar(
            x=trend_df["year"], y=trend_df["n_high"],
            name="High",     marker_color=COLORS["high_risk"],
            hovertemplate="%{y} sections<extra>High</extra>",
        ))
        fig_stack.add_trace(go.Bar(
            x=trend_df["year"], y=trend_df["n_moderate"],
            name="Moderate", marker_color=COLORS["moderate_risk"],
            hovertemplate="%{y} sections<extra>Moderate</extra>",
        ))
        fig_stack.add_trace(go.Bar(
            x=trend_df["year"], y=trend_df["n_low"],
            name="Low",      marker_color=COLORS["low_risk"],
            hovertemplate="%{y} sections<extra>Low</extra>",
        ))
        fig_stack.update_layout(
            template=PLOTLY_TEMPLATE,
            barmode="stack",
            xaxis=dict(dtick=1, tickformat="d"),
            yaxis=dict(title="Sections"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=30, b=50),
            height=360,
        )
        st.plotly_chart(fig_stack, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# SQL 3 — Single-Source Alert Table
# ---------------------------------------------------------------------------

ALERT_SQL = f"""
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  Single-Source Alerts — chapters with dominant supplier (> 50% share)  │
-- └─────────────────────────────────────────────────────────────────────────┘

WITH

-- Aggregate to (HS chapter, supplier) grain across selected years
chapter_partner AS (
    SELECT
        dco.hs_chapter,
        dco.description,
        dco.section_name,
        dc.country_name                 AS supplier,
        dc.iso3_code                    AS supplier_iso3,
        SUM(ft.trade_value_usd)         AS supplier_value
    FROM fact_trade    ft
    JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
    JOIN dim_country   dc  ON ft.partner_id   = dc.country_id
    WHERE ft.trade_value_usd > 0
      {flow_clause}
      AND ft.year BETWEEN {yr_min} AND {yr_max}
      AND dc.iso3_code IS NOT NULL
    GROUP BY dco.hs_chapter, dco.description, dco.section_name,
             dc.country_name, dc.iso3_code
),

-- Chapter totals (denominator)
chapter_totals AS (
    SELECT
        hs_chapter,
        SUM(supplier_value) AS chapter_total
    FROM chapter_partner
    GROUP BY hs_chapter
),

-- Rank suppliers within each chapter; compute share percentage
ranked_suppliers AS (
    SELECT
        cp.hs_chapter,
        cp.description,
        cp.section_name,
        cp.supplier,
        cp.supplier_iso3,
        cp.supplier_value,
        ct.chapter_total,
        ROUND(cp.supplier_value / ct.chapter_total * 100.0, 1) AS share_pct,
        ROW_NUMBER() OVER (
            PARTITION BY cp.hs_chapter
            ORDER BY cp.supplier_value DESC
        ) AS supplier_rank
    FROM chapter_partner  cp
    JOIN chapter_totals   ct ON cp.hs_chapter = ct.hs_chapter
)

-- Keep only the dominant supplier rows where share > 50%
SELECT
    hs_chapter,
    description,
    section_name,
    supplier,
    supplier_iso3,
    share_pct,
    ROUND(supplier_value / 1e9, 3) AS supplier_value_b,
    ROUND(chapter_total  / 1e9, 3) AS chapter_total_b
FROM ranked_suppliers
WHERE supplier_rank = 1
  AND share_pct > 50
ORDER BY share_pct DESC
"""

alert_df = query_uncached(ALERT_SQL)

st.subheader("Single-Source Dependency Alerts")
st.caption(
    "HS chapters where one country supplies > 50% of total import value "
    f"({yr_min}–{yr_max} aggregated)"
)

if alert_df.empty:
    st.success(
        "No single-source dependencies detected for the selected filters. "
        "All commodity chapters have a dominant supplier below the 50% threshold.",
    )
else:
    # Add risk label and sort
    def _risk(share: float) -> str:
        return "HIGH" if share >= 70 else "MODERATE"

    alert_df["risk_level"] = alert_df["share_pct"].apply(_risk)

    # Reorder columns for display
    display_cols = [
        "risk_level", "hs_chapter", "description", "section_name",
        "supplier", "supplier_iso3", "share_pct",
        "supplier_value_b", "chapter_total_b",
    ]
    display_df = alert_df[display_cols].copy()

    n_high = (alert_df["share_pct"] >= 70).sum()
    n_mod  = (alert_df["share_pct"] <  70).sum()
    st.caption(
        f"**{len(alert_df)} chapters** flagged · "
        f"{n_high} HIGH (≥70%) · {n_mod} MODERATE (50–70%)"
    )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "risk_level": st.column_config.TextColumn(
                "Risk", width="small",
            ),
            "hs_chapter": st.column_config.TextColumn(
                "Ch.", width="small",
            ),
            "description": st.column_config.TextColumn(
                "Commodity", width="large",
            ),
            "section_name": st.column_config.TextColumn(
                "HS Section",
            ),
            "supplier": st.column_config.TextColumn(
                "Dominant Supplier",
            ),
            "supplier_iso3": st.column_config.TextColumn(
                "ISO3", width="small",
            ),
            "share_pct": st.column_config.ProgressColumn(
                "Import Share",
                format="%.1f%%",
                min_value=0,
                max_value=100,
            ),
            "supplier_value_b": st.column_config.NumberColumn(
                "Supplier Value (B USD)",
                format="$%.3f",
            ),
            "chapter_total_b": st.column_config.NumberColumn(
                "Chapter Total (B USD)",
                format="$%.3f",
            ),
        },
    )
