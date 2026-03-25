"""
pages/4_Commodity_Explorer.py — Deep dive into any HS commodity section

Demonstrates:
  • NTILE(4) window function for growth-rate quartile assignment
  • FULL OUTER JOIN for early-vs-recent period comparison
  • CASE WHEN for conditional aggregation (new entrants vs existing suppliers)
  • Dual-panel trend analysis (supplier count + HHI on same axis range)
"""

import plotly.graph_objects as go
import streamlit as st

from utils.constants import (
    APP_TITLE, COLORS, HHI_HIGH, HHI_MODERATE, PLOTLY_TEMPLATE,
    SECTION_COLORS, fmt_pct, fmt_usd,
)
from utils.db import query, query_uncached
from utils.nav import render_nav
from utils.styles import inject_css

st.set_page_config(
    page_title=f"Commodity Explorer | {APP_TITLE}",
    page_icon=None,
    layout="wide",
)
inject_css()
render_nav(__file__)

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

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _esc(s: str) -> str:
    """Escape single quotes for safe SQL string literals."""
    return s.replace("'", "''")


def _flow_clause(opt: str) -> str:
    if opt == "Imports":
        return "AND ft.flow_direction IN ('Import', 'Re-import')"
    if opt == "Exports":
        return "AND ft.flow_direction IN ('Export', 'Re-export')"
    return ""


# ---------------------------------------------------------------------------
# Load section list (static — cached)
# ---------------------------------------------------------------------------

sections_df = query("""
    SELECT
        dco.section_name,
        ROUND(SUM(ft.trade_value_usd) / 1e9, 2) AS total_b
    FROM dim_commodity dco
    JOIN fact_trade    ft  ON dco.commodity_id = ft.commodity_id
    WHERE ft.trade_value_usd > 0
    GROUP BY dco.section_name
    ORDER BY total_b DESC
""")

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("GlobalTradeScope")
    st.caption("Israel bilateral trade intelligence · 2014 – 2024")
    st.divider()

    st.header("Filters")

    section_options = [
        f"{row.section_name}  (${row.total_b:.1f}B)"
        for row in sections_df.itertuples()
    ]
    selected_label   = st.selectbox(
        "HS Section",
        options=section_options,
        index=0,
        help="Sections sorted by total bilateral trade value (all years)",
    )
    selected_idx     = section_options.index(selected_label)
    section_name     = sections_df.iloc[selected_idx]["section_name"]
    section_color    = SECTION_COLORS.get(section_name, COLORS["highlight"])

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
        options=["Both", "Imports", "Exports"],
        horizontal=True,
    )

    # Period boundaries for early/recent comparison
    span       = yr_max - yr_min + 1
    period_len = max(1, min(3, span // 2))
    early_end    = yr_min + period_len - 1
    recent_start = yr_max - period_len + 1

    st.divider()
    st.caption(
        f"Early period: **{yr_min}–{early_end}**\n\n"
        f"Recent period: **{recent_start}–{yr_max}**"
    )
    st.divider()
    st.caption("Built by Mahmod Zoubi")
    st.markdown("[GitHub](https://github.com/MahmodZoabi) · [LinkedIn](https://www.linkedin.com/in/mahmod-zoabi/)")

flow_clause = _flow_clause(flow_option)
sec_sql     = _esc(section_name)

# ---------------------------------------------------------------------------
# Page title
# ---------------------------------------------------------------------------

st.title(section_name)
st.caption(
    f"HS Section · Israel {flow_option.lower()} · {yr_min}–{yr_max}"
)

# ---------------------------------------------------------------------------
# SQL 1 — Section summary KPIs
# ---------------------------------------------------------------------------

SUMMARY_SQL = f"""
WITH section_data AS (
    SELECT
        ft.year,
        ft.partner_id,
        dco.hs_chapter,
        ft.trade_value_usd
    FROM fact_trade    ft
    JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
    WHERE dco.section_name  = '{sec_sql}'
      AND ft.trade_value_usd > 0
      {flow_clause}
      AND ft.year BETWEEN {yr_min} AND {yr_max}
)
SELECT
    ROUND(SUM(trade_value_usd) / 1e9, 3)                             AS total_b,
    COUNT(DISTINCT hs_chapter)                                        AS chapter_count,
    COUNT(DISTINCT partner_id)                                        AS partner_count,
    ROUND(SUM(CASE WHEN year = {yr_max}     THEN trade_value_usd END) / 1e9, 3) AS curr_yr_b,
    ROUND(SUM(CASE WHEN year = {yr_max - 1} THEN trade_value_usd END) / 1e9, 3) AS prev_yr_b
FROM section_data
"""

summary_df = query_uncached(SUMMARY_SQL)

if summary_df.empty or summary_df["total_b"].iloc[0] is None:
    st.info(f"No data found for **{section_name}** in the selected filters.")
    st.stop()

s = summary_df.iloc[0]
total_b    = float(s["total_b"]    or 0)
curr_yr_b  = float(s["curr_yr_b"] or 0)
prev_yr_b  = float(s["prev_yr_b"] or 0)
yoy_pct    = ((curr_yr_b - prev_yr_b) / prev_yr_b * 100) if prev_yr_b else None

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Trade Value",  fmt_usd(total_b * 1e9))
k2.metric("HS Chapters",        int(s["chapter_count"] or 0))
k3.metric("Trading Partners",   int(s["partner_count"] or 0))
k4.metric(
    f"YoY Growth ({yr_max})",
    fmt_pct(yoy_pct, decimals=1) if yoy_pct is not None else "N/A",
    delta=f"{yoy_pct:+.1f}%" if yoy_pct is not None else None,
)

st.divider()

# ---------------------------------------------------------------------------
# Charts 1 & 2 — Source countries bar  |  Diversification trend
# ---------------------------------------------------------------------------

col_src, col_div = st.columns([6, 5], gap="large")

# ── Top 15 source / destination countries ────────────────────────────────────

with col_src:
    st.subheader("Top 15 Partner Countries")

    SOURCE_SQL = f"""
    SELECT
        dc.country_name,
        dc.iso3_code,
        ROUND(SUM(CASE WHEN ft.flow_direction IN ('Import', 'Re-import')
                       THEN ft.trade_value_usd ELSE 0 END) / 1e9, 3) AS imports_b,
        ROUND(SUM(CASE WHEN ft.flow_direction IN ('Export', 'Re-export')
                       THEN ft.trade_value_usd ELSE 0 END) / 1e9, 3) AS exports_b
    FROM fact_trade    ft
    JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
    JOIN dim_country   dc  ON ft.partner_id   = dc.country_id
    WHERE dco.section_name   = '{sec_sql}'
      AND ft.trade_value_usd > 0
      {flow_clause}
      AND ft.year BETWEEN {yr_min} AND {yr_max}
    GROUP BY dc.country_name, dc.iso3_code
    ORDER BY imports_b + exports_b DESC
    LIMIT 15
    """

    src_df = query_uncached(SOURCE_SQL)

    if src_df.empty:
        st.info("No partner data for the selected filters.")
    else:
        src_df = src_df.sort_values("imports_b", ascending=True)

        fig_src = go.Figure()
        if flow_option in ("Both", "Imports"):
            fig_src.add_trace(go.Bar(
                y=src_df["country_name"], x=src_df["imports_b"],
                name="Imports", orientation="h",
                marker_color=COLORS["import"],
                hovertemplate="<b>%{y}</b><br>Imports: $%{x:.3f}B<extra></extra>",
            ))
        if flow_option in ("Both", "Exports"):
            fig_src.add_trace(go.Bar(
                y=src_df["country_name"], x=src_df["exports_b"],
                name="Exports", orientation="h",
                marker_color=COLORS["export"],
                hovertemplate="<b>%{y}</b><br>Exports: $%{x:.3f}B<extra></extra>",
            ))
        fig_src.update_layout(
            template=PLOTLY_TEMPLATE,
            barmode="group",
            xaxis=dict(title="USD Billions"),
            yaxis=dict(title=""),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=30, b=50, l=0),
            height=460,
        )
        st.plotly_chart(fig_src, use_container_width=True)

# ── Diversification trend (supplier count + HHI) ─────────────────────────────

with col_div:
    st.subheader("Diversification Trend")

    DIV_SQL = f"""
    -- Supplier count and HHI for this section per year.
    -- Rising supplier count + falling HHI = increasing diversification.

    WITH annual_partner AS (
        SELECT
            ft.year,
            ft.partner_id,
            SUM(ft.trade_value_usd) AS partner_value
        FROM fact_trade    ft
        JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
        WHERE dco.section_name   = '{sec_sql}'
          AND ft.trade_value_usd > 0
          {flow_clause}
          AND ft.year BETWEEN {yr_min} AND {yr_max}
        GROUP BY ft.year, ft.partner_id
    ),
    year_totals AS (
        SELECT year, SUM(partner_value) AS total_value
        FROM annual_partner
        GROUP BY year
    )
    SELECT
        ap.year,
        COUNT(DISTINCT ap.partner_id)                                       AS supplier_count,
        ROUND(SUM(POWER(ap.partner_value / yt.total_value * 100.0, 2)), 0) AS hhi
    FROM annual_partner ap
    JOIN year_totals    yt ON ap.year = yt.year
    GROUP BY ap.year
    ORDER BY ap.year
    """

    div_df = query_uncached(DIV_SQL)

    if not div_df.empty:
        fig_div = go.Figure()

        # Supplier count on primary axis
        fig_div.add_trace(go.Scatter(
            x=div_df["year"], y=div_df["supplier_count"],
            name="Active partners",
            mode="lines+markers",
            line=dict(color=section_color, width=2.5),
            marker=dict(size=7),
            yaxis="y1",
            hovertemplate="<b>%{x}</b><br>Partners: %{y}<extra></extra>",
        ))

        # HHI on secondary axis
        fig_div.add_trace(go.Scatter(
            x=div_df["year"], y=div_df["hhi"],
            name="HHI",
            mode="lines+markers",
            line=dict(color=COLORS["moderate_risk"], width=2, dash="dot"),
            marker=dict(size=6),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>HHI: %{y:,.0f}<extra></extra>",
        ))

        # Threshold lines (HHI axis)
        fig_div.add_hline(
            y=HHI_MODERATE, line_color=COLORS["moderate_risk"],
            line_dash="dot", line_width=1,
            annotation_text=f"HHI {HHI_MODERATE:,}", annotation_position="right",
            secondary_y=True,
        ) if False else None   # hline doesn't support secondary_y; use shape

        fig_div.update_layout(
            template=PLOTLY_TEMPLATE,
            xaxis=dict(title="Year", dtick=1, tickformat="d"),
            yaxis=dict(title="Active partner countries", side="left"),
            yaxis2=dict(
                title="HHI",
                overlaying="y",
                side="right",
                showgrid=False,
            ),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=30, b=50, r=60),
            height=460,
            hovermode="x unified",
        )
        st.plotly_chart(fig_div, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Emerging Suppliers — NTILE growth quartiles
# ---------------------------------------------------------------------------

st.subheader("Emerging Suppliers")

if span < 2:
    st.info("Select at least 2 years to compare early vs recent periods.")
else:
    st.caption(
        f"Comparing **{yr_min}–{early_end}** (early avg/yr) "
        f"vs **{recent_start}–{yr_max}** (recent avg/yr) · "
        f"Min threshold: $1M recent annual average"
    )

    EMERGING_SQL = f"""
    -- Identify the fastest-growing import/export partners for this section.
    -- NTILE(4) partitions suppliers by growth rate into quartiles (Q1 = fastest).
    -- New entrants (zero early share, significant recent presence) are flagged.

    WITH early_period AS (
        SELECT
            dc.country_name             AS partner,
            dc.iso3_code,
            dc.region,
            SUM(ft.trade_value_usd) / {period_len}.0 AS early_avg_usd
        FROM fact_trade    ft
        JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
        JOIN dim_country   dc  ON ft.partner_id   = dc.country_id
        WHERE dco.section_name   = '{sec_sql}'
          AND ft.trade_value_usd > 0
          {flow_clause}
          AND ft.year BETWEEN {yr_min} AND {early_end}
        GROUP BY dc.country_name, dc.iso3_code, dc.region
    ),

    recent_period AS (
        SELECT
            dc.country_name             AS partner,
            dc.iso3_code,
            dc.region,
            SUM(ft.trade_value_usd) / {period_len}.0 AS recent_avg_usd
        FROM fact_trade    ft
        JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
        JOIN dim_country   dc  ON ft.partner_id   = dc.country_id
        WHERE dco.section_name   = '{sec_sql}'
          AND ft.trade_value_usd > 0
          {flow_clause}
          AND ft.year BETWEEN {recent_start} AND {yr_max}
        GROUP BY dc.country_name, dc.iso3_code, dc.region
    ),

    combined AS (
        SELECT
            COALESCE(r.partner,   e.partner)   AS partner,
            COALESCE(r.iso3_code, e.iso3_code) AS iso3_code,
            COALESCE(e.region, r.region, 'Other') AS region,
            COALESCE(e.early_avg_usd,  0)      AS early_avg_usd,
            COALESCE(r.recent_avg_usd, 0)      AS recent_avg_usd,
            COALESCE(e.early_avg_usd, 0) = 0   AS is_new_entrant,
            -- Growth rate: NULL for new entrants (no denominator)
            CASE
                WHEN COALESCE(e.early_avg_usd, 0) > 0
                THEN ROUND(
                    (COALESCE(r.recent_avg_usd, 0) - e.early_avg_usd)
                    / e.early_avg_usd * 100.0, 1)
            END AS growth_pct
        FROM recent_period  r
        FULL OUTER JOIN early_period e ON r.partner = e.partner
        -- Only keep suppliers with meaningful recent presence
        WHERE COALESCE(r.recent_avg_usd, 0) > 1e6
    ),

    with_ntile AS (
        SELECT *,
            -- Q1 = fastest growing 25%; Q4 = slowest / declining 25%
            NTILE(4) OVER (ORDER BY growth_pct DESC NULLS LAST) AS growth_q
        FROM combined
    )

    SELECT
        partner,
        iso3_code,
        region,
        is_new_entrant,
        ROUND(early_avg_usd  / 1e6, 1) AS early_avg_m,
        ROUND(recent_avg_usd / 1e6, 1) AS recent_avg_m,
        growth_pct,
        growth_q
    FROM with_ntile
    -- Rank: new entrants by recent value, then existing by growth rate desc
    ORDER BY
        CASE WHEN is_new_entrant THEN recent_avg_usd ELSE 0 END DESC,
        COALESCE(growth_pct, -9999)                              DESC
    LIMIT 10
    """

    emerging_df = query_uncached(EMERGING_SQL)

    if emerging_df.empty:
        st.info("Not enough data for period comparison.")
    else:
        # Display labels
        Q_LABELS = {1: "Q1 — Top 25%", 2: "Q2 — Upper 50%",
                    3: "Q3 — Lower 50%", 4: "Q4 — Bottom 25%"}

        emerging_df["growth_label"] = emerging_df.apply(
            lambda r: "New entrant" if r["is_new_entrant"]
                      else (f"{r['growth_pct']:+.1f}%" if r["growth_pct"] is not None else "N/A"),
            axis=1,
        )
        emerging_df["quartile_label"] = emerging_df.apply(
            lambda r: "New" if r["is_new_entrant"]
                      else Q_LABELS.get(int(r["growth_q"]), "—") if r["growth_q"] is not None
                      else "—",
            axis=1,
        )

        display_cols = [
            "quartile_label", "partner", "iso3_code", "region",
            "early_avg_m", "recent_avg_m", "growth_label",
        ]
        st.dataframe(
            emerging_df[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "quartile_label": st.column_config.TextColumn(
                    "Growth Tier", width="medium"),
                "partner":        st.column_config.TextColumn("Partner"),
                "iso3_code":      st.column_config.TextColumn("ISO3", width="small"),
                "region":         st.column_config.TextColumn("Region"),
                "early_avg_m":    st.column_config.NumberColumn(
                    f"Early avg/yr ({yr_min}–{early_end})",
                    format="$%.1fM"),
                "recent_avg_m":   st.column_config.NumberColumn(
                    f"Recent avg/yr ({recent_start}–{yr_max})",
                    format="$%.1fM"),
                "growth_label":   st.column_config.TextColumn(
                    "Growth Rate", width="medium"),
            },
        )

st.divider()

# ---------------------------------------------------------------------------
# Chapter Breakdown (expandable)
# ---------------------------------------------------------------------------

CHAPTER_SQL = f"""
-- All HS chapters within the selected section, ranked by total trade value

WITH chapter_totals AS (
    SELECT
        dco.hs_chapter,
        dco.description,
        ROUND(SUM(CASE WHEN ft.flow_direction IN ('Import', 'Re-import')
                       THEN ft.trade_value_usd ELSE 0 END) / 1e9, 3) AS imports_b,
        ROUND(SUM(CASE WHEN ft.flow_direction IN ('Export', 'Re-export')
                       THEN ft.trade_value_usd ELSE 0 END) / 1e9, 3) AS exports_b
    FROM fact_trade    ft
    JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
    WHERE dco.section_name   = '{sec_sql}'
      AND ft.trade_value_usd > 0
      {flow_clause}
      AND ft.year BETWEEN {yr_min} AND {yr_max}
    GROUP BY dco.hs_chapter, dco.description
)
SELECT *,
    ROUND(imports_b + exports_b, 3)            AS total_b,
    ROW_NUMBER() OVER (ORDER BY imports_b + exports_b DESC) AS rank
FROM chapter_totals
ORDER BY total_b DESC
"""

chapter_df = query_uncached(CHAPTER_SQL)

with st.expander(
    f"Chapter Breakdown — {int(s['chapter_count'] or 0)} chapters in this section",
    expanded=True,
):
    if chapter_df.empty:
        st.info("No chapter data for the selected filters.")
    else:
        # Mini bar chart of all chapters
        ch_sorted = chapter_df.sort_values("total_b", ascending=True)

        fig_ch = go.Figure()
        if flow_option in ("Both", "Imports"):
            fig_ch.add_trace(go.Bar(
                y=ch_sorted["description"], x=ch_sorted["imports_b"],
                name="Imports", orientation="h",
                marker_color=COLORS["import"],
                hovertemplate="Ch.%{customdata} %{y}<br>Imports: $%{x:.3f}B<extra></extra>",
                customdata=ch_sorted["hs_chapter"],
            ))
        if flow_option in ("Both", "Exports"):
            fig_ch.add_trace(go.Bar(
                y=ch_sorted["description"], x=ch_sorted["exports_b"],
                name="Exports", orientation="h",
                marker_color=COLORS["export"],
                hovertemplate="Ch.%{customdata} %{y}<br>Exports: $%{x:.3f}B<extra></extra>",
                customdata=ch_sorted["hs_chapter"],
            ))
        fig_ch.update_layout(
            template=PLOTLY_TEMPLATE,
            barmode="group",
            xaxis=dict(title="USD Billions"),
            yaxis=dict(title=""),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=30, b=40, l=0),
            height=max(300, len(chapter_df) * 38),
        )
        st.plotly_chart(fig_ch, use_container_width=True)

        st.markdown("**Drill down — top 5 partners for a specific chapter:**")

        chapter_options = [
            f"Ch.{row.hs_chapter} — {row.description}  (${row.total_b:.3f}B)"
            for row in chapter_df.itertuples()
        ]
        selected_chapter_label = st.selectbox(
            "Select chapter",
            options=chapter_options,
            label_visibility="collapsed",
        )
        selected_chap_idx = chapter_options.index(selected_chapter_label)
        selected_hs       = chapter_df.iloc[selected_chap_idx]["hs_chapter"]

        TOP5_SQL = f"""
        SELECT
            dc.country_name,
            dc.iso3_code,
            ROUND(SUM(CASE WHEN ft.flow_direction IN ('Import', 'Re-import')
                           THEN ft.trade_value_usd ELSE 0 END) / 1e9, 4) AS imports_b,
            ROUND(SUM(CASE WHEN ft.flow_direction IN ('Export', 'Re-export')
                           THEN ft.trade_value_usd ELSE 0 END) / 1e9, 4) AS exports_b,
            ROUND(SUM(ft.trade_value_usd) / 1e9, 4)                       AS total_b
        FROM fact_trade    ft
        JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
        JOIN dim_country   dc  ON ft.partner_id   = dc.country_id
        WHERE dco.hs_chapter     = '{selected_hs}'
          AND ft.trade_value_usd > 0
          {flow_clause}
          AND ft.year BETWEEN {yr_min} AND {yr_max}
        GROUP BY dc.country_name, dc.iso3_code
        ORDER BY total_b DESC
        LIMIT 5
        """

        top5_df = query_uncached(TOP5_SQL)

        if not top5_df.empty:
            t1, t2 = st.columns([3, 5])
            with t1:
                st.dataframe(
                    top5_df[["country_name", "iso3_code", "imports_b", "exports_b", "total_b"]],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "country_name": st.column_config.TextColumn("Country"),
                        "iso3_code":    st.column_config.TextColumn("ISO3", width="small"),
                        "imports_b":    st.column_config.NumberColumn("Imports (B)", format="$%.4f"),
                        "exports_b":    st.column_config.NumberColumn("Exports (B)", format="$%.4f"),
                        "total_b":      st.column_config.NumberColumn("Total (B)",   format="$%.4f"),
                    },
                )
            with t2:
                top5_df = top5_df.sort_values("total_b", ascending=True)
                fig_top5 = go.Figure()
                if flow_option in ("Both", "Imports"):
                    fig_top5.add_trace(go.Bar(
                        y=top5_df["country_name"], x=top5_df["imports_b"],
                        name="Imports", orientation="h",
                        marker_color=COLORS["import"],
                    ))
                if flow_option in ("Both", "Exports"):
                    fig_top5.add_trace(go.Bar(
                        y=top5_df["country_name"], x=top5_df["exports_b"],
                        name="Exports", orientation="h",
                        marker_color=COLORS["export"],
                    ))
                fig_top5.update_layout(
                    template=PLOTLY_TEMPLATE,
                    barmode="group",
                    xaxis=dict(title="USD Billions"),
                    yaxis=dict(title=""),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                    margin=dict(t=20, b=20, l=0),
                    height=240,
                )
                st.plotly_chart(fig_top5, use_container_width=True)
