"""
pages/3_Partner_Deep_Dive.py — Bilateral trade deep-dive for any partner country

Demonstrates:
  • CTEs for multi-grain aggregation (annual totals, commodity breakdown, mirror join)
  • FULL OUTER JOIN between Israel-reported and partner-reported flows
  • Window functions: ROW_NUMBER for top-N ranking, LAG for YoY deltas
  • Conditional rendering based on data availability
"""

import plotly.graph_objects as go
import streamlit as st

from utils.constants import (
    APP_TITLE, COLORS, PLOTLY_TEMPLATE, SECTION_COLORS, fmt_pct, fmt_usd,
)
from utils.db import query, query_uncached
from utils.nav import render_nav
from utils.styles import inject_css

st.set_page_config(
    page_title=f"Partner Deep Dive | {APP_TITLE}",
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
# Load country list (static — cache OK)
# ---------------------------------------------------------------------------

partners_df = query("""
    SELECT
        dc.country_id,
        dc.country_name,
        dc.iso3_code,
        COALESCE(dc.region, 'Unknown') AS region,
        COALESCE(dc.income_group, '')  AS income_group,
        ROUND(SUM(ft.trade_value_usd) / 1e9, 2) AS total_trade_b
    FROM dim_country dc
    JOIN fact_trade  ft ON dc.country_id = ft.partner_id
    WHERE ft.trade_value_usd > 0
    GROUP BY dc.country_id, dc.country_name, dc.iso3_code, dc.region, dc.income_group
    ORDER BY total_trade_b DESC
""")

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("GlobalTradeScope")
    st.caption("Israel bilateral trade intelligence · 2014 – 2024")
    st.divider()

    st.header("Select Partner")

    # Build display labels sorted by trade value (already sorted)
    country_options = [
        f"{row.country_name} ({row.iso3_code})"
        for row in partners_df.itertuples()
    ]
    selected_label = st.selectbox(
        "Country",
        options=country_options,
        index=0,
        help="Countries sorted by total bilateral trade value (2014–2024)",
    )
    selected_idx   = country_options.index(selected_label)
    selected_row   = partners_df.iloc[selected_idx]
    partner_id     = int(selected_row["country_id"])
    partner_name   = selected_row["country_name"]
    partner_iso3   = selected_row["iso3_code"]
    partner_region = selected_row["region"]

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

    st.divider()
    st.caption(f"**Region:** {partner_region}")
    st.caption(f"**Income group:** {selected_row['income_group'] or '—'}")
    st.caption(f"**Total trade:** ${selected_row['total_trade_b']:.1f}B (all years)")
    st.divider()
    st.caption("Built by Mahmod Zoubi")
    st.markdown("[GitHub](https://github.com/MahmodZoabi) · [LinkedIn](https://www.linkedin.com/in/mahmod-zoabi/)")

# ---------------------------------------------------------------------------
# Page title
# ---------------------------------------------------------------------------

st.title(partner_name)
st.caption(f"Bilateral trade with Israel · {yr_min}–{yr_max} · {partner_region}")

# ---------------------------------------------------------------------------
# SQL 1 — Annual bilateral totals + LAG for YoY delta
# ---------------------------------------------------------------------------

ANNUAL_SQL = f"""
-- Annual imports, exports, and balance for the selected partner.
-- LAG window function computes year-on-year deltas inline.

WITH annual_raw AS (
    SELECT
        year,
        SUM(CASE WHEN flow_direction IN ('Import', 'Re-import')
                 THEN trade_value_usd ELSE 0 END) AS imports_usd,
        SUM(CASE WHEN flow_direction IN ('Export', 'Re-export')
                 THEN trade_value_usd ELSE 0 END) AS exports_usd,
        COUNT(DISTINCT
            CASE WHEN flow_direction IN ('Import', 'Re-import')
                 THEN commodity_id END)           AS import_chapters,
        COUNT(DISTINCT
            CASE WHEN flow_direction IN ('Export', 'Re-export')
                 THEN commodity_id END)           AS export_chapters
    FROM fact_trade
    WHERE partner_id    = {partner_id}
      AND trade_value_usd > 0
      AND year BETWEEN {yr_min} AND {yr_max}
    GROUP BY year
),

annual_with_lag AS (
    SELECT
        year,
        imports_usd,
        exports_usd,
        exports_usd - imports_usd                AS balance_usd,
        imports_usd + exports_usd                AS total_usd,
        import_chapters,
        export_chapters,
        LAG(imports_usd) OVER (ORDER BY year)    AS prev_imports_usd,
        LAG(exports_usd) OVER (ORDER BY year)    AS prev_exports_usd
    FROM annual_raw
)

SELECT
    year,
    imports_usd,
    exports_usd,
    balance_usd,
    total_usd,
    import_chapters,
    export_chapters,
    CASE WHEN prev_imports_usd > 0
         THEN ROUND((imports_usd - prev_imports_usd) / prev_imports_usd * 100, 1)
         END AS imports_yoy_pct,
    CASE WHEN prev_exports_usd > 0
         THEN ROUND((exports_usd - prev_exports_usd) / prev_exports_usd * 100, 1)
         END AS exports_yoy_pct
FROM annual_with_lag
ORDER BY year
"""

annual_df = query_uncached(ANNUAL_SQL)

# ---------------------------------------------------------------------------
# KPI Cards
# ---------------------------------------------------------------------------

st.subheader("Summary")

if annual_df.empty:
    st.info(f"No trade data found for {partner_name} in the selected year range.")
    st.stop()

total_imports  = float(annual_df["imports_usd"].sum())
total_exports  = float(annual_df["exports_usd"].sum())
trade_balance  = total_exports - total_imports

# YoY for the latest year in range
latest = annual_df[annual_df["year"] == annual_df["year"].max()]

def _yoy_str(col: str) -> str | None:
    val = latest[col].iloc[0] if not latest.empty else None
    if val is None:
        return None
    return f"{val:+.1f}%"

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Bilateral Trade",   fmt_usd(total_imports + total_exports))
k2.metric("Imports from Partner",    fmt_usd(total_imports),
          delta=_yoy_str("imports_yoy_pct"), delta_color="inverse",
          help=f"YoY change in {annual_df['year'].max()}")
k3.metric("Exports to Partner",      fmt_usd(total_exports),
          delta=_yoy_str("exports_yoy_pct"),
          help=f"YoY change in {annual_df['year'].max()}")
k4.metric("Trade Balance",
          fmt_usd(trade_balance),
          help="Exports − Imports. Negative = trade deficit with this partner.")

# ---------------------------------------------------------------------------
# Macro context (from World Bank via fact_country_stats)
# ---------------------------------------------------------------------------

macro_df = query_uncached(f"""
    SELECT fcs.year, fcs.gdp_usd, fcs.population, fcs.gdp_per_capita
    FROM fact_country_stats fcs
    WHERE fcs.country_id = {partner_id}
    ORDER BY fcs.year DESC
    LIMIT 1
""")

if not macro_df.empty:
    m = macro_df.iloc[0]
    with st.expander(
        f"Macro context — {partner_name} ({int(m['year'])})",
        expanded=False,
    ):
        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("GDP", fmt_usd(m["gdp_usd"]))
        mc2.metric("GDP per Capita", fmt_usd(m["gdp_per_capita"]))
        mc3.metric("Population",
                   f"{m['population'] / 1e6:.1f}M" if m["population"] else "N/A")

st.divider()

# ---------------------------------------------------------------------------
# Chart 1 — Bilateral Trade Trend
# ---------------------------------------------------------------------------

st.subheader("Bilateral Trade Trend")

fig_trend = go.Figure()

fig_trend.add_trace(go.Scatter(
    x=annual_df["year"], y=annual_df["imports_usd"] / 1e9,
    name="Imports", mode="lines+markers",
    line=dict(color=COLORS["import"], width=2.5),
    marker=dict(size=7),
    hovertemplate="<b>%{x}</b><br>Imports: $%{y:.2f}B<extra></extra>",
))
fig_trend.add_trace(go.Scatter(
    x=annual_df["year"], y=annual_df["exports_usd"] / 1e9,
    name="Exports", mode="lines+markers",
    line=dict(color=COLORS["export"], width=2.5),
    marker=dict(size=7),
    hovertemplate="<b>%{x}</b><br>Exports: $%{y:.2f}B<extra></extra>",
))
fig_trend.add_trace(go.Scatter(
    x=annual_df["year"], y=annual_df["balance_usd"] / 1e9,
    name="Balance", mode="lines",
    line=dict(
        color=COLORS["deficit"] if trade_balance < 0 else COLORS["surplus"],
        width=1.8, dash="dot",
    ),
    hovertemplate="<b>%{x}</b><br>Balance: $%{y:.2f}B<extra></extra>",
))
fig_trend.add_hline(y=0, line_color="#94A3B8", line_width=1, line_dash="dash")
fig_trend.update_layout(
    template=PLOTLY_TEMPLATE,
    xaxis=dict(title="Year", dtick=1, tickformat="d"),
    yaxis=dict(title="USD Billions"),
    legend=dict(orientation="h", yanchor="bottom", y=1.02),
    hovermode="x unified",
    margin=dict(t=30, b=50),
    height=360,
)
st.plotly_chart(fig_trend, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Charts 2 & 3 — Top Commodities  |  Partner Share
# ---------------------------------------------------------------------------

col_comm, col_share = st.columns([6, 4], gap="large")

# ── Top 15 commodities exchanged ─────────────────────────────────────────────

with col_comm:
    st.subheader("Top 15 Commodities Exchanged")

    COMMODITY_SQL = f"""
    -- Rank HS chapters by total bilateral value; split by flow direction

    WITH chapter_flows AS (
        SELECT
            dco.hs_chapter,
            dco.description,
            dco.section_name,
            SUM(CASE WHEN ft.flow_direction IN ('Import', 'Re-import')
                     THEN ft.trade_value_usd ELSE 0 END) AS imports_usd,
            SUM(CASE WHEN ft.flow_direction IN ('Export', 'Re-export')
                     THEN ft.trade_value_usd ELSE 0 END) AS exports_usd
        FROM fact_trade    ft
        JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
        WHERE ft.partner_id     = {partner_id}
          AND ft.trade_value_usd > 0
          AND ft.year BETWEEN {yr_min} AND {yr_max}
        GROUP BY dco.hs_chapter, dco.description, dco.section_name
    ),

    ranked AS (
        SELECT *,
            imports_usd + exports_usd AS total_usd,
            ROW_NUMBER() OVER (ORDER BY imports_usd + exports_usd DESC) AS rank
        FROM chapter_flows
    )

    SELECT
        hs_chapter,
        description,
        section_name,
        ROUND(imports_usd / 1e9, 3) AS imports_b,
        ROUND(exports_usd / 1e9, 3) AS exports_b,
        ROUND(total_usd   / 1e9, 3) AS total_b
    FROM ranked
    WHERE rank <= 15
    ORDER BY total_b ASC      -- ascending so largest bar appears at top
    """

    comm_df = query_uncached(COMMODITY_SQL)

    if comm_df.empty:
        st.info("No commodity data for the selected filters.")
    else:
        # Colour each bar by section
        def _sec_color(sec: str) -> str:
            return SECTION_COLORS.get(sec, COLORS["neutral"])

        fig_comm = go.Figure()
        fig_comm.add_trace(go.Bar(
            y=comm_df["description"],
            x=comm_df["imports_b"],
            name="Imports",
            orientation="h",
            marker_color=COLORS["import"],
            hovertemplate="<b>%{y}</b><br>Imports: $%{x:.3f}B<extra></extra>",
        ))
        fig_comm.add_trace(go.Bar(
            y=comm_df["description"],
            x=comm_df["exports_b"],
            name="Exports",
            orientation="h",
            marker_color=COLORS["export"],
            hovertemplate="<b>%{y}</b><br>Exports: $%{x:.3f}B<extra></extra>",
        ))
        fig_comm.update_layout(
            template=PLOTLY_TEMPLATE,
            barmode="group",
            xaxis=dict(title="USD Billions"),
            yaxis=dict(title=""),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(t=30, b=50, l=0),
            height=500,
        )
        st.plotly_chart(fig_comm, use_container_width=True)

# ── Partner's share of Israel's total trade ──────────────────────────────────

with col_share:
    st.subheader(f"{partner_name}'s Share of Israel's Trade")

    SHARE_SQL = f"""
    -- Partner's import and export share of Israel's totals, by year

    WITH israel_totals AS (
        SELECT
            year,
            SUM(CASE WHEN flow_direction IN ('Import', 'Re-import')
                     THEN trade_value_usd ELSE 0 END) AS total_imports,
            SUM(CASE WHEN flow_direction IN ('Export', 'Re-export')
                     THEN trade_value_usd ELSE 0 END) AS total_exports
        FROM fact_trade
        WHERE trade_value_usd > 0
          AND year BETWEEN {yr_min} AND {yr_max}
        GROUP BY year
    ),

    partner_totals AS (
        SELECT
            year,
            SUM(CASE WHEN flow_direction IN ('Import', 'Re-import')
                     THEN trade_value_usd ELSE 0 END) AS partner_imports,
            SUM(CASE WHEN flow_direction IN ('Export', 'Re-export')
                     THEN trade_value_usd ELSE 0 END) AS partner_exports
        FROM fact_trade
        WHERE partner_id    = {partner_id}
          AND trade_value_usd > 0
          AND year BETWEEN {yr_min} AND {yr_max}
        GROUP BY year
    )

    SELECT
        pt.year,
        ROUND(pt.partner_imports / NULLIF(it.total_imports, 0) * 100, 2) AS import_share_pct,
        ROUND(pt.partner_exports / NULLIF(it.total_exports, 0) * 100, 2) AS export_share_pct
    FROM partner_totals pt
    JOIN israel_totals  it ON pt.year = it.year
    ORDER BY pt.year
    """

    share_df = query_uncached(SHARE_SQL)

    if share_df.empty:
        st.info("No share data for the selected filters.")
    else:
        fig_share = go.Figure()
        fig_share.add_trace(go.Scatter(
            x=share_df["year"], y=share_df["import_share_pct"],
            name="Import share", mode="lines+markers",
            fill="tozeroy",
            line=dict(color=COLORS["import"], width=2),
            fillcolor="rgba(37, 99, 235, 0.10)",
            hovertemplate="<b>%{x}</b><br>Import share: %{y:.1f}%<extra></extra>",
        ))
        fig_share.add_trace(go.Scatter(
            x=share_df["year"], y=share_df["export_share_pct"],
            name="Export share", mode="lines+markers",
            fill="tozeroy",
            line=dict(color=COLORS["export"], width=2),
            fillcolor="rgba(22, 163, 74, 0.10)",
            hovertemplate="<b>%{x}</b><br>Export share: %{y:.1f}%<extra></extra>",
        ))
        fig_share.update_layout(
            template=PLOTLY_TEMPLATE,
            xaxis=dict(title="Year", dtick=1, tickformat="d"),
            yaxis=dict(title="% of Israel's total", ticksuffix="%"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            hovermode="x unified",
            margin=dict(t=30, b=50),
            height=500,
        )
        st.plotly_chart(fig_share, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Mirror Statistics Comparison
# ---------------------------------------------------------------------------

st.subheader("Mirror Statistics Comparison")

MIRROR_CHECK_SQL = f"""
    SELECT COUNT(*) AS n FROM fact_mirror_trade
    WHERE reporter_id = {partner_id}
"""
mirror_count = int(query_uncached(MIRROR_CHECK_SQL).iloc[0]["n"])

if mirror_count == 0:
    st.info(
        f"No mirror statistics available for **{partner_name}**. "
        "Mirror data is available for: China, Germany, Turkey, United Kingdom, Italy, South Korea.",
    )
else:
    MIRROR_SQL = f"""
    -- Compare what Israel reports vs what {partner_name} reports for the same flows.
    -- Israel imports ↔ partner exports to Israel  (should match if no CIF/FOB gap)
    -- Israel exports ↔ partner imports from Israel

    WITH israel_side AS (
        SELECT
            year,
            SUM(CASE WHEN flow_direction IN ('Import', 'Re-import')
                     THEN trade_value_usd ELSE 0 END) AS isr_imports_usd,
            SUM(CASE WHEN flow_direction IN ('Export', 'Re-export')
                     THEN trade_value_usd ELSE 0 END) AS isr_exports_usd
        FROM fact_trade
        WHERE partner_id     = {partner_id}
          AND trade_value_usd > 0
        GROUP BY year
    ),

    mirror_side AS (
        -- reporter = {partner_name}, partner = Israel
        -- partner's "Export" flows = Israel's "Import" (they sent it to Israel)
        -- partner's "Import" flows = Israel's "Export" (they received it from Israel)
        SELECT
            year,
            SUM(CASE WHEN flow_direction IN ('Export', 'Re-export')
                     THEN trade_value_usd ELSE 0 END) AS ptr_exports_to_isr,
            SUM(CASE WHEN flow_direction IN ('Import', 'Re-import')
                     THEN trade_value_usd ELSE 0 END) AS ptr_imports_from_isr
        FROM fact_mirror_trade
        WHERE reporter_id    = {partner_id}
          AND trade_value_usd > 0
        GROUP BY year
    ),

    comparison AS (
        SELECT
            m.year,
            COALESCE(i.isr_imports_usd, 0)    AS isr_imports_usd,
            COALESCE(m.ptr_exports_to_isr, 0)  AS ptr_exports_to_isr,
            COALESCE(i.isr_exports_usd, 0)    AS isr_exports_usd,
            COALESCE(m.ptr_imports_from_isr, 0) AS ptr_imports_from_isr,

            -- Import discrepancy: |ISR imports - partner exports to ISR| / MAX
            CASE WHEN GREATEST(COALESCE(i.isr_imports_usd,0),
                               COALESCE(m.ptr_exports_to_isr,0)) > 0
                 THEN ROUND(
                     ABS(COALESCE(i.isr_imports_usd,0) - COALESCE(m.ptr_exports_to_isr,0))
                     / GREATEST(COALESCE(i.isr_imports_usd,0), COALESCE(m.ptr_exports_to_isr,0))
                     * 100, 1)
                 END AS import_disc_pct,

            -- Export discrepancy: |ISR exports - partner imports from ISR| / MAX
            CASE WHEN GREATEST(COALESCE(i.isr_exports_usd,0),
                               COALESCE(m.ptr_imports_from_isr,0)) > 0
                 THEN ROUND(
                     ABS(COALESCE(i.isr_exports_usd,0) - COALESCE(m.ptr_imports_from_isr,0))
                     / GREATEST(COALESCE(i.isr_exports_usd,0), COALESCE(m.ptr_imports_from_isr,0))
                     * 100, 1)
                 END AS export_disc_pct
        FROM mirror_side m
        FULL OUTER JOIN israel_side i ON m.year = i.year
        WHERE m.year IS NOT NULL
    )

    SELECT * FROM comparison ORDER BY year
    """

    mirror_df = query_uncached(MIRROR_SQL)

    if mirror_df.empty:
        st.info("Mirror data exists but no matching years found.")
    else:
        # Build display table
        display_mirror = mirror_df.copy()
        display_mirror["isr_imports_b"]        = display_mirror["isr_imports_usd"]    / 1e9
        display_mirror["ptr_exports_b"]        = display_mirror["ptr_exports_to_isr"] / 1e9
        display_mirror["isr_exports_b"]        = display_mirror["isr_exports_usd"]    / 1e9
        display_mirror["ptr_imports_b"]        = display_mirror["ptr_imports_from_isr"] / 1e9
        display_mirror["import_flag"]          = display_mirror["import_disc_pct"].apply(
            lambda x: "!" if (x is not None and x > 20) else ""
        )
        display_mirror["export_flag"]          = display_mirror["export_disc_pct"].apply(
            lambda x: "!" if (x is not None and x > 20) else ""
        )

        n_flagged = (
            (display_mirror["import_disc_pct"].fillna(0) > 20) |
            (display_mirror["export_disc_pct"].fillna(0) > 20)
        ).sum()

        if n_flagged:
            st.warning(
                f"**{n_flagged} year(s)** show discrepancies > 20% — "
                "see flagged rows below.",
            )

        table_cols = [
            "year",
            "import_flag", "isr_imports_b",   "ptr_exports_b",   "import_disc_pct",
            "export_flag", "isr_exports_b",   "ptr_imports_b",   "export_disc_pct",
        ]

        st.dataframe(
            display_mirror[table_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "year":            st.column_config.NumberColumn("Year", format="%d"),
                "import_flag":     st.column_config.TextColumn("", width="small"),
                "isr_imports_b":   st.column_config.NumberColumn(
                    "ISR Reports Imports (B)", format="$%.3f"),
                "ptr_exports_b":   st.column_config.NumberColumn(
                    f"{partner_name} Reports Exports to ISR (B)", format="$%.3f"),
                "import_disc_pct": st.column_config.ProgressColumn(
                    "Import Discrepancy", format="%.1f%%",
                    min_value=0, max_value=100),
                "export_flag":     st.column_config.TextColumn("", width="small"),
                "isr_exports_b":   st.column_config.NumberColumn(
                    "ISR Reports Exports (B)", format="$%.3f"),
                "ptr_imports_b":   st.column_config.NumberColumn(
                    f"{partner_name} Reports Imports from ISR (B)", format="$%.3f"),
                "export_disc_pct": st.column_config.ProgressColumn(
                    "Export Discrepancy", format="%.1f%%",
                    min_value=0, max_value=100),
            },
        )

    # Methodology expander
    with st.expander("Why do Israel's figures differ from the partner's figures?"):
        st.markdown(f"""
Mirror statistics — where two countries independently report the same bilateral trade —
almost never match perfectly. Common reasons for discrepancy:

| Cause | Effect |
|---|---|
| **CIF vs FOB valuation** | Imports are recorded CIF (cost + insurance + freight); exports FOB. The freight/insurance spread inflates the importer's number. Typical gap: 5–15%. |
| **Re-exports** | Goods transiting through a third country get counted twice — once as an export from origin, once as an import by the recipient. |
| **Timing differences** | Shipments crossing year-end are recorded in different years by each customs authority. |
| **Confidentiality suppression** | Either country may suppress sensitive commodity lines (e.g. arms), leaving a partial picture. |
| **Classification differences** | HS code assigned by the exporting country may differ from the importing country's classification. |

The discrepancy percentage here is:
> **|Israel value − {partner_name} value| ÷ MAX(Israel value, {partner_name} value) × 100**

Rows flagged with "!" exceed 20%, which is unusually high and warrants further investigation.
""")
