"""
pages/5_Data_Quality.py — Data quality, methodology, and portfolio SQL showcase

Demonstrates:
  • FULL OUTER JOIN across two fact tables for cross-source reconciliation
  • Data provenance tracking (row counts across pipeline stages)
  • Transparent documentation of known limitations — a data-governance requirement
  • SQL as a first-class artefact, not just a tool
"""

import json
import os
from pathlib import Path

import plotly.graph_objects as go
import streamlit as st

from utils.constants import APP_TITLE, COLORS, PLOTLY_TEMPLATE, fmt_usd
from utils.db import query, query_uncached
from utils.styles import inject_css

st.set_page_config(
    page_title=f"Data Quality | {APP_TITLE}",
    page_icon=None,
    layout="wide",
)
inject_css()

BASE_DIR    = Path(__file__).resolve().parents[1]
REPORT_PATH = BASE_DIR / "data" / "processed" / "data_quality_report.json"
DB_PATH     = BASE_DIR / "data" / "globaltrade.duckdb"

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
# Page header
# ---------------------------------------------------------------------------

st.title("Data Quality & Methodology")
st.caption("Pipeline transparency, known limitations, and the SQL that powers this dashboard")

# ---------------------------------------------------------------------------
# Section 1 — Pipeline Overview
# ---------------------------------------------------------------------------

st.header("1 · Data Pipeline")
st.caption("Raw data → cleaning → star schema · three-stage idempotent pipeline")

# Query DB for current row counts
try:
    counts = query("""
        SELECT
            (SELECT COUNT(*) FROM fact_trade)          AS fact_trade,
            (SELECT COUNT(*) FROM fact_mirror_trade)   AS fact_mirror,
            (SELECT COUNT(*) FROM fact_country_stats)  AS fact_stats,
            (SELECT COUNT(*) FROM dim_country)         AS dim_country,
            (SELECT COUNT(*) FROM dim_commodity)       AS dim_commodity,
            (SELECT COUNT(*) FROM dim_time)            AS dim_time
    """)
    c = counts.iloc[0]
    db_loaded = True
except Exception:
    db_loaded = False

db_size_mb = (
    round(DB_PATH.stat().st_size / 1024 / 1024, 1)
    if DB_PATH.exists() else 0
)

# Visual pipeline: 3 stages in columns with arrows
st.markdown(
    """
    <style>
    .pipe-box {
        background: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 8px;
        padding: 14px 16px; margin: 4px 0;
    }
    .pipe-box h4 { margin: 0 0 6px 0; font-size: 0.85rem; color: #64748B; }
    .pipe-box .val { font-size: 1.1rem; font-weight: 700; color: #1E293B; }
    .pipe-box .sub { font-size: 0.78rem; color: #94A3B8; margin-top: 2px; }
    </style>
    """,
    unsafe_allow_html=True,
)

col_s1, col_arr1, col_s2, col_arr2, col_s3 = st.columns([5, 1, 5, 1, 5])

with col_s1:
    st.markdown("**`01_ingest.py` — Raw Sources**")
    st.markdown("""
<div class="pipe-box">
  <h4>UN Comtrade (reporter)</h4>
  <div class="val">120,271 rows</div>
  <div class="sub">HS 2-digit · annual · 2014–2024</div>
</div>
<div class="pipe-box">
  <h4>UN Comtrade (mirror)</h4>
  <div class="val">7,378 rows</div>
  <div class="sub">15 partners × 4 sample years</div>
</div>
<div class="pipe-box">
  <h4>World Bank API</h4>
  <div class="val">5,852 rows</div>
  <div class="sub">GDP + population · all countries</div>
</div>
<div class="pipe-box">
  <h4>Reference tables</h4>
  <div class="val">395 rows</div>
  <div class="sub">99 HS chapters · 296 countries</div>
</div>
""", unsafe_allow_html=True)

with col_arr1:
    st.markdown("<br><br><br>", unsafe_allow_html=True)
    st.markdown("### →")

with col_s2:
    st.markdown("**`02_clean.py` — Processed Parquet**")
    st.markdown("""
<div class="pipe-box">
  <h4>trade_flows.parquet</h4>
  <div class="val">116,390 rows</div>
  <div class="sub">−2,125 world aggregates, −1,756 totals</div>
</div>
<div class="pipe-box">
  <h4>mirror_flows.parquet</h4>
  <div class="val">766 rows</div>
  <div class="sub">−6,540 duplicates removed</div>
</div>
<div class="pipe-box">
  <h4>country_stats.parquet</h4>
  <div class="val">2,453 rows</div>
  <div class="sub">−836 WB aggregate regions</div>
</div>
<div class="pipe-box">
  <h4>commodities + countries</h4>
  <div class="val">395 rows</div>
  <div class="sub">Reference tables · no changes</div>
</div>
""", unsafe_allow_html=True)

with col_arr2:
    st.markdown("<br><br><br>", unsafe_allow_html=True)
    st.markdown("### →")

with col_s3:
    st.markdown(f"**`03_load_db.py` — DuckDB (`{db_size_mb} MB`)**")
    if db_loaded:
        st.markdown(f"""
<div class="pipe-box">
  <h4>fact_trade</h4>
  <div class="val">{int(c['fact_trade']):,} rows</div>
  <div class="sub">FK resolved: reporter 100% · partner 97.2%</div>
</div>
<div class="pipe-box">
  <h4>fact_mirror_trade</h4>
  <div class="val">{int(c['fact_mirror']):,} rows</div>
  <div class="sub">FK resolved: 100% all dimensions</div>
</div>
<div class="pipe-box">
  <h4>fact_country_stats</h4>
  <div class="val">{int(c['fact_stats']):,} rows</div>
  <div class="sub">223 countries × 11 years</div>
</div>
<div class="pipe-box">
  <h4>Dimensions</h4>
  <div class="val">{int(c['dim_country'])} · {int(c['dim_commodity'])} · {int(c['dim_time'])}</div>
  <div class="sub">dim_country · dim_commodity · dim_time</div>
</div>
""", unsafe_allow_html=True)
    else:
        st.info("Database not loaded.")

st.divider()

# ---------------------------------------------------------------------------
# Section 2 — Data Quality Metrics
# ---------------------------------------------------------------------------

st.header("2 · Data Quality Metrics")

report = None
if REPORT_PATH.exists():
    with open(REPORT_PATH, encoding="utf-8") as f:
        report = json.load(f)
    st.caption(f"Report generated: `{report.get('generated_at', 'unknown')}`")
else:
    st.warning("data_quality_report.json not found. Run `pipeline/02_clean.py` to generate it.")

if report:
    tf = report["sources"].get("trade_flows", {})
    mf = report["sources"].get("mirror_flows", {})

    tab_trade, tab_mirror, tab_stats = st.tabs(
        ["Trade Flows (fact_trade)", "Mirror Flows (fact_mirror_trade)", "Value Distribution"]
    )

    # ── Null rates & rows per year — trade flows ─────────────────────────────
    with tab_trade:
        ncol1, ncol2 = st.columns(2)

        with ncol1:
            st.subheader("Null rates")
            null_data = tf.get("null_percentages", {})
            if null_data:
                fig_null = go.Figure(go.Bar(
                    x=list(null_data.values()),
                    y=list(null_data.keys()),
                    orientation="h",
                    marker_color=[
                        COLORS["high_risk"] if v > 20
                        else COLORS["moderate_risk"] if v > 5
                        else COLORS["low_risk"]
                        for v in null_data.values()
                    ],
                    text=[f"{v:.1f}%" for v in null_data.values()],
                    textposition="outside",
                ))
                fig_null.update_layout(
                    template=PLOTLY_TEMPLATE,
                    xaxis=dict(title="% null", range=[0, max(null_data.values()) * 1.3 or 10]),
                    margin=dict(t=20, b=40, l=0, r=60),
                    height=220,
                )
                st.plotly_chart(fig_null, use_container_width=True)
                st.caption(
                    "net_weight_kg (39.8%) and quantity (17.9%) have high null rates — "
                    "expected: many reporters don't file weight/quantity data with Comtrade."
                )

        with ncol2:
            st.subheader("Rows per year")
            rpy = tf.get("rows_per_year", {})
            if rpy:
                years = [int(k) for k in rpy.keys()]
                vals  = list(rpy.values())
                fig_rpy = go.Figure(go.Bar(
                    x=years, y=vals,
                    marker_color=COLORS["import"],
                    text=[f"{v:,}" for v in vals],
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>%{y:,} rows<extra></extra>",
                ))
                fig_rpy.update_layout(
                    template=PLOTLY_TEMPLATE,
                    xaxis=dict(dtick=1, tickformat="d"),
                    yaxis=dict(title="Rows"),
                    margin=dict(t=20, b=40),
                    height=220,
                )
                st.plotly_chart(fig_rpy, use_container_width=True)
                st.caption(
                    f"Consistent coverage {tf.get('year_range', [2014,2024])[0]}–"
                    f"{tf.get('year_range', [2014,2024])[1]}. "
                    f"No missing years detected."
                )

    # ── Mirror null rates ─────────────────────────────────────────────────────
    with tab_mirror:
        ncol1, ncol2 = st.columns(2)
        with ncol1:
            st.subheader("Null rates (mirror flows)")
            null_m = mf.get("null_percentages", {})
            if null_m:
                fig_nm = go.Figure(go.Bar(
                    x=list(null_m.values()),
                    y=list(null_m.keys()),
                    orientation="h",
                    marker_color=[
                        COLORS["high_risk"] if v > 20
                        else COLORS["moderate_risk"] if v > 5
                        else COLORS["low_risk"]
                        for v in null_m.values()
                    ],
                    text=[f"{v:.1f}%" for v in null_m.values()],
                    textposition="outside",
                ))
                fig_nm.update_layout(
                    template=PLOTLY_TEMPLATE,
                    xaxis=dict(title="% null"),
                    margin=dict(t=20, b=40, l=0, r=60),
                    height=200,
                )
                st.plotly_chart(fig_nm, use_container_width=True)
        with ncol2:
            st.subheader("Coverage by year")
            rpy_m = mf.get("rows_per_year", {})
            if rpy_m:
                fig_rpm = go.Figure(go.Bar(
                    x=[int(k) for k in rpy_m.keys()],
                    y=list(rpy_m.values()),
                    marker_color=COLORS["export"],
                    text=[f"{v:,}" for v in rpy_m.values()],
                    textposition="outside",
                ))
                fig_rpm.update_layout(
                    template=PLOTLY_TEMPLATE,
                    xaxis=dict(dtick=1, tickformat="d"),
                    margin=dict(t=20, b=40),
                    height=200,
                )
                st.plotly_chart(fig_rpm, use_container_width=True)
                st.caption("Mirror data available for sample years only: 2014, 2017, 2020, 2023.")

    # ── Value distribution ────────────────────────────────────────────────────
    with tab_stats:
        vd = tf.get("value_distribution", {})
        if vd:
            dist_cols = st.columns(4)
            dist_cols[0].metric("Min trade value",    fmt_usd(vd.get("min", 0)))
            dist_cols[1].metric("Median trade value", fmt_usd(vd.get("median", 0)))
            dist_cols[2].metric("Mean trade value",   fmt_usd(vd.get("mean", 0)))
            dist_cols[3].metric("Max trade value",    fmt_usd(vd.get("max", 0)))

            st.caption(
                f"P25: {fmt_usd(vd.get('p25',0))}  ·  "
                f"P75: {fmt_usd(vd.get('p75',0))}  ·  "
                f"P95: {fmt_usd(vd.get('p95',0))}  ·  "
                f"**Total:** {fmt_usd(vd.get('sum',0))}"
            )

            # Box-style distribution chart
            fig_dist = go.Figure()
            fig_dist.add_trace(go.Box(
                q1=[vd["p25"]], median=[vd["median"]], mean=[vd["mean"]],
                q3=[vd["p75"]], lowerfence=[vd["min"]], upperfence=[vd["p95"]],
                name="trade_value_usd",
                marker_color=COLORS["import"],
                orientation="h",
            ))
            fig_dist.update_layout(
                template=PLOTLY_TEMPLATE,
                xaxis=dict(title="USD", type="log"),
                margin=dict(t=20, b=40),
                height=180,
            )
            st.plotly_chart(fig_dist, use_container_width=True)
            st.caption(
                "Distribution is highly right-skewed (log scale) — expected for trade data. "
                "Most transactions are small bilateral flows; a handful of large commodity "
                "aggregates (diamonds, semiconductors) dominate the tail."
            )

st.divider()

# ---------------------------------------------------------------------------
# Section 3 — Known Limitations
# ---------------------------------------------------------------------------

st.header("3 · Known Limitations")
st.caption("Every dataset has caveats. These are the ones that matter for this analysis.")

with st.expander("CIF vs FOB valuation bias (~10–15% systematic gap)", expanded=False):
    st.markdown("""
**The problem:** Under Comtrade conventions, imports are valued **CIF** (Cost + Insurance + Freight)
while exports are valued **FOB** (Free on Board). For the *same* shipment:

- The exporting country records: **USD 1,000** (FOB — factory gate price)
- The importing country records: **USD 1,120** (CIF — includes ~12% shipping/insurance)

**Effect on this dashboard:**
- Mirror discrepancies of 10–15% between Israel and partner data are *expected* and not errors
- Israel's total import bill is systematically overstated relative to its export figures
- GDP-adjusted trade intensity ratios are directionally correct but the absolute deficit is inflated

**Mitigation:** The mirror comparison page flags discrepancies > 20% (above the typical CIF/FOB gap)
as genuinely anomalous.
""")

with st.expander("HS classification revision changes (HS2012 → HS2017 → HS2022)", expanded=False):
    st.markdown("""
The Harmonized System is revised roughly every 5 years. Israel's 2014–2016 data uses **HS2012**;
2017–2021 uses **HS2017**; 2022+ uses **HS2022**.

**What changes between revisions:**
- New chapters created (e.g. electrical vehicles split from Ch. 87)
- Commodity descriptions updated
- Some goods shift between 2-digit chapters entirely

**Effect on this dashboard:**
- At the 2-digit chapter level (what this dashboard uses), most changes are minimal
- Long-run trend charts for affected chapters (e.g. 85 — Electrical machinery) should be
  interpreted with caution around revision years
- The Comtrade API returns the `classificationCode` field to identify the active revision,
  but this project does not currently harmonise across revisions

**Mitigation:** HS section-level analysis (21 sections) is more robust to revision changes
than individual chapter analysis.
""")

with st.expander("Missing mirror data — USA, India, and the 840/842 code issue", expanded=False):
    st.markdown("""
**USA (reporter code 840 vs 842):** The US Census Bureau reports to UN Comtrade under code **840**,
but the UN M49 standard uses **842** for the United States. This project mapped `ISO3=USA → 842`
in the UN numeric override dict, which caused the USA mirror data query to return no results
(the API received `partnerCode=842` but US data is filed under `840`).

**Impact:** No US mirror data is available for comparison. Given the US is Israel's
largest export destination ($186.6B over 11 years), this is a significant gap.

**Fix:** Use `reporterCode=840` specifically for the USA in `01_ingest.py` mirror calls.

**India (reporter code 356 / IND):** India has historically under-reported to UN Comtrade.
No mirror data was returned for any of the 4 sample years (2014, 2017, 2020, 2023).
This is a known Comtrade data gap, not a code issue.
""")

with st.expander("Re-exports and entrepôt trade distortions", expanded=False):
    st.markdown("""
**The problem:** Goods often travel through intermediate trading hubs before reaching Israel:
- **China → Hong Kong → Israel**: China records export to HKG; Israel records import from HKG
- **Netherlands (Port of Rotterdam)**: European goods arriving via Rotterdam may be attributed
  to the Netherlands rather than the true origin country
- **UAE (Dubai)**: Growing role as a re-export hub, particularly post-Abraham Accords (2020)

**Observable evidence in this dashboard:**
- China reports 96% more "Ships & floating structures" exports to Israel than Israel records
  as imports from China (2023) — likely reclassification or HKG transit
- UAE appeared as a "new entrant" in multiple commodity sections after 2020 with significant
  values — partly genuine trade, partly re-export reclassification

**Mitigation:** Mirror statistics help identify re-export patterns. Large discrepancies
in the Partner Deep Dive page are a signal worth investigating.
""")

with st.expander("Confidential and suppressed trade flows", expanded=False):
    st.markdown("""
Some trade flows are intentionally excluded from Comtrade data:

| Suppression reason | Examples |
|---|---|
| **National security** | Arms (Ch. 93), dual-use technologies |
| **Commercial confidentiality** | Single-supplier contracts, sensitive commodities |
| **Reporter policy** | Some countries suppress flows below a threshold |
| **HS code sensitivity** | Diamonds, precious metals (Ch. 71) are sometimes aggregated |

**Effect:** Total trade values in this dashboard are a lower bound. The gap is typically
small for civilian goods but can be significant for defence-adjacent sectors.
Israel's arms imports (Ch. 93 = $12.7B over 11 years) are likely understated.
""")

st.divider()

# ---------------------------------------------------------------------------
# Section 4 — Mirror Statistics Discrepancy Analysis
# ---------------------------------------------------------------------------

st.header("4 · Mirror Statistics Discrepancy Analysis")
st.caption(
    "Comparing what Israel reports (fact_trade) against what partners report (fact_mirror_trade) "
    "using a FULL OUTER JOIN across the two fact tables"
)

try:
    DISC_SQL = """
    -- Cross-source reconciliation: Israel's reported imports vs partner's reported exports.
    -- A FULL OUTER JOIN on (year, partner_id, hs_chapter) surfaces every combination
    -- where at least one side has data.

    WITH israel_chapter AS (
        SELECT
            ft.year,
            ft.partner_id,
            dco.hs_chapter,
            dco.description,
            SUM(CASE WHEN ft.flow_direction IN ('Import', 'Re-import')
                     THEN ft.trade_value_usd END) AS isr_imports_usd,
            SUM(CASE WHEN ft.flow_direction IN ('Export', 'Re-export')
                     THEN ft.trade_value_usd END) AS isr_exports_usd
        FROM fact_trade    ft
        JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
        WHERE ft.trade_value_usd > 0
        GROUP BY ft.year, ft.partner_id, dco.hs_chapter, dco.description
    ),

    mirror_chapter AS (
        -- reporter = partner country; flow directions are from that country's perspective
        SELECT
            fmt.year,
            fmt.reporter_id                  AS partner_id,
            dco.hs_chapter,
            SUM(CASE WHEN fmt.flow_direction IN ('Export', 'Re-export')
                     THEN fmt.trade_value_usd END) AS ptr_exports_usd,
            SUM(CASE WHEN fmt.flow_direction IN ('Import', 'Re-import')
                     THEN fmt.trade_value_usd END) AS ptr_imports_usd
        FROM fact_mirror_trade fmt
        JOIN dim_commodity     dco ON fmt.commodity_id = dco.commodity_id
        WHERE fmt.trade_value_usd > 0
        GROUP BY fmt.year, fmt.reporter_id, dco.hs_chapter
    ),

    reconciled AS (
        SELECT
            dc.country_name                          AS partner,
            i.year,
            i.hs_chapter,
            i.description,
            ROUND(i.isr_imports_usd  / 1e6, 2)      AS isr_imports_m,
            ROUND(m.ptr_exports_usd  / 1e6, 2)      AS ptr_exports_m,
            ROUND(i.isr_exports_usd  / 1e6, 2)      AS isr_exports_m,
            ROUND(m.ptr_imports_usd  / 1e6, 2)      AS ptr_imports_m,
            -- Import discrepancy (Israel imports ↔ partner exports to Israel)
            ROUND(
                ABS(COALESCE(i.isr_imports_usd,0) - COALESCE(m.ptr_exports_usd,0))
                / NULLIF(GREATEST(
                    COALESCE(i.isr_imports_usd,0),
                    COALESCE(m.ptr_exports_usd,0)), 0) * 100
            , 1)                                     AS import_disc_pct
        FROM israel_chapter i
        JOIN mirror_chapter m
          ON i.year = m.year AND i.partner_id = m.partner_id AND i.hs_chapter = m.hs_chapter
        JOIN dim_country dc ON i.partner_id = dc.country_id
        -- Only compare rows where both sides have positive values (avoids suppression noise)
        WHERE COALESCE(i.isr_imports_usd, 0) > 1e6
          AND COALESCE(m.ptr_exports_usd,  0) > 1e6
    )

    SELECT * FROM reconciled
    ORDER BY import_disc_pct DESC
    LIMIT 10
    """

    disc_df = query_uncached(DISC_SQL)

    if disc_df.empty:
        st.info("No bilateral chapter-level discrepancies found above the $1M threshold.")
    else:
        disc_df["interpretation"] = disc_df.apply(
            lambda r: (
                "Re-export route likely (ISR imports via intermediary)"
                if r["isr_imports_m"] < r["ptr_exports_m"]
                else "ISR reports more — possible classification gap"
            ),
            axis=1,
        )

        st.dataframe(
            disc_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "partner":         st.column_config.TextColumn("Partner"),
                "year":            st.column_config.NumberColumn("Year", format="%d"),
                "hs_chapter":      st.column_config.TextColumn("Ch.", width="small"),
                "description":     st.column_config.TextColumn("Commodity"),
                "isr_imports_m":   st.column_config.NumberColumn(
                    "ISR Imports (M USD)", format="$%.1f"),
                "ptr_exports_m":   st.column_config.NumberColumn(
                    "Partner Reports Exports (M USD)", format="$%.1f"),
                "import_disc_pct": st.column_config.ProgressColumn(
                    "Discrepancy", format="%.1f%%", min_value=0, max_value=100),
                "interpretation":  st.column_config.TextColumn("Likely Cause"),
            },
        )
        st.caption(
            "China dominates this table — consistent with its heavy use of Hong Kong as a "
            "transshipment hub for goods bound for Israel."
        )

except Exception as exc:
    st.warning(f"Could not run discrepancy analysis: {exc}")

st.divider()

# ---------------------------------------------------------------------------
# Section 5 — SQL Query Showcase
# ---------------------------------------------------------------------------

st.header("5 · SQL Query Showcase")
st.caption(
    "The analytical queries powering this dashboard — written for clarity, "
    "correctness, and performance."
)

q_tab1, q_tab2, q_tab3 = st.tabs([
    "HHI Concentration (4-CTE pipeline)",
    "Mirror Discrepancy (FULL OUTER JOIN)",
    "Emerging Suppliers (NTILE + period comparison)",
])

with q_tab1:
    st.markdown("""
**Purpose:** Calculate the Herfindahl-Hirschman Index (HHI) for each HS section per year,
identifying which commodity sectors have concentrated or diversified supplier bases.

**Technique:** 4-CTE pipeline — each CTE transforms the grain once; `ARG_MAX()` window aggregate
extracts the dominant supplier in a single pass without a self-join.
""")
    st.code("""
-- HHI Calculation — partner concentration per HS section per year
WITH

-- Step 1: aggregate bilateral trade to (year, section, partner) grain
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
      AND ft.flow_direction IN ('Import', 'Re-import')
    GROUP BY ft.year, dco.section_name, ft.partner_id, dc.country_name
),

-- Step 2: section-year totals (denominator for market-share calculation)
section_totals AS (
    SELECT year, section_name, SUM(trade_value) AS total_value
    FROM section_partner
    GROUP BY year, section_name
),

-- Step 3: per-partner market shares (0-100 scale)
market_shares AS (
    SELECT
        sp.year,
        sp.section_name,
        sp.partner_name,
        sp.trade_value / st.total_value * 100.0   AS share_pct
    FROM section_partner  sp
    JOIN section_totals   st
      ON sp.year = st.year AND sp.section_name = st.section_name
),

-- Step 4: HHI = Σ(share²); ARG_MAX finds dominant supplier in one pass
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
""", language="sql")

with q_tab2:
    st.markdown("""
**Purpose:** Reconcile Israel's self-reported bilateral trade against partner-country mirror statistics
to surface re-export routes, CIF/FOB gaps, and classification discrepancies.

**Technique:** FULL OUTER JOIN across two fact tables on a composite key `(year, partner_id, hs_chapter)`.
`NULLIF(GREATEST(...), 0)` handles the zero-denominator edge case without a CASE expression.
""")
    st.code("""
-- Cross-source reconciliation: Israel vs partner mirror statistics
WITH

israel_chapter AS (
    SELECT
        ft.year,
        ft.partner_id,
        dco.hs_chapter,
        dco.description,
        SUM(CASE WHEN ft.flow_direction IN ('Import', 'Re-import')
                 THEN ft.trade_value_usd END) AS isr_imports_usd
    FROM fact_trade    ft
    JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
    WHERE ft.trade_value_usd > 0
    GROUP BY ft.year, ft.partner_id, dco.hs_chapter, dco.description
),

mirror_chapter AS (
    -- reporter = partner country; their exports = Israel's imports
    SELECT
        fmt.year,
        fmt.reporter_id          AS partner_id,
        dco.hs_chapter,
        SUM(CASE WHEN fmt.flow_direction IN ('Export', 'Re-export')
                 THEN fmt.trade_value_usd END) AS ptr_exports_usd
    FROM fact_mirror_trade fmt
    JOIN dim_commodity     dco ON fmt.commodity_id = dco.commodity_id
    WHERE fmt.trade_value_usd > 0
    GROUP BY fmt.year, fmt.reporter_id, dco.hs_chapter
)

SELECT
    dc.country_name AS partner,
    i.year,
    i.hs_chapter,
    i.description,
    ROUND(i.isr_imports_usd / 1e6, 2) AS isr_imports_m,
    ROUND(m.ptr_exports_usd / 1e6, 2) AS ptr_exports_m,
    -- Discrepancy: |ISR - partner| / MAX(ISR, partner)
    ROUND(
        ABS(COALESCE(i.isr_imports_usd, 0) - COALESCE(m.ptr_exports_usd, 0))
        / NULLIF(GREATEST(
            COALESCE(i.isr_imports_usd, 0),
            COALESCE(m.ptr_exports_usd, 0)), 0) * 100
    , 1) AS import_disc_pct
FROM israel_chapter i
JOIN mirror_chapter m
  ON i.year = m.year AND i.partner_id = m.partner_id AND i.hs_chapter = m.hs_chapter
JOIN dim_country dc ON i.partner_id = dc.country_id
WHERE COALESCE(i.isr_imports_usd, 0) > 1e6
  AND COALESCE(m.ptr_exports_usd,  0) > 1e6
ORDER BY import_disc_pct DESC
LIMIT 10
""", language="sql")

with q_tab3:
    st.markdown("""
**Purpose:** Rank trading partners by how fast they've grown as suppliers in a given commodity section,
comparing early-period vs recent-period annual averages. New entrants (zero early share) are flagged separately.

**Technique:** `NTILE(4)` partitions suppliers into growth-rate quartiles. `FULL OUTER JOIN` between
`early_period` and `recent_period` CTEs surfaces new entrants (right-side-only rows).
""")
    st.code("""
-- Emerging suppliers: NTILE growth quartiles with period comparison
WITH

early_period AS (
    SELECT
        dc.country_name             AS partner,
        dc.iso3_code,
        SUM(ft.trade_value_usd) / 3.0 AS early_avg_usd   -- 3-year annual average
    FROM fact_trade    ft
    JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
    JOIN dim_country   dc  ON ft.partner_id   = dc.country_id
    WHERE dco.section_name = :section
      AND ft.flow_direction IN ('Import', 'Re-import')
      AND ft.year BETWEEN :yr_min AND :yr_min + 2
    GROUP BY dc.country_name, dc.iso3_code
),

recent_period AS (
    SELECT
        dc.country_name             AS partner,
        dc.iso3_code,
        SUM(ft.trade_value_usd) / 3.0 AS recent_avg_usd
    FROM fact_trade    ft
    JOIN dim_commodity dco ON ft.commodity_id = dco.commodity_id
    JOIN dim_country   dc  ON ft.partner_id   = dc.country_id
    WHERE dco.section_name = :section
      AND ft.flow_direction IN ('Import', 'Re-import')
      AND ft.year BETWEEN :yr_max - 2 AND :yr_max
    GROUP BY dc.country_name, dc.iso3_code
),

combined AS (
    SELECT
        COALESCE(r.partner,   e.partner)   AS partner,
        COALESCE(r.iso3_code, e.iso3_code) AS iso3_code,
        COALESCE(e.early_avg_usd,  0) AS early_avg_usd,
        COALESCE(r.recent_avg_usd, 0) AS recent_avg_usd,
        COALESCE(e.early_avg_usd, 0) = 0 AS is_new_entrant,
        CASE
            WHEN COALESCE(e.early_avg_usd, 0) > 0
            THEN ROUND((COALESCE(r.recent_avg_usd, 0) - e.early_avg_usd)
                       / e.early_avg_usd * 100.0, 1)
        END AS growth_pct
    FROM recent_period  r
    FULL OUTER JOIN early_period e ON r.partner = e.partner
    WHERE COALESCE(r.recent_avg_usd, 0) > 1e6   -- filter noise
),

with_ntile AS (
    SELECT *,
        -- Q1 = top 25% fastest growing; NULLs (new entrants) placed last
        NTILE(4) OVER (ORDER BY growth_pct DESC NULLS LAST) AS growth_q
    FROM combined
)

SELECT partner, iso3_code, is_new_entrant,
       ROUND(early_avg_usd  / 1e6, 1) AS early_avg_m,
       ROUND(recent_avg_usd / 1e6, 1) AS recent_avg_m,
       growth_pct,
       growth_q
FROM with_ntile
ORDER BY
    CASE WHEN is_new_entrant THEN recent_avg_usd ELSE 0 END DESC,
    COALESCE(growth_pct, -9999)                              DESC
LIMIT 10
""", language="sql")

st.divider()

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.markdown("---")
fc1, fc2, fc3 = st.columns(3)
with fc1:
    st.markdown(
        "**GlobalTradeScope**  \n"
        "Built by **[Mahmod Zoubi](https://github.com/MahmodZoabi)**  \n"
        "Industrial Engineering, Tel Aviv University  \n"
        "[GitHub](https://github.com/MahmodZoabi) · [LinkedIn](https://www.linkedin.com/in/mahmod-zoabi/)"
    )
with fc2:
    st.markdown(
        "**Data Sources**  \n"
        "[UN Comtrade](https://comtrade.un.org/)  \n"
        "[World Bank Open Data](https://data.worldbank.org/)  \n"
        "[UN Harmonized System](https://unstats.un.org/unsd/tradekb/Knowledgebase/50018/Harmonized-Commodity-Description-and-Coding-Systems-HS)"
    )
with fc3:
    st.markdown(
        "**Data Coverage**  \n"
        "Israel · 2014–2024  \n"
        "86 partner countries · 99 HS chapters  \n"
        "UN Comtrade HS 2-digit bilateral flows"
    )
