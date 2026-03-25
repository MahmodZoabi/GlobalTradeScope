"""
02_clean.py — GlobalTradeScope data cleaning & transformation
Reads CSVs from data/raw/ and writes Parquet files to data/processed/.
"""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

RAW_DIR       = Path(__file__).resolve().parents[1] / "data" / "raw"
PROCESSED_DIR = Path(__file__).resolve().parents[1] / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _rp(path: Path) -> str:
    """Short relative path label for log messages."""
    return path.name


def _save_parquet(df: pd.DataFrame, path: Path) -> None:
    df.to_parquet(path, index=False, engine="pyarrow")
    log.info("  Saved %d rows → %s", len(df), _rp(path))


def _null_pct(df: pd.DataFrame, cols: list[str] | None = None) -> dict:
    target = df[cols] if cols else df
    return {
        col: round(float(target[col].isna().mean() * 100), 2)
        for col in target.columns
    }


# ---------------------------------------------------------------------------
# Comtrade column renaming & shared logic
# ---------------------------------------------------------------------------

COMTRADE_RENAME = {
    "refYear":       "year",
    "reporterCode":  "reporter_code",
    "reporterISO":   "reporter_iso3",
    "partnerCode":   "partner_code",
    "partnerISO":    "partner_iso3",
    "cmdCode":       "hs_code",
    "cmdDesc":       "commodity_desc",
    "flowCode":      "flow_code",
    "primaryValue":  "trade_value_usd",
    "netWgt":        "net_weight_kg",
    "qty":           "quantity",
    "qtyUnitAbbr":   "qty_unit",
}

FLOW_CODE_MAP = {
    "M":  "Import",
    "X":  "Export",
    "RM": "Re-import",
    "RX": "Re-export",
}

FLOW_SIMPLE_MAP = {
    "Import":    "Import",
    "Re-import": "Import",
    "Export":    "Export",
    "Re-export": "Export",
}

FLOW_VALUATION = {
    "Import":    "CIF",
    "Re-import": "CIF",
    "Export":    "FOB",
    "Re-export": "FOB",
}

# Comtrade partner codes that represent "World" or undisclosed totals
WORLD_PARTNER_CODES = {"0", "W00", "WLD"}

# HS code values that indicate commodity aggregates (not real chapters)
AGGREGATE_HS_CODES = {"TOTAL", "AG2", "ALL", "AG4", "AG6", "99"}


def _clean_comtrade_df(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """
    Shared cleaning logic for both reporter and mirror Comtrade data.
    Returns a cleaned DataFrame or an empty one if no usable data.
    """
    original_rows = len(df)
    log.info("  [%s] Starting with %d rows", label, original_rows)

    # ── 1. Rename columns (only those that exist) ────────────────────────────
    rename_map = {k: v for k, v in COMTRADE_RENAME.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    log.info("  [%s] Renamed %d columns", label, len(rename_map))

    # ── 2. Remove world / undisclosed partner rows ───────────────────────────
    if "partner_code" in df.columns:
        partner_str = df["partner_code"].astype(str).str.strip()
        world_mask  = partner_str.isin(WORLD_PARTNER_CODES)
        n_world     = world_mask.sum()
        df = df[~world_mask].copy()
        log.info("  [%s] Dropped %d world-aggregate partner rows", label, n_world)
    else:
        log.warning("  [%s] Column 'partner_code' not found", label)

    # ── 3. HS code normalisation ─────────────────────────────────────────────
    if "hs_code" in df.columns:
        df["hs_code"] = df["hs_code"].astype(str).str.strip().str.upper()

        # Drop known aggregate commodity tokens
        agg_mask = df["hs_code"].isin(AGGREGATE_HS_CODES)
        log.info("  [%s] Dropping %d commodity-aggregate rows", label, agg_mask.sum())
        df = df[~agg_mask].copy()

        # Zero-pad numeric codes to 2 digits
        df["hs_code"] = df["hs_code"].apply(
            lambda x: x.zfill(2) if re.match(r"^\d{1,2}$", x) else x
        )

        # Keep only valid 2-digit chapter codes
        valid_hs = df["hs_code"].str.match(r"^\d{2}$")
        n_invalid = (~valid_hs).sum()
        if n_invalid:
            log.info(
                "  [%s] Dropping %d rows with non-2-digit HS codes (sample: %s)",
                label, n_invalid,
                df.loc[~valid_hs, "hs_code"].value_counts().head(5).to_dict(),
            )
        df = df[valid_hs].copy()
    else:
        log.warning("  [%s] Column 'hs_code' not found", label)

    # ── 4. Flow code mapping ─────────────────────────────────────────────────
    if "flow_code" in df.columns:
        unmapped = df["flow_code"].astype(str).str.strip()
        df["flow_code"] = unmapped.map(FLOW_CODE_MAP).fillna(unmapped)

        unknown_flows = df["flow_code"][~df["flow_code"].isin(FLOW_CODE_MAP.values())]
        if not unknown_flows.empty:
            log.warning(
                "  [%s] Unknown flow codes (kept): %s",
                label, unknown_flows.value_counts().to_dict(),
            )

        df["flow_direction_simple"] = df["flow_code"].map(FLOW_SIMPLE_MAP)
        df["valuation"]             = df["flow_code"].map(FLOW_VALUATION)
        log.info("  [%s] Flow breakdown: %s", label,
                 df["flow_code"].value_counts().to_dict())
    else:
        log.warning("  [%s] Column 'flow_code' not found", label)

    # ── 5. Trade value cleaning ───────────────────────────────────────────────
    if "trade_value_usd" in df.columns:
        df["trade_value_usd"] = pd.to_numeric(df["trade_value_usd"], errors="coerce")

        n_null = df["trade_value_usd"].isna().sum()
        n_neg  = (df["trade_value_usd"] < 0).sum()
        n_zero = (df["trade_value_usd"] == 0).sum()

        if n_null:
            log.warning("  [%s] %d rows have non-parseable trade_value_usd (coerced to NaN)", label, n_null)
        if n_neg:
            log.info("  [%s] %d negative trade values (corrections/revisions) — kept", label, n_neg)
        if n_zero:
            log.info("  [%s] %d zero trade values — kept", label, n_zero)
    else:
        log.warning("  [%s] Column 'trade_value_usd' not found", label)

    # ── 6. Deduplicate ───────────────────────────────────────────────────────
    dedup_cols = [c for c in ["year", "partner_code", "hs_code", "flow_code"]
                  if c in df.columns]
    if len(dedup_cols) == 4:
        before = len(df)
        df = df.drop_duplicates(subset=dedup_cols, keep="first")
        n_dupes = before - len(df)
        if n_dupes:
            log.info("  [%s] Removed %d duplicate rows", label, n_dupes)

    # ── 7. Numeric coercions for remaining columns ────────────────────────────
    for col in ["net_weight_kg", "quantity", "year", "reporter_code", "partner_code"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    log.info(
        "  [%s] Final: %d rows (%.1f%% of original)",
        label, len(df), 100 * len(df) / original_rows if original_rows else 0,
    )
    null_report = _null_pct(df, ["trade_value_usd", "net_weight_kg", "quantity"])
    log.info("  [%s] Null rates — %s", label, null_report)

    return df


# ---------------------------------------------------------------------------
# 1. Clean reporter trade flows
# ---------------------------------------------------------------------------

def clean_trade_flows() -> pd.DataFrame | None:
    log.info("=== [1/5] Cleaning reporter trade flows ===")
    src = RAW_DIR / "comtrade_israel_reporter_all.csv"
    out = PROCESSED_DIR / "trade_flows.parquet"

    if not src.exists():
        log.warning("  Source not found: %s — skipping", _rp(src))
        return None

    df = pd.read_csv(src, low_memory=False)
    df = _clean_comtrade_df(df, "reporter")

    if df.empty:
        log.warning("  No rows remaining after cleaning — skipping save")
        return None

    _save_parquet(df, out)
    return df


# ---------------------------------------------------------------------------
# 2. Clean mirror flows
# ---------------------------------------------------------------------------

def clean_mirror_flows() -> pd.DataFrame | None:
    log.info("=== [2/5] Cleaning mirror trade flows ===")
    src = RAW_DIR / "comtrade_mirror_all.csv"
    out = PROCESSED_DIR / "mirror_flows.parquet"

    if not src.exists():
        log.warning("  Source not found: %s — skipping", _rp(src))
        return None

    df = pd.read_csv(src, low_memory=False)
    df = _clean_comtrade_df(df, "mirror")

    if df.empty:
        log.warning("  No rows remaining after cleaning — skipping save")
        return None

    _save_parquet(df, out)
    return df


# ---------------------------------------------------------------------------
# 3. Clean World Bank country stats
# ---------------------------------------------------------------------------

# World Bank aggregate / non-country iso3 codes to exclude
WB_AGGREGATE_CODES = {
    "WLD", "EUU", "HIC", "LIC", "LMC", "UMC", "MIC", "OEC", "FCS", "HPC",
    "IBD", "IBT", "IDA", "IDB", "IDX", "LAC", "EAP", "ECA", "MNA", "SAR",
    "SSA", "NAC", "INX", "PRE", "SST", "TMN", "TSA", "TSS", "EMU", "ARB",
    "CSS", "CEB", "EAR", "EAS", "ECS", "LCN", "LDC", "MEA", "SAS", "SSF",
    "TEA", "TEC", "TLA", "AFE", "AFW", "TBT", "XZN",
}

WB_INDICATOR_RENAME = {
    "NY.GDP.MKTP.CD": "gdp_usd",
    "SP.POP.TOTL":    "population",
}


def clean_country_stats() -> pd.DataFrame | None:
    log.info("=== [3/5] Cleaning World Bank country stats ===")
    out = PROCESSED_DIR / "country_stats.parquet"

    gdp_path = RAW_DIR / "worldbank_gdp_current_usd.csv"
    pop_path  = RAW_DIR / "worldbank_population.csv"

    missing = [p for p in (gdp_path, pop_path) if not p.exists()]
    if missing:
        log.warning("  Missing source files: %s — skipping", [_rp(p) for p in missing])
        return None

    gdp_df = pd.read_csv(gdp_path)
    pop_df = pd.read_csv(pop_path)
    log.info("  Loaded GDP %d rows, population %d rows", len(gdp_df), len(pop_df))

    # Combine long-format frames
    combined = pd.concat([gdp_df, pop_df], ignore_index=True)
    log.info("  Combined long frame: %d rows", len(combined))

    # Drop rows with null values (unfilled years)
    n_before = len(combined)
    combined = combined.dropna(subset=["value"])
    log.info("  Dropped %d null-value rows", n_before - len(combined))

    # Ensure year is numeric
    combined["year"] = pd.to_numeric(combined["year"], errors="coerce")
    combined = combined.dropna(subset=["year"])
    combined["year"] = combined["year"].astype(int)

    # Rename indicator codes to friendly names
    combined["indicator"] = combined["indicator"].map(WB_INDICATOR_RENAME).fillna(combined["indicator"])

    # Remove aggregate / non-country rows
    n_before = len(combined)
    combined = combined[~combined["country_iso3"].isin(WB_AGGREGATE_CODES)].copy()
    log.info("  Dropped %d aggregate-region rows", n_before - len(combined))

    # Pivot: one row per (country, year), columns = gdp_usd, population
    log.info("  Pivoting from long to wide …")
    pivot = combined.pivot_table(
        index=["country_iso3", "country_name", "year"],
        columns="indicator",
        values="value",
        aggfunc="first",
    ).reset_index()
    pivot.columns.name = None

    # Ensure both columns exist even if one indicator had no data
    for col in ("gdp_usd", "population"):
        if col not in pivot.columns:
            pivot[col] = pd.NA
            log.warning("  Column '%s' missing after pivot — filled with NA", col)

    # Calculate GDP per capita
    pivot["gdp_per_capita"] = (
        pd.to_numeric(pivot["gdp_usd"],    errors="coerce") /
        pd.to_numeric(pivot["population"], errors="coerce")
    )

    n_no_percap = pivot["gdp_per_capita"].isna().sum()
    log.info(
        "  GDP per capita: calculated for %d rows, %d rows have NaN (missing GDP or population)",
        pivot["gdp_per_capita"].notna().sum(), n_no_percap,
    )

    log.info(
        "  Final stats: %d rows | %d unique countries | years %d–%d",
        len(pivot),
        pivot["country_iso3"].nunique(),
        int(pivot["year"].min()),
        int(pivot["year"].max()),
    )

    _save_parquet(pivot, out)
    return pivot


# ---------------------------------------------------------------------------
# 4. Clean reference tables
# ---------------------------------------------------------------------------

def clean_reference_tables() -> None:
    log.info("=== [4/5] Cleaning reference tables ===")

    # ── HS chapters ──────────────────────────────────────────────────────────
    hs_src = RAW_DIR / "hs_chapters.csv"
    hs_out = PROCESSED_DIR / "commodities.parquet"
    if hs_src.exists():
        df = pd.read_csv(hs_src)
        df["chapter_int"] = pd.to_numeric(df["chapter_int"], errors="coerce")
        # Ensure chapter_code is zero-padded string
        df["chapter_code"] = df["chapter_int"].apply(
            lambda x: f"{int(x):02d}" if pd.notna(x) else None
        )
        _save_parquet(df, hs_out)
        log.info("  HS chapters: %d chapters across %d sections",
                 len(df), df["section_num"].nunique())
    else:
        log.warning("  %s not found — skipping commodities.parquet", _rp(hs_src))

    # ── Country mapping ──────────────────────────────────────────────────────
    cm_src = RAW_DIR / "country_mapping.csv"
    cm_out = PROCESSED_DIR / "countries.parquet"
    if cm_src.exists():
        df = pd.read_csv(cm_src)
        n_before = len(df)
        # Drop rows with no name (blank entries sometimes returned by WB API)
        df = df.dropna(subset=["name"])
        df = df[df["name"].str.strip() != ""]
        log.info(
            "  Country mapping: %d entries (%d dropped blank)",
            len(df), n_before - len(df),
        )
        log.info(
            "  Coverage: %d with un_numeric mapping, %d with region",
            df["un_numeric"].notna().sum(),
            df["region"].notna().sum(),
        )
        _save_parquet(df, cm_out)
    else:
        log.warning("  %s not found — skipping countries.parquet", _rp(cm_src))


# ---------------------------------------------------------------------------
# 5. Data quality report
# ---------------------------------------------------------------------------

def _distribution_stats(series: pd.Series) -> dict:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return {}
    return {
        "min":    round(float(s.min()), 2),
        "p25":    round(float(s.quantile(0.25)), 2),
        "median": round(float(s.median()), 2),
        "mean":   round(float(s.mean()), 2),
        "p75":    round(float(s.quantile(0.75)), 2),
        "p95":    round(float(s.quantile(0.95)), 2),
        "max":    round(float(s.max()), 2),
        "sum":    round(float(s.sum()), 2),
    }


def _trade_flow_report(parquet_path: Path, label: str) -> dict | None:
    if not parquet_path.exists():
        return None
    df = pd.read_parquet(parquet_path)

    rows_per_year: dict = {}
    if "year" in df.columns:
        rows_per_year = {
            str(int(k)): int(v)
            for k, v in df["year"].value_counts().sort_index().items()
            if pd.notna(k)
        }

    return {
        "source":            label,
        "total_rows":        len(df),
        "year_range":        [
            int(df["year"].min()) if "year" in df.columns and df["year"].notna().any() else None,
            int(df["year"].max()) if "year" in df.columns and df["year"].notna().any() else None,
        ],
        "unique_partners":   int(df["partner_code"].nunique()) if "partner_code" in df.columns else None,
        "unique_hs_codes":   int(df["hs_code"].nunique())      if "hs_code"       in df.columns else None,
        "flow_distribution": (
            df["flow_code"].value_counts().to_dict()
            if "flow_code" in df.columns else {}
        ),
        "null_percentages":  _null_pct(df, [
            c for c in ["trade_value_usd", "net_weight_kg", "quantity", "partner_iso3"]
            if c in df.columns
        ]),
        "value_distribution": (
            _distribution_stats(df["trade_value_usd"])
            if "trade_value_usd" in df.columns else {}
        ),
        "rows_per_year":     rows_per_year,
    }


def generate_quality_report() -> None:
    log.info("=== [5/5] Generating data quality report ===")
    out = PROCESSED_DIR / "data_quality_report.json"

    report: dict = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {},
    }

    # Trade flows
    tf_report = _trade_flow_report(PROCESSED_DIR / "trade_flows.parquet", "reporter")
    if tf_report:
        report["sources"]["trade_flows"] = tf_report

    # Mirror flows
    mf_report = _trade_flow_report(PROCESSED_DIR / "mirror_flows.parquet", "mirror")
    if mf_report:
        report["sources"]["mirror_flows"] = mf_report

    # Country stats
    cs_path = PROCESSED_DIR / "country_stats.parquet"
    if cs_path.exists():
        df = pd.read_parquet(cs_path)
        report["sources"]["country_stats"] = {
            "total_rows":      len(df),
            "unique_countries": int(df["country_iso3"].nunique()),
            "year_range": [
                int(df["year"].min()),
                int(df["year"].max()),
            ],
            "null_percentages": _null_pct(df, ["gdp_usd", "population", "gdp_per_capita"]),
            "gdp_distribution": _distribution_stats(df["gdp_usd"]),
        }

    # Reference tables
    for fname, label in [("commodities.parquet", "hs_chapters"), ("countries.parquet", "country_mapping")]:
        p = PROCESSED_DIR / fname
        if p.exists():
            df = pd.read_parquet(p)
            report["sources"][label] = {"total_rows": len(df), "columns": list(df.columns)}

    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    log.info("  Quality report → %s", _rp(out))

    # Print summary to console
    for src_name, src_data in report["sources"].items():
        rows = src_data.get("total_rows", "?")
        log.info("  %-20s  %s rows", src_name, f"{rows:,}" if isinstance(rows, int) else rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("GlobalTradeScope — data cleaning starting")
    log.info("Raw dir:       %s", RAW_DIR)
    log.info("Processed dir: %s", PROCESSED_DIR)

    # Reference tables first (other steps may reference them later)
    clean_reference_tables()
    clean_trade_flows()
    clean_mirror_flows()
    clean_country_stats()
    generate_quality_report()

    log.info("Data cleaning complete.")


if __name__ == "__main__":
    main()
