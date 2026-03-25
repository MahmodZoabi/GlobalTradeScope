"""
03_load_db.py — GlobalTradeScope DuckDB star-schema loader
Reads Parquet files from data/processed/ and loads them into a clean
star-schema database at data/globaltrade.duckdb.

Schema
------
  dim_country       — ISO / World Bank country metadata
  dim_commodity     — HS 2-digit chapters
  dim_time          — 2014-2024 with decade / period labels
  fact_trade        — Israel-as-reporter bilateral flows
  fact_mirror_trade — Partner-as-reporter bilateral flows (mirror)
  fact_country_stats — Annual GDP, population, GDP per capita

The database is deleted and recreated on every run (idempotent clean load).
"""

import logging
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

BASE_DIR      = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
DB_PATH       = BASE_DIR / "data" / "globaltrade.duckdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _pq(filename: str) -> str:
    """Return a DuckDB-safe Parquet path string (forward slashes)."""
    return (PROCESSED_DIR / filename).as_posix()


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL = """
-- ── Dimensions ────────────────────────────────────────────────────────────

CREATE TABLE dim_country (
    country_id   INTEGER      PRIMARY KEY,
    iso3_code    VARCHAR(3),
    iso2_code    VARCHAR(2),
    country_name VARCHAR,
    region       VARCHAR,
    sub_region   VARCHAR,        -- not provided by WB API, reserved for future
    income_group VARCHAR,
    un_numeric   INTEGER,
    latitude     DOUBLE,
    longitude    DOUBLE
);

CREATE TABLE dim_commodity (
    commodity_id INTEGER      PRIMARY KEY,
    hs_chapter   VARCHAR(2)   NOT NULL,
    description  VARCHAR,
    section_code VARCHAR,
    section_name VARCHAR
);

CREATE TABLE dim_time (
    year         INTEGER      PRIMARY KEY,
    decade       VARCHAR,
    period_label VARCHAR
);

-- ── Facts ─────────────────────────────────────────────────────────────────

CREATE TABLE fact_trade (
    trade_id        BIGINT       PRIMARY KEY,
    year            INTEGER      REFERENCES dim_time(year),
    reporter_id     INTEGER      REFERENCES dim_country(country_id),
    partner_id      INTEGER      REFERENCES dim_country(country_id),
    commodity_id    INTEGER      REFERENCES dim_commodity(commodity_id),
    flow_direction  VARCHAR,
    trade_value_usd DOUBLE,
    net_weight_kg   DOUBLE,
    quantity        DOUBLE,
    qty_unit        VARCHAR,
    valuation       VARCHAR
);

CREATE TABLE fact_mirror_trade (
    trade_id        BIGINT       PRIMARY KEY,
    year            INTEGER      REFERENCES dim_time(year),
    reporter_id     INTEGER      REFERENCES dim_country(country_id),
    partner_id      INTEGER      REFERENCES dim_country(country_id),
    commodity_id    INTEGER      REFERENCES dim_commodity(commodity_id),
    flow_direction  VARCHAR,
    trade_value_usd DOUBLE,
    net_weight_kg   DOUBLE,
    quantity        DOUBLE,
    qty_unit        VARCHAR,
    valuation       VARCHAR
);

CREATE TABLE fact_country_stats (
    country_id     INTEGER  REFERENCES dim_country(country_id),
    year           INTEGER  REFERENCES dim_time(year),
    gdp_usd        DOUBLE,
    population     DOUBLE,
    gdp_per_capita DOUBLE,
    PRIMARY KEY (country_id, year)
);
"""


# ---------------------------------------------------------------------------
# Dimension loaders
# ---------------------------------------------------------------------------

def load_dim_time(con: duckdb.DuckDBPyConnection) -> None:
    log.info("  Loading dim_time (2014-2024) …")

    def _period(year: int) -> str:
        if year <= 2019:
            return "Pre-COVID 2014-2019"
        if year <= 2021:
            return "COVID era 2020-2021"
        return "Post-COVID 2022+"

    def _decade(year: int) -> str:
        return "2010s" if year < 2020 else "2020s"

    rows = [
        (yr, _decade(yr), _period(yr))
        for yr in range(2014, 2025)
    ]
    con.executemany(
        "INSERT INTO dim_time VALUES (?, ?, ?)",
        rows,
    )
    log.info("    Inserted %d time dimension rows", len(rows))


def load_dim_country(con: duckdb.DuckDBPyConnection) -> None:
    log.info("  Loading dim_country …")
    pq = _pq("countries.parquet")

    # Verify file exists before hitting DuckDB
    if not (PROCESSED_DIR / "countries.parquet").exists():
        log.warning("    countries.parquet not found — dim_country will be empty")
        return

    con.execute(f"""
        INSERT INTO dim_country
        SELECT
            ROW_NUMBER() OVER (ORDER BY iso3)  AS country_id,
            iso3                               AS iso3_code,
            iso2                               AS iso2_code,
            "name"                             AS country_name,
            region,
            NULL                               AS sub_region,
            income_group,
            TRY_CAST(un_numeric AS INTEGER)    AS un_numeric,
            TRY_CAST(latitude   AS DOUBLE)     AS latitude,
            TRY_CAST(longitude  AS DOUBLE)     AS longitude
        FROM read_parquet('{pq}')
        WHERE iso3 IS NOT NULL
          AND "name" IS NOT NULL
          AND TRIM("name") <> ''
    """)

    n = con.execute("SELECT COUNT(*) FROM dim_country").fetchone()[0]
    log.info("    Inserted %d countries", n)


def load_dim_commodity(con: duckdb.DuckDBPyConnection) -> None:
    log.info("  Loading dim_commodity …")
    pq = _pq("commodities.parquet")

    if not (PROCESSED_DIR / "commodities.parquet").exists():
        log.warning("    commodities.parquet not found — dim_commodity will be empty")
        return

    con.execute(f"""
        INSERT INTO dim_commodity
        SELECT
            ROW_NUMBER() OVER (ORDER BY chapter_code)  AS commodity_id,
            chapter_code                               AS hs_chapter,
            chapter_name                               AS description,
            section_num                                AS section_code,
            section_name
        FROM read_parquet('{pq}')
        WHERE chapter_code IS NOT NULL
    """)

    n = con.execute("SELECT COUNT(*) FROM dim_commodity").fetchone()[0]
    log.info("    Inserted %d commodity chapters", n)


# ---------------------------------------------------------------------------
# Fact loaders (shared logic)
# ---------------------------------------------------------------------------

def _log_fk_resolution(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Log foreign-key resolution rates for reporter_id, partner_id, commodity_id."""
    total, = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    if total == 0:
        return

    rep_null, = con.execute(f"SELECT COUNT(*) FROM {table} WHERE reporter_id IS NULL").fetchone()
    par_null, = con.execute(f"SELECT COUNT(*) FROM {table} WHERE partner_id  IS NULL").fetchone()
    com_null, = con.execute(f"SELECT COUNT(*) FROM {table} WHERE commodity_id IS NULL").fetchone()

    def _pct(n: int) -> str:
        return f"{n:,} ({100*n/total:.1f}%)" if total else str(n)

    log.info("    FK resolution for %s (%s total rows):", table, f"{total:,}")
    log.info("      reporter_id unresolved : %s", _pct(rep_null))
    log.info("      partner_id  unresolved : %s", _pct(par_null))
    log.info("      commodity_id unresolved: %s", _pct(com_null))


_FACT_INSERT_SQL = """
INSERT INTO {table}
SELECT
    {offset} + ROW_NUMBER() OVER ()          AS trade_id,
    tf.year,
    -- Reporter: ISO3 lookup first, fall back to UN numeric code
    COALESCE(
        rc_iso.country_id,
        rc_num.country_id
    )                                        AS reporter_id,
    -- Partner: ISO3 lookup first, fall back to UN numeric code
    COALESCE(
        pc_iso.country_id,
        pc_num.country_id
    )                                        AS partner_id,
    dc.commodity_id,
    tf.flow_direction_simple                 AS flow_direction,
    tf.trade_value_usd,
    tf.net_weight_kg,
    TRY_CAST(tf.quantity AS DOUBLE)          AS quantity,
    tf.qty_unit,
    tf.valuation
FROM read_parquet('{pq}') tf
-- Reporter ISO3 → country_id
LEFT JOIN dim_country rc_iso
       ON UPPER(TRIM(tf.reporter_iso3)) = rc_iso.iso3_code
-- Reporter UN numeric → country_id (fallback)
LEFT JOIN dim_country rc_num
       ON TRY_CAST(tf.reporter_code AS INTEGER) = rc_num.un_numeric
      AND rc_iso.country_id IS NULL
-- Partner ISO3 → country_id
LEFT JOIN dim_country pc_iso
       ON UPPER(TRIM(tf.partner_iso3)) = pc_iso.iso3_code
-- Partner UN numeric → country_id (fallback)
LEFT JOIN dim_country pc_num
       ON TRY_CAST(tf.partner_code AS INTEGER) = pc_num.un_numeric
      AND pc_iso.country_id IS NULL
-- HS chapter → commodity_id
LEFT JOIN dim_commodity dc
       ON LPAD(TRIM(tf.hs_code), 2, '0') = dc.hs_chapter
"""


def load_fact_trade(con: duckdb.DuckDBPyConnection) -> None:
    log.info("  Loading fact_trade …")
    pq_path = PROCESSED_DIR / "trade_flows.parquet"

    if not pq_path.exists():
        log.warning("    trade_flows.parquet not found — fact_trade will be empty")
        return

    con.execute(
        _FACT_INSERT_SQL.format(
            table="fact_trade",
            offset=0,
            pq=pq_path.as_posix(),
        )
    )
    n = con.execute("SELECT COUNT(*) FROM fact_trade").fetchone()[0]
    log.info("    Inserted %s rows into fact_trade", f"{n:,}")
    _log_fk_resolution(con, "fact_trade")


def load_fact_mirror_trade(con: duckdb.DuckDBPyConnection) -> None:
    log.info("  Loading fact_mirror_trade …")
    pq_path = PROCESSED_DIR / "mirror_flows.parquet"

    if not pq_path.exists():
        log.warning("    mirror_flows.parquet not found — fact_mirror_trade will be empty")
        return

    # Offset trade_id to avoid collisions with fact_trade
    con.execute(
        _FACT_INSERT_SQL.format(
            table="fact_mirror_trade",
            offset=10_000_000,
            pq=pq_path.as_posix(),
        )
    )
    n = con.execute("SELECT COUNT(*) FROM fact_mirror_trade").fetchone()[0]
    log.info("    Inserted %s rows into fact_mirror_trade", f"{n:,}")
    _log_fk_resolution(con, "fact_mirror_trade")


def load_fact_country_stats(con: duckdb.DuckDBPyConnection) -> None:
    log.info("  Loading fact_country_stats …")
    pq_path = PROCESSED_DIR / "country_stats.parquet"

    if not pq_path.exists():
        log.warning("    country_stats.parquet not found — fact_country_stats will be empty")
        return

    con.execute(f"""
        INSERT INTO fact_country_stats
        SELECT
            dc.country_id,
            cs.year,
            TRY_CAST(cs.gdp_usd        AS DOUBLE) AS gdp_usd,
            TRY_CAST(cs.population      AS DOUBLE) AS population,
            TRY_CAST(cs.gdp_per_capita  AS DOUBLE) AS gdp_per_capita
        FROM read_parquet('{pq_path.as_posix()}') cs
        JOIN dim_country dc ON UPPER(TRIM(cs.country_iso3)) = dc.iso3_code
        JOIN dim_time    dt ON cs.year = dt.year
        WHERE cs.country_iso3 IS NOT NULL
          AND cs.year IS NOT NULL
        ON CONFLICT (country_id, year) DO NOTHING
    """)

    n = con.execute("SELECT COUNT(*) FROM fact_country_stats").fetchone()[0]
    iso_unmatched = con.execute(f"""
        SELECT COUNT(DISTINCT cs.country_iso3)
        FROM read_parquet('{pq_path.as_posix()}') cs
        LEFT JOIN dim_country dc ON UPPER(TRIM(cs.country_iso3)) = dc.iso3_code
        WHERE dc.country_id IS NULL AND cs.country_iso3 IS NOT NULL
    """).fetchone()[0]

    log.info("    Inserted %s rows into fact_country_stats", f"{n:,}")
    if iso_unmatched:
        log.warning("    %d ISO3 codes in country_stats had no match in dim_country", iso_unmatched)


# ---------------------------------------------------------------------------
# Validation queries
# ---------------------------------------------------------------------------

def run_validation(con: duckdb.DuckDBPyConnection) -> None:
    log.info("=== Validation ===")

    checks = [
        ("dim_country rows",      "SELECT COUNT(*) FROM dim_country"),
        ("dim_commodity rows",    "SELECT COUNT(*) FROM dim_commodity"),
        ("dim_time rows",         "SELECT COUNT(*) FROM dim_time"),
        ("fact_trade total rows", "SELECT COUNT(*) FROM fact_trade"),
        ("fact_mirror_trade rows","SELECT COUNT(*) FROM fact_mirror_trade"),
        ("fact_country_stats rows","SELECT COUNT(*) FROM fact_country_stats"),
        ("Unique years in fact_trade",      "SELECT COUNT(DISTINCT year)         FROM fact_trade"),
        ("Unique partners in fact_trade",   "SELECT COUNT(DISTINCT partner_id)   FROM fact_trade"),
        ("Unique commodities in fact_trade","SELECT COUNT(DISTINCT commodity_id) FROM fact_trade"),
    ]
    for label, sql in checks:
        val = con.execute(sql).fetchone()[0]
        log.info("  %-40s %s", label + ":", f"{val:,}" if isinstance(val, int) else val)

    # Total trade value in billions
    result = con.execute(
        "SELECT SUM(trade_value_usd) / 1e9 FROM fact_trade WHERE trade_value_usd > 0"
    ).fetchone()[0]
    log.info("  %-40s %.2f B USD", "Total positive trade value:", result or 0)

    # Year-by-year trade value
    log.info("  --- Annual trade value (USD billions) ---")
    rows = con.execute("""
        SELECT
            ft.year,
            dt.period_label,
            ROUND(SUM(CASE WHEN ft.flow_direction = 'Import' THEN ft.trade_value_usd ELSE 0 END) / 1e9, 3) AS imports_b,
            ROUND(SUM(CASE WHEN ft.flow_direction = 'Export' THEN ft.trade_value_usd ELSE 0 END) / 1e9, 3) AS exports_b,
            ROUND(SUM(ft.trade_value_usd) / 1e9, 3) AS total_b
        FROM fact_trade ft
        JOIN dim_time dt ON ft.year = dt.year
        WHERE ft.trade_value_usd > 0
        GROUP BY ft.year, dt.period_label
        ORDER BY ft.year
    """).fetchall()
    for yr, period, imp, exp, tot in rows:
        log.info("    %d  %-28s  Imp: %6.2fB  Exp: %6.2fB  Tot: %6.2fB",
                 yr, period, imp or 0, exp or 0, tot or 0)

    # Top 5 import partners
    log.info("  --- Top 5 import partners (all years) ---")
    rows = con.execute("""
        SELECT
            dc.country_name,
            dc.iso3_code,
            ROUND(SUM(ft.trade_value_usd) / 1e9, 3)  AS import_value_b,
            COUNT(DISTINCT ft.year)                   AS years_present
        FROM fact_trade ft
        JOIN dim_country dc ON ft.partner_id = dc.country_id
        WHERE ft.flow_direction = 'Import'
          AND ft.trade_value_usd > 0
        GROUP BY dc.country_name, dc.iso3_code
        ORDER BY import_value_b DESC
        LIMIT 5
    """).fetchall()
    for rank, (name, iso3, val, yrs) in enumerate(rows, 1):
        log.info("    #%d  %-28s (%s)  %.2f B USD  (%d years)", rank, name, iso3, val or 0, yrs)

    # Top 5 export destinations
    log.info("  --- Top 5 export destinations (all years) ---")
    rows = con.execute("""
        SELECT
            dc.country_name,
            dc.iso3_code,
            ROUND(SUM(ft.trade_value_usd) / 1e9, 3)  AS export_value_b,
            COUNT(DISTINCT ft.year)                   AS years_present
        FROM fact_trade ft
        JOIN dim_country dc ON ft.partner_id = dc.country_id
        WHERE ft.flow_direction = 'Export'
          AND ft.trade_value_usd > 0
        GROUP BY dc.country_name, dc.iso3_code
        ORDER BY export_value_b DESC
        LIMIT 5
    """).fetchall()
    for rank, (name, iso3, val, yrs) in enumerate(rows, 1):
        log.info("    #%d  %-28s (%s)  %.2f B USD  (%d years)", rank, name, iso3, val or 0, yrs)

    # Commodity breakdown — top 10 chapters by total value
    log.info("  --- Top 10 HS chapters by total trade value ---")
    rows = con.execute("""
        SELECT
            dc.hs_chapter,
            dc.description,
            ROUND(SUM(ft.trade_value_usd) / 1e9, 3) AS total_b
        FROM fact_trade ft
        JOIN dim_commodity dc ON ft.commodity_id = dc.commodity_id
        WHERE ft.trade_value_usd > 0
        GROUP BY dc.hs_chapter, dc.description
        ORDER BY total_b DESC
        LIMIT 10
    """).fetchall()
    for chap, desc, val in rows:
        log.info("    Ch.%s  %-40s  %.2f B USD", chap, (desc or "")[:40], val or 0)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    # Delete DB for a clean load
    if DB_PATH.exists():
        DB_PATH.unlink()
        log.info("Deleted existing database: %s", DB_PATH.name)

    log.info("Creating database: %s", DB_PATH)
    con = duckdb.connect(str(DB_PATH))

    try:
        log.info("=== Creating schema ===")
        con.execute(DDL)
        log.info("  Schema created (6 tables)")

        log.info("=== Loading dimensions ===")
        load_dim_time(con)
        load_dim_country(con)
        load_dim_commodity(con)

        # ── Patch missing region values not covered by World Bank API ──────
        log.info("  Patching missing dim_country regions …")
        con.execute("""
            UPDATE dim_country
            SET region = 'Middle East & North Africa'
            WHERE iso3_code IN (
                'ARE','BHR','QAT','KWT','OMN','SAU','JOR','LBN',
                'IRQ','IRN','ISR','PSE','SYR','YEM','EGY','LBY',
                'TUN','DZA','MAR'
            )
        """)
        con.execute("""
            UPDATE dim_country
            SET region = 'East Asia & Pacific'
            WHERE iso3_code IN ('TWN','HKG','MAC')
              AND region IS NULL
        """)
        con.execute("""
            UPDATE dim_country
            SET region = 'Europe & Central Asia'
            WHERE iso3_code IN ('XKX','IMN','GIB')
              AND region IS NULL
        """)
        n_patched = con.execute(
            "SELECT COUNT(*) FROM dim_country WHERE region IS NOT NULL"
        ).fetchone()[0]
        n_null = con.execute(
            "SELECT COUNT(*) FROM dim_country WHERE region IS NULL"
        ).fetchone()[0]
        log.info("    Regions set: %d  |  Still NULL: %d", n_patched, n_null)

        log.info("=== Loading fact tables ===")
        load_fact_trade(con)
        load_fact_mirror_trade(con)
        load_fact_country_stats(con)

        run_validation(con)

    finally:
        con.close()
        log.info("Database connection closed.")

    log.info("Load complete → %s", DB_PATH)


if __name__ == "__main__":
    main()
