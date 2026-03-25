-- =============================================================================
-- GlobalTradeScope — DuckDB Star Schema
-- =============================================================================
-- Star schema for analysing Israel's bilateral trade flows (2014-2024).
--
-- Sources
--   dim_country       ← World Bank country API + manual UN-numeric overrides
--   dim_commodity     ← HS 2-digit chapters (UN classification, 99 chapters)
--   dim_time          ← Generated for 2014-2024
--   fact_trade        ← UN Comtrade (Israel as reporter)
--   fact_mirror_trade ← UN Comtrade (top-15 partners reporting on Israel)
--   fact_country_stats← World Bank GDP / population indicators
--
-- Load order: dimensions first, then facts (foreign keys enforced).
-- =============================================================================


-- =============================================================================
-- DIMENSIONS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_country
-- One row per country / territory.
-- iso3_code is the ISO 3166-1 alpha-3 code used by the World Bank API.
-- un_numeric is the UN M49 / ISO 3166-1 numeric code used by UN Comtrade.
-- sub_region is reserved; the standard WB country endpoint does not expose it.
-- -----------------------------------------------------------------------------
CREATE TABLE dim_country (
    country_id   INTEGER     PRIMARY KEY,  -- surrogate key (ROW_NUMBER)
    iso3_code    VARCHAR(3),               -- ISO 3166-1 alpha-3  e.g. "ISR"
    iso2_code    VARCHAR(2),               -- ISO 3166-1 alpha-2  e.g. "IL"
    country_name VARCHAR,                  -- English display name
    region       VARCHAR,                  -- World Bank macro-region
    sub_region   VARCHAR,                  -- Reserved (not in WB API response)
    income_group VARCHAR,                  -- WB income classification
    un_numeric   INTEGER,                  -- UN M49 / Comtrade numeric code
    latitude     DOUBLE,                   -- Approximate centroid
    longitude    DOUBLE
);


-- -----------------------------------------------------------------------------
-- dim_commodity
-- One row per HS 2-digit chapter (01–99).
-- hs_chapter is zero-padded to 2 characters ("01", "84", etc.).
-- section_code uses Roman numerals (I … XXI) matching the official HS schedule.
-- -----------------------------------------------------------------------------
CREATE TABLE dim_commodity (
    commodity_id INTEGER     PRIMARY KEY,  -- surrogate key (ROW_NUMBER)
    hs_chapter   VARCHAR(2)  NOT NULL,     -- zero-padded 2-digit code  e.g. "84"
    description  VARCHAR,                  -- Chapter short title
    section_code VARCHAR,                  -- Roman numeral section  e.g. "XVI"
    section_name VARCHAR                   -- Section description
);


-- -----------------------------------------------------------------------------
-- dim_time
-- One row per calendar year covered by the dataset (2014-2024).
-- decade and period_label support quick GROUP BY slicing in dashboards.
-- -----------------------------------------------------------------------------
CREATE TABLE dim_time (
    year         INTEGER     PRIMARY KEY,  -- Calendar year  e.g. 2020
    decade       VARCHAR,                  -- "2010s" | "2020s"
    period_label VARCHAR                   -- "Pre-COVID 2014-2019"
                                           -- "COVID era 2020-2021"
                                           -- "Post-COVID 2022+"
);


-- =============================================================================
-- FACTS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- fact_trade
-- Israel-as-reporter bilateral trade flows from UN Comtrade.
-- Each row is one unique (year, partner, HS chapter, flow direction) observation.
-- trade_value_usd can be negative for revision/correction records (kept as-is).
-- valuation: Imports recorded CIF, Exports recorded FOB (Comtrade standard).
-- -----------------------------------------------------------------------------
CREATE TABLE fact_trade (
    trade_id        BIGINT   PRIMARY KEY,            -- surrogate key

    year            INTEGER
        REFERENCES dim_time(year),

    reporter_id     INTEGER                          -- always Israel (ISR / 376)
        REFERENCES dim_country(country_id),

    partner_id      INTEGER                          -- bilateral trade partner
        REFERENCES dim_country(country_id),

    commodity_id    INTEGER                          -- HS 2-digit chapter
        REFERENCES dim_commodity(commodity_id),

    flow_direction  VARCHAR,  -- "Import" | "Export" | "Re-import" | "Re-export"
    trade_value_usd DOUBLE,   -- USD; CIF for imports, FOB for exports
    net_weight_kg   DOUBLE,   -- Net weight in kilograms (may be NULL)
    quantity        DOUBLE,   -- Supplementary quantity (unit varies by chapter)
    qty_unit        VARCHAR,  -- Quantity unit abbreviation  e.g. "KG", "NO"
    valuation       VARCHAR   -- "CIF" | "FOB"
);


-- -----------------------------------------------------------------------------
-- fact_mirror_trade
-- Same structure as fact_trade but the reporter is a trading partner country,
-- not Israel. Used to cross-check Israel's self-reported statistics.
-- reporter_id = the partner country (CHN, DEU, USA …)
-- partner_id  = Israel (ISR)
-- -----------------------------------------------------------------------------
CREATE TABLE fact_mirror_trade (
    trade_id        BIGINT   PRIMARY KEY,            -- surrogate key (offset +10M)

    year            INTEGER
        REFERENCES dim_time(year),

    reporter_id     INTEGER                          -- partner country reporting
        REFERENCES dim_country(country_id),

    partner_id      INTEGER                          -- Israel as the partner
        REFERENCES dim_country(country_id),

    commodity_id    INTEGER
        REFERENCES dim_commodity(commodity_id),

    flow_direction  VARCHAR,
    trade_value_usd DOUBLE,
    net_weight_kg   DOUBLE,
    quantity        DOUBLE,
    qty_unit        VARCHAR,
    valuation       VARCHAR
);


-- -----------------------------------------------------------------------------
-- fact_country_stats
-- Annual macroeconomic indicators for all countries from the World Bank.
-- Compound primary key (country_id, year) — one row per country per year.
-- gdp_per_capita is derived: gdp_usd / population.
-- -----------------------------------------------------------------------------
CREATE TABLE fact_country_stats (
    country_id     INTEGER
        REFERENCES dim_country(country_id),

    year           INTEGER
        REFERENCES dim_time(year),

    gdp_usd        DOUBLE,   -- Current USD (NY.GDP.MKTP.CD)
    population     DOUBLE,   -- Total population (SP.POP.TOTL)
    gdp_per_capita DOUBLE,   -- Derived: gdp_usd / population

    PRIMARY KEY (country_id, year)
);
