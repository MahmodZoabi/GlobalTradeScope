# GlobalTradeScope — Israel's Import/Export Dependency Analyzer

[![Live Demo](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://globaltradescope.streamlit.app/)

A data engineering and analytics project that transforms a decade of UN Comtrade bilateral trade data (2014–2024) into an interactive dashboard for identifying supply-chain dependencies, partner concentration risk, and commodity-level trade patterns. Built as a portfolio project demonstrating end-to-end data pipeline design, star-schema warehousing, and analytical SQL.

---

## Key Findings

- **Persistent trade deficit, widening post-2020.** Israel's import bill exceeded exports in every year of the decade. The gap widened sharply after 2020, peaking at $15.7B in 2022 as post-COVID energy and goods prices surged.
- **Netherlands controls 84% of live plant imports.** A single country dominates HS Chapter 06 (live trees and plants), flagging a critical single-source dependency in the horticultural supply chain.
- **UAE trade surged after the Abraham Accords (2020).** The UAE entered Israel's top-20 import partners in 2021 and has expanded year-on-year, providing a visible, data-backed case study of the Accords' economic impact.
- **China drives the largest mirror-data discrepancies.** Israel's self-reported China import figures diverge 62–96% from China's partner-reported figures across maritime and food sectors — consistent with Hong Kong re-export re-attribution.
- **U.S. absorbs over 30% of all Israeli exports.** The United States is Israel's dominant export destination, with $186.6B across the decade — nearly 5x the second-largest destination (China at $41.4B).

---

## Dashboard Pages

| Page | What it shows |
|---|---|
| **Home** | Dataset summary — record counts, year range, partner count, total trade value |
| **Trade Overview** | Imports vs exports over time, top-20 partners treemap, commodity section breakdown |
| **Dependency Risk** | HHI concentration by commodity section, single-source dependency alerts (>30% share), risk heatmap |
| **Partner Deep Dive** | Bilateral trade balance for any partner, HS chapter breakdown, mirror-data reconciliation, World Bank macro context |
| **Commodity Explorer** | Source/destination maps, NTILE growth analysis for emerging suppliers, diversification trend (supplier count + HHI) |
| **Data Quality** | Pipeline provenance, null rates, FK resolution rates, mirror-data gap analysis, methodology notes |

---

## Technical Highlights

- **Star schema in DuckDB** — `fact_trade`, `fact_mirror_trade`, `fact_country_stats` joined to `dim_country`, `dim_commodity`, `dim_time`; surrogate keys generated with `ROW_NUMBER() OVER (...)`
- **4-CTE HHI pipeline** — multi-step SQL computes partner and commodity Herfindahl-Hirschman Index per year; `ARG_MAX` window function identifies the dominant supplier in a single pass
- **Two-stage FK resolution** — ISO3 lookup with UN numeric fallback in a single `INSERT … SELECT` handles non-standard country codes without post-load patching
- **FULL OUTER JOIN mirror reconciliation** — Israel-reported and partner-reported flows joined across two fact tables to surface systematic reporting gaps
- **NTILE(4) growth quartile ranking** — suppliers ranked by average annual growth rate into quartiles; new entrants (zero early-period presence) flagged and sorted separately
- **LAG for year-over-year deltas** — window function computes YoY trade value change per partner directly in SQL, passed to the Streamlit metric delta parameter
- **116,390 trade records** across 198 partner countries, 96 HS chapters, 11 years

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data ingestion | Python · `comtradeapicall` · `requests` |
| Data cleaning | `pandas` · `pyarrow` |
| Database | DuckDB (star schema) |
| Dashboard | Streamlit · Plotly |
| Fuzzy matching | `fuzzywuzzy` · `python-Levenshtein` |
| Environment | `python-dotenv` |
| Data sources | UN Comtrade API · World Bank API |

---

## Quick Start (Windows)

**1. Clone the repo**
```bat
git clone https://github.com/MahmodZoabi/GlobalTradeScope.git
cd GlobalTradeScope
```

**2. Create and activate a virtual environment**
```bat
python -m venv venv
venv\Scripts\activate
```

**3. Install dependencies**
```bat
pip install -r requirements.txt
```

**4. Add your UN Comtrade API key**
```bat
copy env.example .env
```
Open `.env` and paste your key:
```
COMTRADE_API_KEY=your_key_here
```
> Free key at [comtradedeveloper.un.org](https://comtradedeveloper.un.org) — subscribe to **comtrade - v1**, then copy the Primary Key from your profile.
> The pipeline works without a key (500-row preview mode) but data will be limited.

**5. Run the pipeline**
```bat
python pipeline/01_ingest.py
python pipeline/02_clean.py
python pipeline/03_load_db.py
```

**6. Launch the dashboard**
```bat
streamlit run app.py
```

---

## Project Structure

```
GlobalTradeScope/
├── app.py                        # Streamlit home page and dataset summary
├── pages/
│   ├── 1_Overview.py             # Trade balance, top partners, commodity mix
│   ├── 2_Dependency_Risk.py      # HHI concentration analysis and risk heatmap
│   ├── 3_Partner_Deep_Dive.py    # Bilateral deep-dive with mirror reconciliation
│   ├── 4_Commodity_Explorer.py   # HS chapter drill-down and supplier growth analysis
│   └── 5_Data_Quality.py         # Pipeline metadata, null rates, methodology notes
├── pipeline/
│   ├── 01_ingest.py              # Download raw data from Comtrade and World Bank APIs
│   ├── 02_clean.py               # Validate, normalise, and write to Parquet
│   └── 03_load_db.py             # Build DuckDB star schema from processed files
├── utils/
│   ├── db.py                     # Cached DuckDB connection helpers
│   ├── constants.py              # Colour palettes, formatters, HHI thresholds
│   ├── nav.py                    # Horizontal top navigation bar (mobile-friendly)
│   └── styles.py                 # CSS injection for premium dashboard styling
├── sql/
│   └── schema.sql                # DDL reference matching 03_load_db.py
├── data/
│   ├── raw/                      # CSVs downloaded by ingest step (gitignored)
│   └── processed/                # Parquet files produced by clean step (gitignored)
├── .streamlit/
│   └── config.toml               # Theme colours and server settings
├── requirements.txt
└── env.example
```

---

## Data Sources

| Source | Content | Coverage |
|---|---|---|
| [UN Comtrade](https://comtrade.un.org/) | Bilateral trade flows at HS 2-digit level | 2014–2024, annual |
| [UN Comtrade mirror](https://comtrade.un.org/) | Top-15 partners reporting on Israel | 2014, 2017, 2020, 2023 |
| [World Bank API](https://datahelpdesk.worldbank.org/knowledgebase/articles/898581) | GDP (current USD), GDP per capita, population | 2014–2024, all countries |
| UN Harmonized System | HS chapter to section mapping | 99 chapters, 21 sections |

---

## Author

**Mahmod Zoubi** — Industrial Engineering, Tel Aviv University

[GitHub](https://github.com/MahmodZoabi) · [LinkedIn](https://www.linkedin.com/in/mahmod-zoabi/)
