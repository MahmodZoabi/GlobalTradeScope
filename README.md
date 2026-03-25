# GlobalTradeScope — Israel's Import/Export Dependency Analyzer

A data engineering and analytics project that transforms a decade of UN Comtrade bilateral trade data into an interactive dashboard for identifying supply-chain dependencies, partner concentration risk, and commodity-level trade patterns.

---

## What it does

### Overview
- Aggregates Israel's annual imports and exports from 2014 to 2024 into a single interactive view
- Displays total trade value, active partner count, and trade balance with year-over-year deltas
- Renders a filled area chart of imports vs exports over time, clearly showing deficit periods
- Shows the top 20 trading partners in a treemap coloured by World Bank region
- Breaks down trade by all 21 HS commodity sections in a grouped horizontal bar chart

### Dependency Risk Analysis
- Calculates the Herfindahl-Hirschman Index (HHI) for both partner and commodity concentration
- Flags single-country dependencies where one partner supplies more than 30% of a commodity's import value
- Visualises risk as a heatmap across all commodity × partner combinations over time
- Tracks whether Israel's import sourcing is diversifying or concentrating year-on-year
- Compares concentration levels against standard HHI thresholds (High ≥ 2500, Moderate ≥ 1500)

### Partner Deep Dive
- Allows selection of any trading partner with region and income-group filters
- Shows bilateral trade balance — imports, exports, and net balance — over the full decade
- Breaks down what Israel buys from and sells to the selected partner by HS chapter
- Cross-references Israel's self-reported figures against the partner's mirror statistics
- Adds macro context (GDP, GDP per capita, population) from World Bank indicators

### Commodity Explorer
- Lets users drill into any of the 99 HS 2-digit commodity chapters
- Maps import sources — which countries supply the chapter, with share trends over time
- Maps export destinations — where Israel sells each commodity
- Computes a unit-value price proxy (trade value per kg) as a crude price index
- Highlights sourcing shifts following supply-chain disruptions

### Data Quality & Methodology
- Displays pipeline run metadata: last ingest date and row counts per data source
- Reports null rates per column across all fact tables
- Shows foreign-key resolution rates (% of trade rows matched to a known country / commodity)
- Identifies mirror-data gaps — years and partners with no coverage
- Documents CIF vs FOB valuation conventions, HS revision handling, and API limits

---

## Tech stack

| Layer | Technology |
|---|---|
| Data ingestion | Python · `comtradeapicall` · `requests` |
| Data cleaning | `pandas` · `pyarrow` |
| Database | DuckDB (star schema) |
| Dashboard | Streamlit · Plotly |
| Fuzzy matching | `fuzzywuzzy` · `python-Levenshtein` |
| Environment | `python-dotenv` |
| Excel export | `openpyxl` |

---

## Key SQL / analytics skills demonstrated

| Skill | Where used |
|---|---|
| **Star schema design** | `dim_country`, `dim_commodity`, `dim_time` → `fact_trade` |
| **Window functions** | `ROW_NUMBER() OVER (ORDER BY …)` for surrogate key generation |
| **CTEs** | Multi-step aggregations in dependency risk queries |
| **Self-joins** | `fact_trade` joined twice to `dim_country` for reporter + partner |
| **HHI calculation** | `SUM(share²) × 10 000` aggregated per commodity per year |
| **Compound primary keys** | `fact_country_stats (country_id, year)` |
| **Two-stage FK resolution** | ISO3 lookup → UN numeric fallback in a single INSERT SELECT |
| **Pivot / unpivot** | Long → wide transformation for World Bank indicators |

---

## Data sources

| Source | What | Coverage |
|---|---|---|
| [UN Comtrade](https://comtrade.un.org/) | Bilateral trade flows (HS 2-digit) | 2014 – 2024, annual |
| [UN Comtrade mirror](https://comtrade.un.org/) | Top-15 partners reporting on Israel | 2014, 2017, 2020, 2023 |
| [World Bank API](https://datahelpdesk.worldbank.org/knowledgebase/articles/898581) | GDP (current USD), population | 2014 – 2024, all countries |
| UN Harmonized System | HS chapter → section mapping | 99 chapters, 21 sections |

---

## Quick start (Windows)

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

## Project structure

```
GlobalTradeScope/
├── app.py                        # Streamlit home page
├── pages/
│   ├── 1_Overview.py             # Trade balance, partners, commodity mix
│   ├── 2_Dependency_Risk.py      # HHI concentration analysis
│   ├── 3_Partner_Deep_Dive.py    # Bilateral deep-dive + mirror comparison
│   ├── 4_Commodity_Explorer.py   # HS chapter drill-down
│   └── 5_Data_Quality.py         # Pipeline metadata & methodology
├── pipeline/
│   ├── 01_ingest.py              # Download raw data → data/raw/
│   ├── 02_clean.py               # Clean & transform → data/processed/
│   └── 03_load_db.py             # Load DuckDB star schema
├── utils/
│   ├── db.py                     # Cached DuckDB connection helpers
│   └── constants.py              # Colours, formatters, thresholds
├── sql/
│   └── schema.sql                # DDL reference (matches 03_load_db.py)
├── data/
│   ├── raw/                      # CSVs from ingest (gitignored)
│   └── processed/                # Parquet files from clean (gitignored)
├── .streamlit/
│   └── config.toml               # Theme and server settings
├── requirements.txt
└── env.example
```

---

## Author

**Mahmod Zoubi** — Industrial Engineering, Tel Aviv University
[GitHub](https://github.com/MahmodZoabi) · [LinkedIn](https://www.linkedin.com/in/mahmod-zoabi/)
