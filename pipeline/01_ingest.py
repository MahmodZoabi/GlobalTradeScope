"""
01_ingest.py — GlobalTradeScope data ingestion
Downloads 4 data sources into data/raw/:
  1. UN Comtrade Israel reporter flows (2014-2024)
  2. UN Comtrade mirror statistics from top-15 partners
  3. World Bank GDP & population indicators
  4. Reference tables: HS chapters, country mapping
"""

import os
import time
import logging
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

load_dotenv()

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

API_KEY = os.getenv("COMTRADE_API_KEY")
if API_KEY:
    log.info("COMTRADE_API_KEY found — using getFinalData (250K row limit)")
else:
    log.warning("COMTRADE_API_KEY not found — falling back to previewFinalData (500 row limit)")

RATE_LIMIT_SLEEP = 1.5  # seconds between Comtrade calls


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _save_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)
    log.info("  Saved %d rows → %s", len(df), path.name)


def _skip(path: Path) -> bool:
    if path.exists():
        log.info("  Skip (exists): %s", path.name)
        return True
    return False


# ---------------------------------------------------------------------------
# 1. UN Comtrade — Israel as reporter (imports + exports, 2014-2024)
# ---------------------------------------------------------------------------

ISRAEL_REPORTER = "376"
YEARS = list(range(2014, 2025))

COMTRADE_COMMON = dict(
    typeCode="C",
    freqCode="A",
    clCode="HS",
    reporterCode=ISRAEL_REPORTER,
    cmdCode="AG2",
    flowCode="M,X",
    partnerCode=None,   # None = all bilateral partners (0 = World-aggregate only)
    partner2Code="0",
    customsCode="C00",
    motCode="0",
    format_output="JSON",
    aggregateBy=None,
    breakdownMode="plus",
    countOnly=None,
    includeDesc=True,
)


def fetch_comtrade_israel() -> None:
    log.info("=== [1/4] Comtrade — Israel reporter flows ===")
    import comtradeapicall

    year_dfs = []
    for year in YEARS:
        out_path = RAW_DIR / f"comtrade_israel_reporter_{year}.csv"
        if _skip(out_path):
            year_dfs.append(pd.read_csv(out_path))
            continue

        log.info("  Fetching year %d …", year)
        try:
            if API_KEY:
                df = comtradeapicall.getFinalData(
                    subscription_key=API_KEY,
                    period=str(year),
                    maxRecords=250000,
                    **COMTRADE_COMMON,
                )
            else:
                df = comtradeapicall.previewFinalData(
                    period=str(year),
                    maxRecords=500,
                    **COMTRADE_COMMON,
                )
        except Exception as exc:
            log.error("  Error fetching year %d: %s", year, exc)
            time.sleep(RATE_LIMIT_SLEEP)
            continue

        if df is None or df.empty:
            log.warning("  No data returned for year %d", year)
        else:
            _save_csv(df, out_path)
            year_dfs.append(df)

        time.sleep(RATE_LIMIT_SLEEP)

    if year_dfs:
        combined_path = RAW_DIR / "comtrade_israel_reporter_all.csv"
        if not combined_path.exists():
            combined = pd.concat(year_dfs, ignore_index=True)
            _save_csv(combined, combined_path)
        else:
            log.info("  Skip (exists): comtrade_israel_reporter_all.csv")
    else:
        log.warning("  No yearly files to combine for Israel reporter.")


# ---------------------------------------------------------------------------
# 2. UN Comtrade — mirror statistics (top-15 partners report on Israel)
# ---------------------------------------------------------------------------

TOP15_PARTNERS = {
    156: "China",
    276: "Germany",
    840: "USA",
    792: "Turkey",
    380: "Italy",
    756: "Switzerland",
    826: "UK",
    528: "Netherlands",
    356: "India",
    410: "South Korea",
    392: "Japan",
    250: "France",
    56:  "Belgium",
    724: "Spain",
    643: "Russia",
}
MIRROR_YEARS = [2014, 2017, 2020, 2023]

MIRROR_COMMON = dict(
    typeCode="C",
    freqCode="A",
    clCode="HS",
    partnerCode=ISRAEL_REPORTER,
    cmdCode="AG2",
    flowCode="M,X",
    partner2Code="0",
    customsCode="C00",
    motCode="0",
    format_output="JSON",
    aggregateBy=None,
    breakdownMode="plus",
    countOnly=None,
    includeDesc=True,
)


def fetch_comtrade_mirror() -> None:
    if not API_KEY:
        log.info("=== [2/4] Comtrade mirror — skipped (no API key) ===")
        return

    log.info("=== [2/4] Comtrade — mirror statistics ===")
    import comtradeapicall

    all_dfs = []
    for partner_code, partner_name in TOP15_PARTNERS.items():
        for year in MIRROR_YEARS:
            out_path = RAW_DIR / f"comtrade_mirror_{partner_code}_{year}.csv"
            if _skip(out_path):
                all_dfs.append(pd.read_csv(out_path))
                continue

            log.info("  Fetching %s (%d) — %d …", partner_name, partner_code, year)
            try:
                df = comtradeapicall.getFinalData(
                    subscription_key=API_KEY,
                    period=str(year),
                    reporterCode=str(partner_code),
                    maxRecords=250000,
                    **MIRROR_COMMON,
                )
            except Exception as exc:
                log.error("  Error: %s/%d: %s", partner_name, year, exc)
                time.sleep(RATE_LIMIT_SLEEP)
                continue

            if df is None or df.empty:
                log.warning("  No data: %s/%d", partner_name, year)
            else:
                _save_csv(df, out_path)
                all_dfs.append(df)

            time.sleep(RATE_LIMIT_SLEEP)

    if all_dfs:
        combined_path = RAW_DIR / "comtrade_mirror_all.csv"
        if not combined_path.exists():
            combined = pd.concat(all_dfs, ignore_index=True)
            _save_csv(combined, combined_path)
        else:
            log.info("  Skip (exists): comtrade_mirror_all.csv")
    else:
        log.warning("  No mirror files to combine.")


# ---------------------------------------------------------------------------
# 3. World Bank — GDP and population indicators
# ---------------------------------------------------------------------------

WB_BASE = "https://api.worldbank.org/v2"
WB_INDICATORS = {
    "NY.GDP.MKTP.CD": "worldbank_gdp_current_usd.csv",
    "SP.POP.TOTL":    "worldbank_population.csv",
}


def _fetch_wb_indicator(indicator: str) -> pd.DataFrame:
    url = f"{WB_BASE}/country/all/indicator/{indicator}"
    params = {"format": "json", "date": "2014:2024", "per_page": 1000, "page": 1}
    rows = []
    while True:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2:
            break
        meta, data = payload[0], payload[1]
        if data:
            rows.extend(data)
        total_pages = meta.get("pages", 1)
        log.info("    Page %d/%d (%d records so far)", params["page"], total_pages, len(rows))
        if params["page"] >= total_pages:
            break
        params["page"] += 1
        time.sleep(0.3)

    records = []
    for item in rows:
        records.append({
            "country_iso3": item.get("countryiso3code"),  # iso3; "country.id" is the iso2/aggregate code
            "country_name": item.get("country", {}).get("value"),
            "indicator":    indicator,
            "year":         item.get("date"),
            "value":        item.get("value"),
        })
    return pd.DataFrame(records)


def fetch_worldbank() -> None:
    log.info("=== [3/4] World Bank indicators ===")
    for indicator, filename in WB_INDICATORS.items():
        out_path = RAW_DIR / filename
        if _skip(out_path):
            continue
        log.info("  Fetching %s …", indicator)
        try:
            df = _fetch_wb_indicator(indicator)
            _save_csv(df, out_path)
        except Exception as exc:
            log.error("  Error fetching %s: %s", indicator, exc)


# ---------------------------------------------------------------------------
# 4. Reference tables — HS chapters & country mapping
# ---------------------------------------------------------------------------

# HS 2-digit chapter → section mapping
HS_SECTION_MAP = {
    # Section I — Live Animals & Animal Products
    **{ch: ("I", "Live Animals & Animal Products") for ch in range(1, 6)},
    # Section II — Vegetable Products
    **{ch: ("II", "Vegetable Products") for ch in range(6, 15)},
    # Section III — Animal or Vegetable Fats & Oils
    15: ("III", "Animal or Vegetable Fats & Oils"),
    # Section IV — Prepared Foodstuffs; Beverages, Spirits & Vinegar; Tobacco
    **{ch: ("IV", "Prepared Foodstuffs, Beverages & Tobacco") for ch in range(16, 25)},
    # Section V — Mineral Products
    **{ch: ("V", "Mineral Products") for ch in range(25, 28)},
    # Section VI — Chemical & Allied Industries
    **{ch: ("VI", "Chemical & Allied Industries") for ch in range(28, 39)},
    # Section VII — Plastics & Rubber
    **{ch: ("VII", "Plastics & Rubber") for ch in range(39, 41)},
    # Section VIII — Raw Hides, Skins, Leather & Furskins
    **{ch: ("VIII", "Hides, Skins, Leather & Furskins") for ch in range(41, 44)},
    # Section IX — Wood & Articles of Wood
    **{ch: ("IX", "Wood & Articles of Wood") for ch in range(44, 47)},
    # Section X — Pulp of Wood, Paper & Paperboard
    **{ch: ("X", "Pulp, Paper & Paperboard") for ch in range(47, 50)},
    # Section XI — Textiles & Textile Articles
    **{ch: ("XI", "Textiles & Textile Articles") for ch in range(50, 64)},
    # Section XII — Footwear, Headgear, Umbrellas
    **{ch: ("XII", "Footwear, Headgear & Umbrellas") for ch in range(64, 68)},
    # Section XIII — Articles of Stone, Plaster, Cement, Glass
    **{ch: ("XIII", "Stone, Plaster, Cement & Glass") for ch in range(68, 71)},
    # Section XIV — Natural or Cultured Pearls, Precious Metals
    71: ("XIV", "Pearls, Precious Metals & Stones"),
    # Section XV — Base Metals & Articles
    **{ch: ("XV", "Base Metals & Articles") for ch in range(72, 84)},
    # Section XVI — Machinery & Electrical Equipment
    **{ch: ("XVI", "Machinery & Electrical Equipment") for ch in range(84, 86)},
    # Section XVII — Vehicles, Aircraft, Vessels
    **{ch: ("XVII", "Vehicles, Aircraft & Vessels") for ch in range(86, 90)},
    # Section XVIII — Optical, Photographic, Medical Instruments
    **{ch: ("XVIII", "Optical, Photographic & Medical Instruments") for ch in range(90, 93)},
    # Section XIX — Arms & Ammunition
    93: ("XIX", "Arms & Ammunition"),
    # Section XX — Miscellaneous Manufactured Articles
    **{ch: ("XX", "Miscellaneous Manufactured Articles") for ch in range(94, 97)},
    # Section XXI — Works of Art, Collectors' Pieces
    97: ("XXI", "Works of Art & Collectors' Pieces"),
    # Special / national use
    98: ("SPECIAL", "Special Classifications"),
    99: ("SPECIAL", "Special Classifications"),
}

HS_CHAPTER_NAMES = {
    1: "Live animals",
    2: "Meat & edible offal",
    3: "Fish & crustaceans",
    4: "Dairy, eggs, honey",
    5: "Other animal products",
    6: "Live trees & plants",
    7: "Edible vegetables",
    8: "Edible fruit & nuts",
    9: "Coffee, tea, spices",
    10: "Cereals",
    11: "Milling industry products",
    12: "Oil seeds & misc grains",
    13: "Lac, gums & resins",
    14: "Vegetable plaiting materials",
    15: "Animal or vegetable fats & oils",
    16: "Preparations of meat or fish",
    17: "Sugars & confectionery",
    18: "Cocoa & cocoa preparations",
    19: "Cereals, flour, starch preparations",
    20: "Preparations of vegetables & fruit",
    21: "Miscellaneous edible preparations",
    22: "Beverages, spirits & vinegar",
    23: "Food industry residues & waste",
    24: "Tobacco & manufactured tobacco",
    25: "Salt, sulphur, earths & stone",
    26: "Ores, slag & ash",
    27: "Mineral fuels & oils",
    28: "Inorganic chemicals",
    29: "Organic chemicals",
    30: "Pharmaceutical products",
    31: "Fertilisers",
    32: "Tanning & dyeing extracts",
    33: "Essential oils & cosmetics",
    34: "Soap, lubricants, waxes",
    35: "Albuminoidal substances, starches",
    36: "Explosives & pyrotechnics",
    37: "Photographic goods",
    38: "Miscellaneous chemical products",
    39: "Plastics & articles thereof",
    40: "Rubber & articles thereof",
    41: "Raw hides & skins",
    42: "Leather articles & saddlery",
    43: "Furskins & artificial fur",
    44: "Wood & articles of wood",
    45: "Cork & articles of cork",
    46: "Manufactures of straw & plaiting",
    47: "Pulp of wood",
    48: "Paper & paperboard",
    49: "Printed books & newspapers",
    50: "Silk",
    51: "Wool & fine animal hair",
    52: "Cotton",
    53: "Other vegetable textile fibres",
    54: "Man-made filaments",
    55: "Man-made staple fibres",
    56: "Wadding, felt & nonwovens",
    57: "Carpets & floor coverings",
    58: "Special woven fabrics",
    59: "Impregnated textile fabrics",
    60: "Knitted or crocheted fabrics",
    61: "Knitted or crocheted apparel",
    62: "Woven apparel",
    63: "Other made-up textile articles",
    64: "Footwear",
    65: "Headgear",
    66: "Umbrellas & walking sticks",
    67: "Feathers, artificial flowers",
    68: "Articles of stone, plaster, cement",
    69: "Ceramic products",
    70: "Glass & glassware",
    71: "Pearls, precious metals, jewellery",
    72: "Iron & steel",
    73: "Articles of iron or steel",
    74: "Copper & articles thereof",
    75: "Nickel & articles thereof",
    76: "Aluminium & articles thereof",
    77: "Reserved",
    78: "Lead & articles thereof",
    79: "Zinc & articles thereof",
    80: "Tin & articles thereof",
    81: "Other base metals",
    82: "Tools, implements of base metal",
    83: "Miscellaneous articles of base metal",
    84: "Nuclear reactors, boilers, machinery",
    85: "Electrical machinery & equipment",
    86: "Railway locomotives & rolling stock",
    87: "Vehicles (not railway)",
    88: "Aircraft & spacecraft",
    89: "Ships & floating structures",
    90: "Optical & medical instruments",
    91: "Clocks & watches",
    92: "Musical instruments",
    93: "Arms & ammunition",
    94: "Furniture, bedding, mattresses",
    95: "Toys, games & sports equipment",
    96: "Miscellaneous manufactured articles",
    97: "Works of art & collectors' pieces",
    98: "Special classifications (national)",
    99: "Special classifications (national)",
}


def build_hs_chapters() -> None:
    log.info("=== [4a/4] Reference: HS chapters ===")
    out_path = RAW_DIR / "hs_chapters.csv"
    if _skip(out_path):
        return

    rows = []
    for chapter in range(1, 100):
        section_num, section_name = HS_SECTION_MAP.get(chapter, ("?", "Unknown"))
        rows.append({
            "chapter_code": f"{chapter:02d}",
            "chapter_int":  chapter,
            "chapter_name": HS_CHAPTER_NAMES.get(chapter, ""),
            "section_num":  section_num,
            "section_name": section_name,
        })

    df = pd.DataFrame(rows)
    _save_csv(df, out_path)


# Manual ISO3 → UN M49 numeric overrides (Comtrade reporter/partner codes)
ISO3_TO_UN_NUMERIC = {
    "USA": 842,   # United States (Comtrade uses 842)
    "CHN": 156,
    "DEU": 276,
    "ISR": 376,
    "TUR": 792,
    "ITA": 380,
    "CHE": 756,
    "GBR": 826,
    "NLD": 528,
    "IND": 356,
    "KOR": 410,
    "JPN": 392,
    "FRA": 250,
    "BEL":  56,
    "ESP": 724,
    "RUS": 643,
    "BRA": 76,
    "CAN": 124,
    "AUS": 36,
    "MEX": 484,
    "SAU": 682,
    "ZAF": 710,
    "NGA": 566,
    "EGY": 818,
    "ARG": 32,
    "IDN": 360,
    "POL": 616,
    "SWE": 752,
    "NOR": 578,
    "DNK": 208,
    "FIN": 246,
    "AUT": 40,
    "PRT": 620,
    "GRC": 300,
    "CZE": 203,
    "HUN": 348,
    "ROU": 642,
    "UKR": 804,
    "THA": 764,
    "VNM": 704,
    "MYS": 458,
    "SGP": 702,
    "PHL": 608,
    "PAK": 586,
    "BGD": 50,
    "IRN": 364,
    "IRQ": 368,
    "JOR": 400,
    "LBN": 422,
    "SYR": 760,
    "YEM": 887,
    "MAR": 504,
    "DZA": 12,
    "TUN": 788,
    "LBY": 434,
    "ETH": 231,
    "KEN": 404,
    "TZA": 834,
    "GHA": 288,
    "CIV": 384,
    "CMR": 120,
    "UGA": 800,
    "AGO": 24,
    "MOZ": 508,
    "ZMB": 894,
    "ZWE": 716,
    "COL": 170,
    "VEN": 862,
    "CHL": 152,
    "PER": 604,
    "ECU": 218,
    "BOL": 68,
    "PRY": 600,
    "URY": 858,
    "HKG": 344,
    "TWN": 490,
    "NZL": 554,
}


def build_country_mapping() -> None:
    log.info("=== [4b/4] Reference: country mapping ===")
    out_path = RAW_DIR / "country_mapping.csv"
    if _skip(out_path):
        return

    log.info("  Fetching World Bank country metadata …")
    url = "https://api.worldbank.org/v2/country"
    params = {"format": "json", "per_page": 400, "page": 1}
    all_countries = []
    while True:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2:
            break
        meta, data = payload[0], payload[1]
        if data:
            all_countries.extend(data)
        if params["page"] >= meta.get("pages", 1):
            break
        params["page"] += 1
        time.sleep(0.3)

    rows = []
    for item in all_countries:
        iso3 = item.get("id", "")
        rows.append({
            "iso3":         iso3,
            "name":         item.get("name"),
            "region":       item.get("region", {}).get("value"),
            "income_group": item.get("incomeLevel", {}).get("value"),
            "iso2":         item.get("iso2Code"),
            "capital":      item.get("capitalCity"),
            "longitude":    item.get("longitude"),
            "latitude":     item.get("latitude"),
            "un_numeric":   ISO3_TO_UN_NUMERIC.get(iso3),
        })

    df = pd.DataFrame(rows)
    _save_csv(df, out_path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("GlobalTradeScope — data ingestion starting")
    log.info("Output directory: %s", RAW_DIR)

    fetch_comtrade_israel()
    fetch_comtrade_mirror()
    fetch_worldbank()
    build_hs_chapters()
    build_country_mapping()

    log.info("Data ingestion complete.")


if __name__ == "__main__":
    main()
