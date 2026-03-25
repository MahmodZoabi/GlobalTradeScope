"""
utils/db.py — DuckDB connection helper for Streamlit.

Usage
-----
    from utils.db import query, query_uncached

    # Cached (1-hour TTL) — use for static/filter-independent queries
    df = query("SELECT * FROM dim_country")

    # Uncached — use when SQL is built from user-selected filters
    df = query_uncached(f"SELECT * FROM fact_trade WHERE year = {year}")
"""

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "globaltrade.duckdb"


# ---------------------------------------------------------------------------
# Cached connection (one shared read-only connection per Streamlit session)
# ---------------------------------------------------------------------------

@st.cache_resource
def _get_connection() -> duckdb.DuckDBPyConnection:
    """
    Open a read-only DuckDB connection and cache it for the lifetime of the
    Streamlit app process.  @st.cache_resource ensures only one connection is
    created across all user sessions.
    """
    if not _DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {_DB_PATH}. "
            "Run pipeline/03_load_db.py first."
        )
    return duckdb.connect(str(_DB_PATH), read_only=True)


# ---------------------------------------------------------------------------
# Public query helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def query(sql: str) -> pd.DataFrame:
    """
    Execute *sql* and return results as a DataFrame.

    Results are cached for 1 hour (ttl=3600).  Use this for queries whose
    output depends only on the SQL string — i.e. anything that does NOT
    incorporate runtime user-input values directly into the SQL text.

    Parameters
    ----------
    sql : str
        A read-only SELECT statement.

    Returns
    -------
    pd.DataFrame
        Query results; empty DataFrame on error.
    """
    try:
        con = _get_connection()
        return con.execute(sql).df()
    except Exception as exc:
        st.error(f"Query error: {exc}")
        return pd.DataFrame()


def query_uncached(sql: str) -> pd.DataFrame:
    """
    Execute *sql* and return results as a DataFrame **without caching**.

    Use this when the SQL string is assembled from dynamic user-selected
    filters (e.g. year sliders, multi-selects) to avoid stale cache hits.

    Parameters
    ----------
    sql : str
        A read-only SELECT statement.

    Returns
    -------
    pd.DataFrame
        Query results; empty DataFrame on error.
    """
    try:
        con = _get_connection()
        return con.execute(sql).df()
    except Exception as exc:
        st.error(f"Query error: {exc}")
        return pd.DataFrame()
