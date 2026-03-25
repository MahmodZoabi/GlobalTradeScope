"""
utils/nav.py — Horizontal top navigation bar for GlobalTradeScope.

Call render_nav(__file__) once per page, right after inject_css().
The current page link is rendered disabled (bold/accented via CSS).
"""

from pathlib import Path

import streamlit as st

_PAGES = [
    ("app.py",                        "Home"),
    ("pages/1_Overview.py",           "Overview"),
    ("pages/2_Dependency_Risk.py",    "Dependency Risk"),
    ("pages/3_Partner_Deep_Dive.py",  "Partner Deep Dive"),
    ("pages/4_Commodity_Explorer.py", "Commodity Explorer"),
    ("pages/5_Data_Quality.py",       "Data Quality"),
]


def render_nav(current_file: str = "") -> None:
    """Render a horizontal navigation bar across the top of the main area.

    Args:
        current_file: Pass ``__file__`` from the calling page so the active
                      link can be styled differently.
    """
    current_name = Path(current_file).name if current_file else ""
    cols = st.columns(len(_PAGES))
    for col, (path, label) in zip(cols, _PAGES):
        is_current = Path(path).name == current_name
        with col:
            st.page_link(
                path,
                label=label,
                disabled=is_current,
                use_container_width=True,
            )
    st.divider()
