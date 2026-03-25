"""
utils/constants.py — Dashboard-wide constants, colour palettes, and formatters.
"""

# ---------------------------------------------------------------------------
# App metadata
# ---------------------------------------------------------------------------

APP_TITLE        = "GlobalTradeScope"
PLOTLY_TEMPLATE  = "plotly_white"

# ---------------------------------------------------------------------------
# Herfindahl-Hirschman Index (HHI) concentration thresholds
# ---------------------------------------------------------------------------

HHI_HIGH     = 2500   # Highly concentrated  (≥ 2500)
HHI_MODERATE = 1500   # Moderately concentrated (1500 – 2499)
                      # < 1500 → unconcentrated

# ---------------------------------------------------------------------------
# Semantic colour palette
# ---------------------------------------------------------------------------

COLORS: dict[str, str] = {
    # Trade flows
    "import":         "#2563EB",   # blue-600
    "export":         "#16A34A",   # green-600
    # Balance
    "deficit":        "#DC2626",   # red-600
    "surplus":        "#16A34A",   # green-600
    # Risk levels (e.g. concentration risk)
    "high_risk":      "#DC2626",   # red-600
    "moderate_risk":  "#F59E0B",   # amber-400
    "low_risk":       "#16A34A",   # green-600
    # Neutrals / misc
    "neutral":        "#6B7280",   # gray-500
    "highlight":      "#7C3AED",   # violet-600
    "background":     "#F9FAFB",   # gray-50
}

# ---------------------------------------------------------------------------
# HS section colour palette (21 sections, visually distinct)
# ---------------------------------------------------------------------------

SECTION_COLORS: dict[str, str] = {
    "Live Animals & Animal Products":           "#F97316",  # orange-500
    "Vegetable Products":                       "#84CC16",  # lime-500
    "Animal or Vegetable Fats & Oils":          "#EAB308",  # yellow-500
    "Prepared Foodstuffs, Beverages & Tobacco": "#FB923C",  # orange-400
    "Mineral Products":                         "#78716C",  # stone-500
    "Chemical & Allied Industries":             "#A855F7",  # purple-500
    "Plastics & Rubber":                        "#EC4899",  # pink-500
    "Hides, Skins, Leather & Furskins":         "#92400E",  # amber-800
    "Wood & Articles of Wood":                  "#A16207",  # yellow-700
    "Pulp, Paper & Paperboard":                 "#BAE6FD",  # sky-200
    "Textiles & Textile Articles":              "#E879F9",  # fuchsia-400
    "Footwear, Headgear & Umbrellas":           "#F43F5E",  # rose-500
    "Stone, Plaster, Cement & Glass":           "#94A3B8",  # slate-400
    "Pearls, Precious Metals & Stones":         "#FDE68A",  # amber-200
    "Base Metals & Articles":                   "#64748B",  # slate-500
    "Machinery & Electrical Equipment":         "#2563EB",  # blue-600
    "Vehicles, Aircraft & Vessels":             "#0EA5E9",  # sky-500
    "Optical, Photographic & Medical Instruments": "#6366F1",  # indigo-500
    "Arms & Ammunition":                        "#B91C1C",  # red-700
    "Miscellaneous Manufactured Articles":      "#14B8A6",  # teal-500
    "Works of Art & Collectors' Pieces":        "#D97706",  # amber-600
}

# ---------------------------------------------------------------------------
# Number formatters
# ---------------------------------------------------------------------------

def fmt_usd(value: float | int | None, decimals: int = 1) -> str:
    """
    Format a USD value with a B / M / K suffix.

    Examples
    --------
    >>> fmt_usd(4_320_000_000)
    '$4.3B'
    >>> fmt_usd(52_700_000)
    '$52.7M'
    >>> fmt_usd(8_400)
    '$8.4K'
    >>> fmt_usd(312)
    '$312'
    >>> fmt_usd(-1_200_000_000)
    '-$1.2B'
    >>> fmt_usd(None)
    'N/A'
    """
    if value is None:
        return "N/A"

    try:
        value = float(value)
    except (TypeError, ValueError):
        return "N/A"

    sign   = "-" if value < 0 else ""
    absval = abs(value)

    if absval >= 1e9:
        return f"{sign}${absval / 1e9:.{decimals}f}B"
    if absval >= 1e6:
        return f"{sign}${absval / 1e6:.{decimals}f}M"
    if absval >= 1e3:
        return f"{sign}${absval / 1e3:.{decimals}f}K"
    return f"{sign}${absval:,.0f}"


def fmt_pct(value: float | int | None, decimals: int = 1, scale: float = 1.0) -> str:
    """
    Format a percentage value.

    Parameters
    ----------
    value : float | int | None
        The value to format.
    decimals : int
        Number of decimal places (default 1).
    scale : float
        Multiplier applied before formatting.
        Use ``scale=100`` when *value* is a proportion in [0, 1].
        Default is 1.0 (value is already in percent-scale).

    Examples
    --------
    >>> fmt_pct(23.7)
    '23.7%'
    >>> fmt_pct(0.237, scale=100)
    '23.7%'
    >>> fmt_pct(None)
    'N/A'
    """
    if value is None:
        return "N/A"

    try:
        return f"{float(value) * scale:.{decimals}f}%"
    except (TypeError, ValueError):
        return "N/A"
