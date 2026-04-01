"""
contracts/_profiler.py
Profiling module for Phase 1 data contract generation.

Attempts to use ydata-profiling if available; falls back to a native
pandas implementation that produces an identical output shape.
"""

from __future__ import annotations

import re
import pandas as pd

# Try ydata-profiling (requires Python <=3.12)
try:
    from ydata_profiling import ProfileReport as _YDataProfileReport  # type: ignore
    _YDATA_AVAILABLE = True
except ImportError:
    _YDATA_AVAILABLE = False

# ── Pattern constants ──────────────────────────────────────────────────────────
_UUID_RE    = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_HEX64_RE   = re.compile(r"^[0-9a-f]{64}$")
_PASCAL_RE  = re.compile(r"^[A-Z][a-zA-Z0-9]+$")
_ISO_DT_RE  = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}")

_DTYPE_MAP = {
    "O": "string",
    "U": "string",
    "S": "string",
    "f": "number",
    "i": "integer",
    "u": "integer",
    "b": "boolean",
    "M": "datetime",
}


def profile_dataframe(df: pd.DataFrame, minimal: bool = True) -> dict[str, dict]:
    """
    Profile a DataFrame and return a column-keyed dict of stats.

    Each column entry contains:
        dtype             : "string" | "number" | "integer" | "boolean" | "datetime"
        null_fraction     : float  (0.0–1.0)
        n_unique          : int
        is_enum_candidate : bool   (n_unique <= 20 and ratio < 0.5)
        enum_values       : list | None
        min / max / mean / std / p25 / p75 : float | None (numeric only)
        sample_values     : list   (first 5 non-null values)
        all_uuid          : bool
        all_hex64         : bool
        all_pascalcase    : bool
        all_datetime      : bool
        zero_variance     : bool
    """
    if _YDATA_AVAILABLE:
        return _ydata_profile(df, minimal)
    return _native_profile(df)


# ── ydata path ─────────────────────────────────────────────────────────────────
def _ydata_profile(df: pd.DataFrame, minimal: bool) -> dict[str, dict]:
    profile = _YDataProfileReport(df, minimal=minimal, progress_bar=False)
    desc = profile.get_description()
    result: dict[str, dict] = {}
    for col, stats in desc.variables.items():
        result[col] = _normalise_ydata_col(col, stats, df[col])
    return result


def _normalise_ydata_col(col: str, stats: object, series: pd.Series) -> dict:
    """Map ydata column stats object → our standard dict."""
    non_null = series.dropna()
    kind = series.dtype.kind
    dtype_label = _DTYPE_MAP.get(kind, str(series.dtype))

    n = len(non_null)
    null_fraction = float(getattr(stats, "p_missing", series.isna().mean()))
    n_unique      = int(getattr(stats, "n_unique", series.nunique()))

    entry = _base_entry(dtype_label, null_fraction, n_unique, series, non_null)

    # Override numeric stats from ydata if available
    for attr, key in [("min", "min"), ("max", "max"), ("mean", "mean"),
                      ("std", "std"), ("p25", "p25"), ("p75", "p75")]:
        val = getattr(stats, attr, None)
        if val is not None:
            entry[key] = float(val)

    return entry


# ── native path ────────────────────────────────────────────────────────────────
def _native_profile(df: pd.DataFrame) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for col in df.columns:
        series   = df[col]
        non_null = series.dropna()
        kind     = series.dtype.kind
        dtype_label = _DTYPE_MAP.get(kind, "string")

        entry = _base_entry(dtype_label, series.isna().mean(), series.nunique(), series, non_null)

        # Numeric stats
        if kind in ("f", "i", "u") and len(non_null) > 0:
            desc = non_null.astype(float).describe(percentiles=[0.25, 0.75, 0.95, 0.99])
            entry.update({
                "min":  float(desc["min"]),
                "max":  float(desc["max"]),
                "mean": float(desc["mean"]),
                "std":  float(non_null.astype(float).std()),
                "p25":  float(desc["25%"]),
                "p75":  float(desc["75%"]),
                "zero_variance": float(non_null.astype(float).std()) == 0.0,
            })

        result[col] = entry
    return result


def _base_entry(
    dtype_label: str,
    null_fraction: float,
    n_unique: int,
    series: pd.Series,
    non_null: pd.Series,
) -> dict:
    """Build the common part of a column profile entry."""
    n = len(non_null)

    # Enum detection: low cardinality + not all unique + stable-looking values
    is_enum = (
        n > 0
        and n_unique <= 20
        and (n_unique / n) < 0.5
        and dtype_label == "string"
    )

    entry: dict = {
        "dtype":             dtype_label,
        "null_fraction":     float(null_fraction),
        "n_unique":          int(n_unique),
        "is_enum_candidate": is_enum,
        "enum_values":       sorted(non_null.astype(str).unique().tolist()) if is_enum else None,
        "min":               None,
        "max":               None,
        "mean":              None,
        "std":               None,
        "p25":               None,
        "p75":               None,
        "sample_values":     non_null.head(5).tolist() if n > 0 else [],
        "all_uuid":          False,
        "all_hex64":         False,
        "all_pascalcase":    False,
        "all_datetime":      False,
        "zero_variance":     False,
    }

    # Pattern sniffing (string columns only)
    if dtype_label == "string" and n > 0:
        sample = non_null.astype(str).head(200)
        entry["all_uuid"]       = bool(sample.str.match(_UUID_RE).all())
        entry["all_hex64"]      = bool(sample.str.match(_HEX64_RE).all())
        entry["all_pascalcase"] = bool(sample.str.match(_PASCAL_RE).all())
        entry["all_datetime"]   = bool(sample.str.match(_ISO_DT_RE).all())

    return entry
