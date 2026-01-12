"""
csv_utils.py

Generic CSV reading and DataFrame preprocessing utilities.
"""

from pathlib import Path
import pandas as pd


# ───────────────────────────────────────────────────────────────────────────────
#  ENCODING HANDLING
# ───────────────────────────────────────────────────────────────────────────────

ENCODING_CANDIDATES = (
    "utf-8",
    "utf-8-sig",
    "cp1252",
    "latin1",
)


def read_csv_smart(path: Path, **pd_kwargs) -> pd.DataFrame:
    """
    Read a CSV file, trying multiple encodings until one succeeds.

    Args:
        path: Path to CSV file
        **pd_kwargs: Additional arguments passed to pd.read_csv

    Returns:
        DataFrame with CSV contents

    Raises:
        UnicodeDecodeError: If none of the candidate encodings work
    """
    last_err = None
    for enc in ENCODING_CANDIDATES:
        try:
            return pd.read_csv(path, encoding=enc, **pd_kwargs)
        except UnicodeDecodeError as e:
            last_err = e
    raise UnicodeDecodeError(
        f"Could not decode {path} with tried encodings "
        f"{', '.join(ENCODING_CANDIDATES)}"
    ) from last_err


# ───────────────────────────────────────────────────────────────────────────────
#  DATAFRAME PREPROCESSING
# ───────────────────────────────────────────────────────────────────────────────

def _nonempty(s: pd.Series) -> pd.Series:
    """Strip whitespace and convert to string, filling NaN with empty string."""
    return s.fillna("").astype(str).str.strip()


def preprocess_df(
    df: pd.DataFrame,
    parent_col: str = "Parent SKU",
    size_abbr_col: str = "Size Abbreviation",
) -> pd.DataFrame:
    """
    Normalize a DataFrame for consistent processing.

    - Strips whitespace from column headers
    - Strips whitespace from all string cells
    - Uppercases SKU-related columns

    Args:
        df: Input DataFrame
        parent_col: Name of parent SKU column
        size_abbr_col: Name of size abbreviation column

    Returns:
        Preprocessed DataFrame copy
    """
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    for sku_col in (parent_col, "SKU", "SKU (Parent)"):
        if sku_col in df.columns:
            df[sku_col] = df[sku_col].str.upper()

    if size_abbr_col in df.columns:
        df[size_abbr_col] = df[size_abbr_col].str.strip().str.upper()

    return df
