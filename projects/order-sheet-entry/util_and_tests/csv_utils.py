"""
csv_utils.py

Generic CSV reading and DataFrame preprocessing utilities.
"""

import re
from datetime import datetime, timedelta
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


# ───────────────────────────────────────────────────────────────────────────────
#  SHIP DATE CLEANING
# ───────────────────────────────────────────────────────────────────────────────

# Pattern to find dates: M/D, M/D/YY, M/D/YYYY, M-D, M-D-YY, M-D-YYYY
DATE_PATTERN = re.compile(r"(\d{1,2})[/\-](\d{1,2})(?:[/\-](\d{2,4}))?")


def clean_ship_date(raw_date: str) -> tuple[str, bool]:
    """
    Clean and normalize a ship date string.

    Args:
        raw_date: Raw date string from order sheet

    Returns:
        (formatted_date, was_defaulted) where:
        - formatted_date is MM/DD/YYYY format
        - was_defaulted is True if we defaulted to tomorrow
    """
    tomorrow = datetime.now() + timedelta(days=1)
    tomorrow_str = tomorrow.strftime("%m/%d/%Y")

    raw = raw_date.strip()

    # Empty or ASAP → tomorrow
    if not raw or raw.upper() == "ASAP":
        return tomorrow_str, True

    # Try to extract a date pattern
    match = DATE_PATTERN.search(raw)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        year_str = match.group(3)

        if year_str:
            year = int(year_str)
            # Handle 2-digit years
            if year < 100:
                year += 2000 if year < 50 else 1900
        else:
            # No year provided - assume current or next year
            current_year = datetime.now().year
            try:
                test_date = datetime(current_year, month, day)
                # If date is in the past, use next year
                if test_date < datetime.now():
                    year = current_year + 1
                else:
                    year = current_year
            except ValueError:
                return tomorrow_str, True

        try:
            parsed = datetime(year, month, day)
            return parsed.strftime("%m/%d/%Y"), False
        except ValueError:
            # Invalid date
            return tomorrow_str, True

    # No date pattern found → tomorrow
    return tomorrow_str, True
