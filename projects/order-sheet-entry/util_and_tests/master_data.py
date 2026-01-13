"""
master_data.py

Master SKU data loading, joining, and filtering utilities.
"""

from pathlib import Path
import logging

import pandas as pd

from .csv_utils import read_csv_smart, preprocess_df, _nonempty


# ───────────────────────────────────────────────────────────────────────────────
#  CONSTANTS
# ───────────────────────────────────────────────────────────────────────────────

BAD_SENTINELS = {"", "N/A", "#N/A", "NA", "NONE"}


# ───────────────────────────────────────────────────────────────────────────────
#  BUILD MASTER WITH EXT_ID (runtime join)
# ───────────────────────────────────────────────────────────────────────────────

def build_master_with_ext_id(
    master_sku_path: Path,
    product_variant_path: Path,
) -> pd.DataFrame:
    """
    Join master-sku.csv with product-variant-export.csv to add EXT_ID column.

    - Matches on: master_sku.SKU = product_variant.Internal Reference
    - Adds: EXT_ID column from product_variant.ID
    - Logs match statistics for visibility
    - Handles duplicates in product-variant by keeping first match

    Args:
        master_sku_path: Path to master-sku.csv
        product_variant_path: Path to product-variant-export.csv

    Returns:
        Master DataFrame with EXT_ID column added
    """
    logging.info("Building master SKU list with External IDs...")

    # Load master SKU list
    master_df = read_csv_smart(master_sku_path, dtype=str, keep_default_na=False)
    master_df = preprocess_df(master_df)

    # Load product variant export
    variant_df = read_csv_smart(product_variant_path, dtype=str, keep_default_na=False)
    variant_df.columns = [c.strip() for c in variant_df.columns]

    # Normalize Internal Reference for matching (uppercase, strip)
    variant_df["Internal Reference"] = (
        variant_df["Internal Reference"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # Drop duplicates in variant file, keeping first (prefer non-empty ID)
    variant_df["__has_id"] = variant_df["ID"].fillna("").astype(str).str.strip().ne("")
    variant_df = variant_df.sort_values("__has_id", ascending=False, kind="mergesort")
    variant_df = variant_df.drop_duplicates(subset=["Internal Reference"], keep="first")
    variant_df = variant_df.drop(columns=["__has_id"])

    # Create lookup dict: Internal Reference -> ID
    ext_id_lookup = dict(zip(
        variant_df["Internal Reference"],
        variant_df["ID"].fillna("").astype(str).str.strip()
    ))

    # Match statistics
    master_skus = set(master_df["SKU"].dropna().unique())
    variant_refs = set(variant_df["Internal Reference"].dropna().unique())
    matched = master_skus & variant_refs
    unmatched = master_skus - variant_refs

    match_pct = (len(matched) / len(master_skus) * 100) if master_skus else 0
    logging.info(
        "EXT_ID join: %d/%d SKUs matched (%.1f%%), %d unmatched",
        len(matched), len(master_skus), match_pct, len(unmatched)
    )

    if unmatched and len(unmatched) <= 20:
        # Log individual unmatched SKUs if not too many
        for sku in sorted(unmatched):
            logging.debug("No EXT_ID for SKU: %s", sku)
    elif unmatched:
        logging.warning(
            "%d SKUs have no External ID (will be empty in output). "
            "Update product-variant-export.csv from Odoo to fix.",
            len(unmatched)
        )

    # Add EXT_ID column via lookup
    master_df["EXT_ID"] = master_df["SKU"].map(ext_id_lookup).fillna("")

    return master_df


# ───────────────────────────────────────────────────────────────────────────────
#  MASTER DATA FILTERING & DEDUPLICATION
# ───────────────────────────────────────────────────────────────────────────────

def align_master_with_dupecheck(
    master_df: pd.DataFrame,
    size_abbr_col: str = "Size Abbreviation",
) -> pd.DataFrame:
    """
    Normalize and filter master data, enforcing uniqueness.

    Filtering rules:
      - Drop rows where any status column == 'EXCLUSIVE'
        (Season, FAHO24 Status, SPSU25 Status, FAHO25 Status, SPSU26 Status)
      - Drop rows where Collection == 'HAREM PANTS'
      - Drop rows where SKU contains 'HIC'
      - Drop rows with bad/empty keys

    Deduplication:
      - Prefer rows having EXT_ID, then having UPC
      - Enforce uniqueness on (SKU (Parent), Size Abbreviation)

    Args:
        master_df: Master DataFrame to process
        size_abbr_col: Name of size abbreviation column

    Returns:
        Filtered and deduplicated DataFrame

    Raises:
        RuntimeError: If duplicates remain after processing
    """
    m = master_df.copy()

    # Columns to check for EXCLUSIVE status
    exclusive_filter_cols = (
        "Season",
        "FAHO24 Status",
        "SPSU25 Status",
        "FAHO25 Status",
        "SPSU26 Status",
    )

    # Normalize key/used columns
    normalize_cols = ("SKU (Parent)", "Size Abbreviation", "SKU", "Collection") + exclusive_filter_cols
    for c in normalize_cols:
        if c in m.columns:
            m[c] = _nonempty(m[c]).str.upper()

    # Filter out EXCLUSIVE from any status/season column
    for c in exclusive_filter_cols:
        if c in m.columns:
            m = m[~m[c].eq("EXCLUSIVE")]

    # Other filter rules
    if "Collection" in m.columns:
        m = m[~m["Collection"].eq("HAREM PANTS")]
    if "SKU" in m.columns:
        m = m[~m["SKU"].str.contains("HIC", na=False)]

    # Drop obviously bad keys
    bad_parent = m["SKU (Parent)"].isin(BAD_SENTINELS) if "SKU (Parent)" in m.columns else False
    bad_size = m[size_abbr_col].isin(BAD_SENTINELS) if size_abbr_col in m.columns else False
    if isinstance(bad_parent, pd.Series) and isinstance(bad_size, pd.Series):
        m = m[~(bad_parent | bad_size)]

    # Preference: EXT_ID, then UPC
    m["__has_ext"] = _nonempty(m["EXT_ID"]).ne("") if "EXT_ID" in m.columns else False
    m["__has_upc"] = _nonempty(m["UPC"]).ne("") if "UPC" in m.columns else False
    m = m.sort_values(["__has_ext", "__has_upc"], ascending=[False, False], kind="mergesort")

    # Enforce uniqueness
    m = m.drop_duplicates(subset=["SKU (Parent)", size_abbr_col], keep="first")

    # Final guarantee
    dups = m.duplicated(subset=["SKU (Parent)", size_abbr_col])
    if dups.any():
        ct = int(dups.sum())
        raise RuntimeError(f"Master still not unique after alignment ({ct} duplicate keys).")

    return m.drop(columns=["__has_ext", "__has_upc"], errors="ignore")
