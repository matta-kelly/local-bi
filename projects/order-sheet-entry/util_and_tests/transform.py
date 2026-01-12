"""
transform.py

Order transformation logic: wide-to-long conversion and master data enrichment.
"""

import logging

import pandas as pd


# ───────────────────────────────────────────────────────────────────────────────
#  SIZE COLUMN DETECTION
# ───────────────────────────────────────────────────────────────────────────────

def get_size_columns(
    order_df: pd.DataFrame,
    master_df: pd.DataFrame,
    size_abbr_col: str = "Size Abbreviation",
) -> dict[str, str]:
    """
    Identify which order-sheet columns correspond to size-qty fields.

    Matches column headers against known size abbreviations from the master list.

    Args:
        order_df: Order sheet DataFrame
        master_df: Master SKU DataFrame
        size_abbr_col: Name of size abbreviation column in master

    Returns:
        Dict mapping {order_column_name → canonical_size_abbreviation}

    Raises:
        ValueError: If no size columns detected
    """
    size_set = (
        master_df[size_abbr_col]
        .dropna()
        .unique()
        .astype(str)
    )
    size_set = {s.upper() for s in size_set} | {"OSFM"}  # ensure OSFM included

    size_cols: dict[str, str] = {}
    for col in order_df.columns:
        col_clean = col.upper().replace("QTY", "").replace("/", " ").strip()
        for size in size_set:
            if size in col_clean.split():  # exact token match
                size_cols[col] = size
                break

    if not size_cols:
        raise ValueError("Could not detect any size-quantity columns!")

    return size_cols


# ───────────────────────────────────────────────────────────────────────────────
#  WIDE TO LONG CONVERSION
# ───────────────────────────────────────────────────────────────────────────────

def breakout_matrix(
    order_row: pd.Series,
    size_cols: dict[str, str],
    parent_col: str = "Parent SKU",
) -> pd.DataFrame:
    """
    Convert one order-sheet row into long form.

    Takes a single row (one parent SKU) and expands it into multiple rows,
    one for each size with non-zero quantity.

    Args:
        order_row: Single row from order sheet
        size_cols: Dict mapping column names to size abbreviations
        parent_col: Name of parent SKU column

    Returns:
        DataFrame with columns [parent, size, qty] for nonzero quantities
    """
    records: list[dict] = []
    parent = order_row[parent_col]

    if not parent:
        return pd.DataFrame(columns=["parent", "size", "qty"])

    for col, size_abbr in size_cols.items():
        qty = order_row.get(col, "")
        qty = pd.to_numeric(qty, errors="coerce")
        if pd.isna(qty) or qty == 0:
            continue
        records.append({
            "parent": parent,
            "size": size_abbr,
            "qty": int(qty),
        })

    return pd.DataFrame.from_records(records)


# ───────────────────────────────────────────────────────────────────────────────
#  MASTER DATA ENRICHMENT
# ───────────────────────────────────────────────────────────────────────────────

def enrich_with_master(
    breakout_df: pd.DataFrame,
    master_df: pd.DataFrame,
    size_abbr_col: str = "Size Abbreviation",
) -> pd.DataFrame:
    """
    Enrich breakout rows with master data (SKU, UPC, EXT_ID).

    Performs left-join against master list with size fallbacks:
    - If size="S" fails, tries "SM"
    - If size="L" fails, tries "LXL"

    Args:
        breakout_df: DataFrame with [parent, size, qty] columns
        master_df: Master SKU DataFrame
        size_abbr_col: Name of size abbreviation column

    Returns:
        DataFrame with columns [sku, barcode, qty, ext_id]
    """
    join_cols = breakout_df.rename(columns={"parent": "SKU (Parent)", "size": size_abbr_col})

    # First pass: exact match on (parent, size)
    merged = join_cols.merge(
        master_df[["SKU (Parent)", size_abbr_col, "SKU", "UPC", "EXT_ID"]],
        how="left",
        on=["SKU (Parent)", size_abbr_col],
        validate="many_to_one",
    )

    keep_success = merged[~merged["SKU"].isna()].copy()
    missing = merged[merged["SKU"].isna()].copy()

    # Split missing into S, L, and other sizes
    group_S = missing[missing[size_abbr_col] == "S"].copy()
    group_L = missing[missing[size_abbr_col] == "L"].copy()
    group_other = missing[~missing[size_abbr_col].isin({"S", "L"})].copy()

    # Second pass for S -> SM
    if not group_S.empty:
        temp_S = group_S[["SKU (Parent)", size_abbr_col, "qty"]].copy()
        temp_S[size_abbr_col] = "SM"
        second_S = temp_S.merge(
            master_df[["SKU (Parent)", size_abbr_col, "SKU", "UPC", "EXT_ID"]],
            how="left",
            on=["SKU (Parent)", size_abbr_col],
            validate="many_to_one",
        )
        hits_S = second_S[~second_S["SKU"].isna()].copy()
        still_missing_S = second_S[second_S["SKU"].isna()].copy()
        hits_S[size_abbr_col] = "S"
    else:
        hits_S = pd.DataFrame(columns=merged.columns)
        still_missing_S = pd.DataFrame(columns=merged.columns)

    # Second pass for L -> LXL
    if not group_L.empty:
        temp_L = group_L[["SKU (Parent)", size_abbr_col, "qty"]].copy()
        temp_L[size_abbr_col] = "LXL"
        second_L = temp_L.merge(
            master_df[["SKU (Parent)", size_abbr_col, "SKU", "UPC", "EXT_ID"]],
            how="left",
            on=["SKU (Parent)", size_abbr_col],
            validate="many_to_one",
        )
        hits_L = second_L[~second_L["SKU"].isna()].copy()
        still_missing_L = second_L[second_L["SKU"].isna()].copy()
        hits_L[size_abbr_col] = "L"
    else:
        hits_L = pd.DataFrame(columns=merged.columns)
        still_missing_L = pd.DataFrame(columns=merged.columns)

    # Anything left is truly unmatched
    still_unmatched = pd.concat([group_other, still_missing_S, still_missing_L], ignore_index=True)
    if not still_unmatched.empty:
        combos = (
            still_unmatched[["SKU (Parent)", size_abbr_col]]
            .drop_duplicates()
            .sort_values(["SKU (Parent)", size_abbr_col])
        )
        for _, row in combos.iterrows():
            logging.warning(
                "Unmatched combo: Parent=%s, Size=%s",
                row["SKU (Parent)"], row[size_abbr_col]
            )
        logging.warning(
            "%d parent/size rows had no match in master and were dropped.",
            len(still_unmatched),
        )

    final_hits = pd.concat([keep_success, hits_S, hits_L], ignore_index=True)
    return (
        final_hits[["SKU", "UPC", "qty", "EXT_ID"]]
        .rename(columns={"SKU": "sku", "UPC": "barcode", "qty": "qty", "EXT_ID": "ext_id"})
        .astype({"qty": int})
    )
