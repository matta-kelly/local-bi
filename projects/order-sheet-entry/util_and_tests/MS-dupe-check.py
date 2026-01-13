#!/usr/bin/env python3
"""
MS-dupe-check.py

Validate master SKU processing by checking for duplicate (Parent SKU, Size Abbreviation)
keys after applying all filters.

Uses the same processing pipeline as order-transformation.py:
  - build_master_with_ext_id() to join master-sku + product-variant
  - align_master_with_dupecheck() to apply filtering

If duplicates remain after processing, something is wrong with our deduplication logic.
"""

import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from util_and_tests import (
    build_master_with_ext_id,
    align_master_with_dupecheck,
)

ROOT = Path(__file__).resolve().parent.parent
MASTER_SKU_FILE = ROOT / "input" / "utils" / "master-sku.csv"
PRODUCT_VARIANT_FILE = ROOT / "input" / "utils" / "product-variant-export.csv"
OUTPUT_DIR = ROOT / "output"
DUPES_CSV = OUTPUT_DIR / "master_dupes.csv"

SIZE_ABBR_COL = "Size Abbreviation"


def main():
    print("Building master SKU with EXT_ID (on-demand)...")
    master_df = build_master_with_ext_id(MASTER_SKU_FILE, PRODUCT_VARIANT_FILE)

    print(f"Raw master rows: {len(master_df)}")

    print("Applying filters and deduplication...")
    try:
        filtered_df = align_master_with_dupecheck(master_df, SIZE_ABBR_COL)
        print(f"Filtered master rows: {len(filtered_df)}")
        print("No duplicates found - processing is valid.")
    except RuntimeError as e:
        # align_master_with_dupecheck raises if duplicates remain
        print(f"ERROR: {e}")

        # Find and report the duplicates manually for debugging
        m = master_df.copy()
        # Apply same normalization to see what's duplicated
        for c in ("SKU (Parent)", SIZE_ABBR_COL):
            if c in m.columns:
                m[c] = m[c].fillna("").astype(str).str.strip().str.upper()

        dupes = m[m.duplicated(subset=["SKU (Parent)", SIZE_ABBR_COL], keep=False)]
        dupes = dupes.sort_values(["SKU (Parent)", SIZE_ABBR_COL])

        OUTPUT_DIR.mkdir(exist_ok=True)
        output_cols = ["SKU (Parent)", SIZE_ABBR_COL, "SKU", "UPC", "EXT_ID"]
        available_cols = [c for c in output_cols if c in dupes.columns]
        dupes[available_cols].to_csv(DUPES_CSV, index=False)

        print(f"Wrote {len(dupes)} duplicate rows to {DUPES_CSV}")
        sys.exit(1)


if __name__ == "__main__":
    main()
