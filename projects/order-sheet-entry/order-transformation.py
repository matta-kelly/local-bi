#!/usr/bin/env python3
"""
order-transformation.py

Transform sales rep order sheets from Google Sheets into Odoo-ready import CSVs.

This is the main entry point - edit the CONFIG section below, then run:
    ../../.venv/bin/python order-transformation.py
"""

from pathlib import Path
import logging
import sys

import pandas as pd

# Add util-and-tests to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

from util_and_tests import (
    read_csv_smart,
    preprocess_df,
    build_master_with_ext_id,
    align_master_with_dupecheck,
    get_size_columns,
    breakout_matrix,
    enrich_with_master,
)


# ═══════════════════════════════════════════════════════════════════════════════
#  CONFIG - Edit these for each run
# ═══════════════════════════════════════════════════════════════════════════════

ROOT_DIR   = Path(__file__).resolve().parent
INPUT_DIR  = ROOT_DIR / "input"
OUTPUT_DIR = ROOT_DIR / "output"

# ┌─────────────────────────────────────────────────────────────────────────────┐
# │  SET THIS FOR EACH FILE YOU PROCESS                                        │
# └─────────────────────────────────────────────────────────────────────────────┘
ORDER_SHEET_FILE = INPUT_DIR / "order_sheets/order-sheet-input-sample.csv"

# Master data files (joined at runtime)
MASTER_SKU_FILE       = INPUT_DIR / "utils/master-sku.csv"
PRODUCT_VARIANT_FILE  = INPUT_DIR / "utils/product-variant-export.csv"

# Column names
PARENT_COL    = "Parent SKU"
SIZE_ABBR_COL = "Size Abbreviation"

# ┌─────────────────────────────────────────────────────────────────────────────┐
# │  UPDATE THESE PER BATCH / TRADE SHOW                                        │
# └─────────────────────────────────────────────────────────────────────────────┘
SALES_TEAM = "Wholesale"
TAG = "SURFJAN26"


# ═══════════════════════════════════════════════════════════════════════════════
#  SALES REP MAPPING
# ═══════════════════════════════════════════════════════════════════════════════

SALES_REP_PREFIX = {
    "JC": "Jada Claiborne",
    "JC1": "Janelle Clasby",
    "AK": "Alyssa Kallal",
    "AG": "Angela Gonzales",
    "CF": "Christina Freberg",
}


def infer_salesperson_from_filename(path: Path) -> tuple[str, str]:
    """
    Infer salesperson from filename prefix.
    Example: 'JC-1.csv' -> ('Jada Claiborne', 'JC')
    """
    stem = path.name
    token = stem.split('.')[0].split('-')[0]
    prefix = token[:2].upper()
    rep = SALES_REP_PREFIX.get(prefix, "Unknown")
    return rep, prefix


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Derive output filename from input
    output_file = OUTPUT_DIR / f"output-{ORDER_SHEET_FILE.name}"

    # Infer salesperson from filename prefix
    salesperson, prefix = infer_salesperson_from_filename(ORDER_SHEET_FILE)
    unknown_rep = (salesperson == "Unknown")
    if unknown_rep:
        logging.warning(
            "Unknown salesperson prefix '%s' in file '%s'. Using 'Unknown'.",
            prefix, ORDER_SHEET_FILE.name
        )

    # ── Load order sheet ──────────────────────────────────────────────────────
    logging.info("Reading order sheet …")
    order_df_raw = read_csv_smart(ORDER_SHEET_FILE, dtype=str, keep_default_na=False)
    order_df = preprocess_df(order_df_raw, PARENT_COL, SIZE_ABBR_COL)

    # ── Build master data ─────────────────────────────────────────────────────
    master_df = build_master_with_ext_id(MASTER_SKU_FILE, PRODUCT_VARIANT_FILE)
    master_df = align_master_with_dupecheck(master_df, SIZE_ABBR_COL)

    # ── Detect size columns ───────────────────────────────────────────────────
    logging.info("Detecting size columns …")
    size_cols = get_size_columns(order_df, master_df, SIZE_ABBR_COL)

    # ── Process orders ────────────────────────────────────────────────────────
    output_rows = []
    current_order_header = {}
    first_item_in_order = False

    for _, row in order_df.iterrows():
        customer = row.get("Customer", "").strip()

        # New order starts when Customer is non-empty
        if customer:
            first_item_in_order = True
            current_order_header = {
                "Salesperson": salesperson,
                "Sales Team":  SALES_TEAM,
                "Name":        customer,
                "Ship Date":   row.get("Ship Date", "").strip(),
                "Tags":        TAG,
            }

        # Break out row into long form
        breakout_df = breakout_matrix(row, size_cols, PARENT_COL)

        # Enrich with master data
        enriched_items = enrich_with_master(breakout_df, master_df, SIZE_ABBR_COL)

        # Build output rows
        for _, item in enriched_items.iterrows():
            if first_item_in_order:
                output_row = {
                    **current_order_header,
                    "SKU":         item["sku"],
                    "Quantity":    item["qty"],
                    "External ID": item["ext_id"],
                }
                first_item_in_order = False
            else:
                output_row = {
                    "Salesperson": "",
                    "Sales Team":  "",
                    "Name":        "",
                    "Ship Date":   "",
                    "SKU":         item["sku"],
                    "Quantity":    item["qty"],
                    "External ID": item["ext_id"],
                    "Tags":        "",
                }
            output_rows.append(output_row)

    # ── Write output ──────────────────────────────────────────────────────────
    if output_rows:
        out_df = pd.DataFrame(output_rows)
        OUTPUT_DIR.mkdir(exist_ok=True)

        columns_order = [
            "Salesperson", "Sales Team", "Name", "SKU", "Quantity", "External ID", "Tags"
        ]
        out_df.to_csv(output_file, columns=columns_order, index=False)
        logging.info("Wrote %d rows → %s", len(out_df), output_file)

        if unknown_rep:
            sidecar = OUTPUT_DIR / f"output-{ORDER_SHEET_FILE.stem}.log"
            sidecar.write_text(
                f"Unknown salesperson for input '{ORDER_SHEET_FILE.name}'. "
                f"Unrecognized prefix '{prefix}'.\n",
                encoding="utf-8"
            )
            logging.info("Wrote note → %s", sidecar)
    else:
        logging.info("No items found to write; output file not created.")


if __name__ == "__main__":
    main()
