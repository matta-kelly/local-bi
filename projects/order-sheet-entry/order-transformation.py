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
    clean_ship_date,
    build_master_with_ext_id,
    align_master_with_dupecheck,
    get_size_columns,
    breakout_matrix,
    enrich_with_master,
    match_all_customers,
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
ORDER_SHEET_FILE = INPUT_DIR / "order_sheets/JC-3.csv"

# Master data files (joined at runtime)
MASTER_SKU_FILE       = INPUT_DIR / "utils/master-sku.csv"
PRODUCT_VARIANT_FILE  = INPUT_DIR / "utils/product-variant-export.csv"
CONTACTS_FILE         = INPUT_DIR / "utils/contacts.csv"

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
    current_ship_date = ""
    first_item_in_order = False
    missing_ext_ids = []
    defaulted_ship_dates = []

    # Track notes and first row index per order
    current_order_notes = []
    current_order_first_idx = None

    for _, row in order_df.iterrows():
        customer = row.get("Customer", "").strip()
        rep_note = row.get("Rep Notes", "").strip()

        # New order starts when Customer is non-empty
        if customer:
            # Finalize previous order's notes
            if current_order_first_idx is not None and current_order_notes:
                combined_notes = " --- ".join(current_order_notes)
                output_rows[current_order_first_idx]["Rep Notes"] = combined_notes

            first_item_in_order = True
            raw_ship_date = row.get("Ship Date", "").strip()
            current_ship_date, was_defaulted = clean_ship_date(raw_ship_date)
            if was_defaulted:
                defaulted_ship_dates.append((customer, raw_ship_date or "(blank)"))
            current_order_header = {
                "Salesperson": salesperson,
                "Sales Team":  SALES_TEAM,
                "Name":        customer,
                "_ship_date":  current_ship_date,  # stored for notes, not output
                "Tags":        TAG,
                "Rep Notes":   "",
            }
            current_order_notes = []
            current_order_first_idx = None

        # Collect non-empty notes for this order
        if rep_note:
            current_order_notes.append(rep_note)

        # Break out row into long form
        breakout_df = breakout_matrix(row, size_cols, PARENT_COL)

        # Enrich with master data
        enriched_items = enrich_with_master(breakout_df, master_df, SIZE_ABBR_COL)

        # Build output rows
        for _, item in enriched_items.iterrows():
            sku = item["sku"]
            ext_id = item["ext_id"]

            # Track missing data
            if not ext_id:
                missing_ext_ids.append(sku)

            if first_item_in_order:
                current_order_first_idx = len(output_rows)
                output_row = {
                    **current_order_header,
                    "SKU":          sku,
                    "Quantity":     item["qty"],
                    "External ID":  ext_id,
                }
                first_item_in_order = False
            else:
                output_row = {
                    "Salesperson": "",
                    "Sales Team":  "",
                    "Name":        "",
                    "_ship_date":  "",
                    "SKU":         sku,
                    "Quantity":    item["qty"],
                    "External ID": ext_id,
                    "Tags":        "",
                    "Rep Notes":   "",
                }
            output_rows.append(output_row)

    # Finalize last order's notes
    if current_order_first_idx is not None and current_order_notes:
        combined_notes = " --- ".join(current_order_notes)
        output_rows[current_order_first_idx]["Rep Notes"] = combined_notes

    # ── Log missing data ───────────────────────────────────────────────────────
    if missing_ext_ids:
        logging.warning(
            "%d SKUs missing External ID: %s",
            len(missing_ext_ids), ", ".join(missing_ext_ids[:20]) + ("..." if len(missing_ext_ids) > 20 else "")
        )
    if defaulted_ship_dates:
        logging.warning(
            "%d orders had ship date defaulted to tomorrow:",
            len(defaulted_ship_dates)
        )
        for customer, raw in defaulted_ship_dates[:10]:
            logging.warning("  %s: %s", customer, raw)
        if len(defaulted_ship_dates) > 10:
            logging.warning("  ... and %d more", len(defaulted_ship_dates) - 10)

    # ── Match customers to contact IDs and build final Rep Notes ────────────────
    if output_rows:
        matches = match_all_customers(output_rows, CONTACTS_FILE)

        for row in output_rows:
            name = row.get("Name", "").strip()
            ship_date = row.pop("_ship_date", "")  # remove internal field
            existing_notes = row.get("Rep Notes", "")

            if name:
                # Build notes parts
                notes_parts = []

                # 1. Ship Date
                if ship_date:
                    notes_parts.append(f"SHIP DATE: {ship_date}")

                # 2. Customer Match
                if name in matches:
                    matched_id, match_info = matches[name]
                    row["ID"] = matched_id
                    # Convert "[Exact match, company]" to "CUSTOMER MATCH: (Name --> ID) Exact, company"
                    match_type = match_info.strip("[]")
                    if matched_id:
                        notes_parts.append(f"CUSTOMER MATCH: ({name} --> {matched_id}) {match_type}")
                    else:
                        notes_parts.append(f"CUSTOMER MATCH: ({name}) No match")
                else:
                    row["ID"] = ""
                    notes_parts.append(f"CUSTOMER MATCH: ({name}) No match")

                # 3. Original rep notes
                if existing_notes:
                    notes_parts.append(existing_notes)

                row["Rep Notes"] = " | ".join(notes_parts)
            else:
                row["ID"] = ""

    # ── Write output ──────────────────────────────────────────────────────────
    if output_rows:
        out_df = pd.DataFrame(output_rows)
        OUTPUT_DIR.mkdir(exist_ok=True)

        columns_order = [
            "Salesperson", "Sales Team", "Name", "ID", "SKU", "Quantity", "External ID", "Tags", "Rep Notes"
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
