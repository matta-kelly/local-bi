#!/usr/bin/env python3
"""
check_master_dupes_filtered_hic.py

One‐off script to scan “input/MS-EXT-ID.csv” row by row,
drop any rows where:
  • FAHO25 Status == "Exclusive"
  • Collection     == "HAREM PANTS"
  • SKU             contains "HIC"
Then identify duplicate (Parent SKU, Size Abbreviation) keys
among the remaining rows, write those duplicates (five columns)
to “output/master_dupes.csv” and print the duplicate count.
"""

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
INPUT_CSV  = ROOT / "input" / "utils/MS-EXT-ID.csv"
OUTPUT_DIR = ROOT / "output"
DUPES_CSV  = OUTPUT_DIR / "master_dupes.csv"

# ── STEP 1: Read header and find column indices ─────────────────────────────────
with INPUT_CSV.open(newline="", encoding="utf-8", errors="ignore") as f:
    reader = csv.reader(f)
    header = next(reader)
    header = [h.strip() for h in header]

    try:
        idx_parent      = header.index("SKU (Parent)")
        idx_size        = header.index("Size Abbreviation")
        idx_sku         = header.index("SKU")
        idx_upc         = header.index("UPC")
        idx_ext         = header.index("EXT_ID")
        idx_faho25      = header.index("FAHO25 Status")
        idx_collection  = header.index("Collection")
    except ValueError as e:
        raise RuntimeError(f"Required column missing: {e}")

# ── STEP 2: Build mapping from (parent, size) → list of rows, with filtering ────
duplicates_map = {}  # key: (parent, size) → list of row lists

with INPUT_CSV.open(newline="", encoding="utf-8", errors="ignore") as f:
    reader = csv.reader(f)
    next(reader)  # skip header
    for row in reader:
        parent     = row[idx_parent].strip().upper()
        size       = row[idx_size].strip().upper()
        sku        = row[idx_sku].strip().upper()
        faho25     = row[idx_faho25].strip().upper()
        collection = row[idx_collection].strip().upper()

        # Skip rows marked Exclusive, in Harem Pants collection, or SKU containing "HIC"
        if faho25 == "EXCLUSIVE" or collection == "HAREM PANTS" or "HIC" in sku:
            continue

        key = (parent, size)
        duplicates_map.setdefault(key, []).append(row)

# ── STEP 3: Extract keys with >1 occurrence ─────────────────────────────────────
duplicate_rows = []
for key, rows in duplicates_map.items():
    if len(rows) > 1:
        duplicate_rows.extend(rows)

total_dupes = len(duplicate_rows)
print(f"Duplicates found: {total_dupes}")

# ── STEP 4: Write duplicate rows to CSV (only five relevant columns) ───────────
if total_dupes:
    OUTPUT_DIR.mkdir(exist_ok=True)
    with DUPES_CSV.open("w", newline="", encoding="utf-8") as out_f:
        writer = csv.writer(out_f)
        writer.writerow(["SKU (Parent)", "Size Abbreviation", "SKU", "UPC", "EXT_ID"])
        for row in duplicate_rows:
            writer.writerow([
                row[idx_parent].strip(),
                row[idx_size].strip(),
                row[idx_sku].strip(),
                row[idx_upc].strip(),
                row[idx_ext].strip(),
            ])
