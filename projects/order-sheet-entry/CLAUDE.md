# CLAUDE.md

## What This Repo Does

Transforms "wide" order matrices from Google Sheets (one row per product, qty in size columns) into "long" Odoo import CSVs (one row per SKU/size).

## Project Structure

```
order-sheet-entry/
├── order-transformation.py      # Config + orchestration (edit this to run)
├── util_and_tests/              # Reusable logic
│   ├── csv_utils.py             # CSV reading, encoding, preprocessing
│   ├── master_data.py           # Master SKU join + filtering
│   └── transform.py             # Order transformation logic
├── input/
│   ├── order_sheets/*.csv       # Input files from reps
│   └── utils/
│       ├── master-sku.csv       # Product catalog
│       └── product-variant-export.csv  # Odoo External IDs
└── output/                      # Generated Odoo import files
```

## Running It

```bash
# Edit ORDER_SHEET_FILE on line 42, then run (from project dir)
../../.venv/bin/python order-transformation.py
```

Uses shared BI venv at `../../.venv`. Dependencies in `requirements.txt`.

## File Naming Convention

Input files: `{PREFIX}-{N}.csv` where PREFIX maps to salesperson:
- JC = Jada Claiborne
- AK = Alyssa Kallal
- AG = Angela Gonzales
- CF = Christina Freberg

## Architecture

```
order-transformation.py (orchestration)
│
├── CONFIG section (lines 31-56)
│   ├── ORDER_SHEET_FILE  ← set per run
│   ├── SALES_TEAM, TAG   ← set per batch
│   └── SALES_REP_PREFIX  ← business data
│
└── main()
    ├── read_csv_smart()           ← csv_utils.py
    ├── preprocess_df()            ← csv_utils.py
    ├── build_master_with_ext_id() ← master_data.py
    ├── align_master_with_dupecheck() ← master_data.py
    ├── get_size_columns()         ← transform.py
    ├── breakout_matrix()          ← transform.py
    └── enrich_with_master()       ← transform.py
```

## Utility Modules

### csv_utils.py
- `read_csv_smart()` - Try multiple encodings (utf-8, cp1252, latin1)
- `preprocess_df()` - Normalize whitespace, uppercase SKUs

### master_data.py
- `build_master_with_ext_id()` - Join master-sku + product-variant → EXT_ID
- `align_master_with_dupecheck()` - Filter exclusives, harem pants, HIC; dedupe

### transform.py
- `get_size_columns()` - Detect size/qty columns from headers
- `breakout_matrix()` - Wide → long conversion
- `enrich_with_master()` - Join to get child SKU, UPC, EXT_ID (with S→SM, L→LXL fallback)

## EXT_ID Join (Runtime)

```
master-sku.csv.SKU  ←→  product-variant-export.csv."Internal Reference"
                              ↓
                           ID column → EXT_ID
```

**Every run logs match statistics:**
```
INFO: EXT_ID join: 3527/4988 SKUs matched (70.7%), 1461 unmatched
```

## Config (update per batch)

```python
# order-transformation.py
ORDER_SHEET_FILE = INPUT_DIR / "order_sheets/AK-1.csv"  # line 42
SALES_TEAM = "Wholesale"   # line 55
TAG = "SURFJAN26"          # line 56
```

## Filtering Rules

Products excluded if:
- `FAHO25 Status` == "EXCLUSIVE"
- `Collection` == "HAREM PANTS"
- `SKU` contains "HIC"

## Common Tasks

**Process a new order file**: Edit line 42, run script

**Add a new sales rep**: Edit `SALES_REP_PREFIX` dict (line 63)

**Update master SKU list**: Replace `input/utils/master-sku.csv`

**Update External IDs**: Export fresh `product-variant-export.csv` from Odoo

**Change the batch tag**: Edit `TAG` on line 56

## Gotchas

- Order grouping uses `Customer` column - empty customer = continuation of previous order
- Size columns detected by matching headers against master's `Size Abbreviation` values
- Unmatched SKU/size combos logged as warnings and dropped
- Output has order header fields only on first line of each order
- ~30% of SKUs may lack External IDs if product-variant-export is stale
