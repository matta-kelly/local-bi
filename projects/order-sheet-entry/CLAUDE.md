# CLAUDE.md

## What This Repo Does

Transforms "wide" order matrices from Google Sheets (one row per product, qty in size columns) into "long" Odoo import CSVs (one row per SKU/size). Also matches customer names to database IDs and cleans ship dates.

## Project Structure

```
order-sheet-entry/
├── order-transformation.py      # Config + orchestration (edit this to run)
├── util_and_tests/              # Reusable logic
│   ├── csv_utils.py             # CSV reading, encoding, preprocessing, ship date cleaning
│   ├── master_data.py           # Master SKU join + filtering
│   ├── transform.py             # Order transformation logic
│   ├── contact_matching.py      # Customer name → database ID matching
│   └── MS-dupe-check.py         # Validate master processing (uses shared utils)
├── input/
│   ├── order_sheets/*.csv       # Input files from reps
│   └── utils/
│       ├── master-sku.csv       # Product catalog
│       ├── product-variant-export.csv  # Odoo External IDs
│       └── contacts.csv         # Customer database (Name, ID, Is a Company)
└── output/                      # Generated Odoo import files
```

## Running It

```bash
# Edit ORDER_SHEET_FILE on line 44, then run (from project dir)
../../.venv/bin/python order-transformation.py
```

## File Naming Convention

Input files: `{PREFIX}-{N}.csv` where PREFIX maps to salesperson:
- JC = Jada Claiborne, JC1 = Janelle Clasby
- AK = Alyssa Kallal
- AG = Angela Gonzales
- CF = Christina Freberg

## Architecture

```
order-transformation.py (orchestration)
│
├── CONFIG section (lines 33-59)
│   ├── ORDER_SHEET_FILE  ← set per run
│   ├── SALES_TEAM, TAG   ← set per batch
│   └── SALES_REP_PREFIX  ← business data
│
└── main()
    ├── read_csv_smart()           ← csv_utils.py
    ├── preprocess_df()            ← csv_utils.py
    ├── clean_ship_date()          ← csv_utils.py
    ├── build_master_with_ext_id() ← master_data.py
    ├── align_master_with_dupecheck() ← master_data.py
    ├── get_size_columns()         ← transform.py
    ├── breakout_matrix()          ← transform.py
    ├── enrich_with_master()       ← transform.py
    └── match_all_customers()      ← contact_matching.py
```

## Utility Modules

### csv_utils.py
- `read_csv_smart()` - Try multiple encodings (utf-8, cp1252, latin1)
- `preprocess_df()` - Normalize whitespace, uppercase SKUs
- `clean_ship_date()` - Parse dates from messy input (ASAP→tomorrow, "3/15 apparel"→03/15/YYYY)

### master_data.py
- `build_master_with_ext_id()` - Join master-sku + product-variant → EXT_ID
- `align_master_with_dupecheck()` - Filter exclusives, harem pants, HIC; dedupe

### transform.py
- `get_size_columns()` - Detect size/qty columns from headers
- `breakout_matrix()` - Wide → long conversion
- `enrich_with_master()` - Join to get child SKU, UPC, EXT_ID (with S→SM, L→LXL fallback)

### contact_matching.py
- `match_all_customers()` - Match customer names to database IDs
- Priority: exact → normalized → contains → partial match
- Prefers companies over individuals when multiple matches

## Output Format

Columns: `Salesperson, Sales Team, Name, ID, SKU, Quantity, External ID, Tags, Rep Notes`

Rep Notes format: `SHIP DATE: MM/DD/YYYY | CUSTOMER MATCH: (Input Name --> DB ID) Match type | original notes`

## Filtering Rules

Products excluded if any of these columns == "EXCLUSIVE":
- Season, FAHO24 Status, SPSU25 Status, FAHO25 Status, SPSU26 Status

Also excluded:
- `Collection` == "HAREM PANTS"
- `SKU` contains "HIC"

## Common Tasks

**Process a new order file**: Edit line 44, run script

**Validate master processing**: `python util_and_tests/MS-dupe-check.py`

**Add a new sales rep**: Edit `SALES_REP_PREFIX` dict (line 66)

**Update master SKU list**: Replace `input/utils/master-sku.csv`

**Update External IDs**: Export fresh `product-variant-export.csv` from Odoo

**Update contacts**: Export fresh `contacts.csv` from database

## Gotchas

- Order grouping uses `Customer` column - empty customer = continuation of previous order
- Size columns detected by matching headers against master's `Size Abbreviation` values
- Unmatched SKU/size combos logged as warnings and dropped
- Ship dates default to tomorrow if blank, ASAP, or unparseable
- Customer match info and ship date go in Rep Notes (not separate columns)
