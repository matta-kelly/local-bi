# Order Sheet Entry

Transform sales rep order sheets from Google Sheets into Odoo-ready import CSVs.

## Quick Start

1. **Download** the rep's order sheet from Google Sheets as CSV
2. **Save** to `input/order_sheets/` with naming: `{PREFIX}-{N}.csv`
   - `JC` = Jada Claiborne, `JC1` = Janelle Clasby
   - `AK` = Alyssa Kallal
   - `AG` = Angela Gonzales
   - `CF` = Christina Freberg
   - Example: `AK-1.csv`, `JC-2.csv`
3. **Edit** `order-transformation.py` line 44 to point to your file:
   ```python
   ORDER_SHEET_FILE = INPUT_DIR / "order_sheets/AK-1.csv"
   ```
4. **Update** TAG if needed (line 59) for the current batch/trade show
5. **Run** (from this project directory):
   ```bash
   ../../.venv/bin/python order-transformation.py
   ```
6. **Output** appears at `output/output-{your-input-filename}.csv`

## Dependencies

Uses the shared BI venv at `../../.venv`. Required packages listed in `requirements.txt`.

## Project Structure

```
order-sheet-entry/
├── order-transformation.py      # Main entry point (config + orchestration)
├── input/
│   ├── order_sheets/            # Place order CSVs here
│   └── utils/
│       ├── master-sku.csv       # Product catalog
│       ├── product-variant-export.csv  # Odoo IDs
│       └── contacts.csv         # Customer database for ID matching
├── output/                      # Generated files
└── util_and_tests/              # Reusable utilities
    ├── __init__.py
    ├── csv_utils.py             # CSV reading, preprocessing, ship date cleaning
    ├── master_data.py           # Master SKU joining & filtering
    ├── transform.py             # Order transformation logic
    ├── contact_matching.py      # Customer name → database ID matching
    └── MS-dupe-check.py         # Validate master processing
```

## Input Format

Order sheets from Google Sheets need these columns:

| Column | Purpose |
|--------|---------|
| `Customer` | Customer name - marks the start of a new order |
| `Parent SKU` | Product identifier (matched against master list) |
| `Ship Date` | Requested ship date (ASAP, blank, or date like 3/15) |
| `Rep Notes` | Notes from sales rep (optional) |
| `OSFM` | Quantity for one-size-fits-most items |
| `S`, `M`, `L`, `XL` | Quantities by size |
| `PET-S`, `PET-M`, `PET-L` | Pet product sizes |

**Order grouping**: Each row with a non-empty `Customer` value starts a new order. Rows without a customer belong to the previous order.

## Output Format

| Column | Description |
|--------|-------------|
| `Salesperson` | Inferred from filename prefix (first line only) |
| `Sales Team` | Configured value (default: "Wholesale") |
| `Name` | Customer name from order sheet |
| `ID` | Matched database ID from contacts.csv |
| `SKU` | Child SKU from master list |
| `Quantity` | Order quantity |
| `External ID` | Odoo external ID |
| `Tags` | Batch tag (e.g., "SURFJAN26") |
| `Rep Notes` | Combined notes (see below) |

**Rep Notes format**: `SHIP DATE: 01/30/2026 | CUSTOMER MATCH: (Customer Name --> 12345) Exact, company | original rep notes here`

## Features

### Customer Matching
Automatically matches customer names to database IDs from `contacts.csv`:
- Exact match (case-sensitive)
- Normalized match (lowercase, no punctuation)
- Contains match (partial name matching)
- Prefers companies over individuals when multiple matches found

### Ship Date Cleaning
Handles messy ship date inputs:
- `ASAP` or blank → tomorrow's date
- `3/15` → `03/15/2026` (assumes current/next year)
- `4/1 apparel` → extracts `04/01/2026`
- Output format: `MM/DD/YYYY`

### Rep Notes Consolidation
Collects notes from all rows in an order, joins with ` --- ` separator.

## Configuration

Edit these values in `order-transformation.py`:

```python
# Line 44 - Set for each file you process
ORDER_SHEET_FILE = INPUT_DIR / "order_sheets/AK-1.csv"

# Lines 58-59 - Update per batch/trade show
SALES_TEAM = "Wholesale"
TAG = "SURFJAN26"

# Lines 66-72 - Sales rep prefix mapping
SALES_REP_PREFIX = {
    "JC": "Jada Claiborne",
    ...
}
```

## Master Data Files

| File | Source | Purpose |
|------|--------|---------|
| `master-sku.csv` | Internal system | Product catalog with SKU, sizes, UPC |
| `product-variant-export.csv` | Odoo export | Maps SKU → External ID |
| `contacts.csv` | Database export | Maps customer name → database ID |

### Filtering Rules

Products excluded if any status column == "EXCLUSIVE":
- Season, FAHO24 Status, SPSU25 Status, FAHO25 Status, SPSU26 Status

Also excluded:
- `Collection` == "HAREM PANTS"
- Any SKU containing "HIC"

### Size Fallbacks

If exact size match fails:
- `S` → `SM`
- `L` → `LXL`

## Validation

Run the dupe checker to validate master processing:
```bash
../../.venv/bin/python util_and_tests/MS-dupe-check.py
```

## Troubleshooting

### "Could not decode file with tried encodings"
Re-export from Excel/Sheets as UTF-8 CSV.

### "Could not detect any size-quantity columns"
Check column headers match expected sizes (S, M, L, OSFM, etc.).

### "Unmatched combo: Parent=X, Size=Y"
Parent SKU + size not in master list. Check for typos or missing products.

### "X SKUs have no External ID"
Export fresh `product-variant-export.csv` from Odoo.

### "Unmatched customers: X, Y, Z"
Customer names not found in contacts.csv. Add them or check spelling.

### "X orders had ship date defaulted to tomorrow"
Ship date was blank, ASAP, or unparseable. Review and fix in order sheet if needed.
