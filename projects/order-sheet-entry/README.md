# Order Sheet Entry

Transform sales rep order sheets from Google Sheets into Odoo-ready import CSVs.

## Quick Start

1. **Download** the rep's order sheet from Google Sheets as CSV
2. **Clean** the CSV if needed (remove extra rows, fix encoding issues)
3. **Save** to `input/order_sheets/` with naming: `{PREFIX}-{N}.csv`
   - `JC` = Jada Claiborne
   - `AK` = Alyssa Kallal
   - `AG` = Angela Gonzales
   - `CF` = Christina Freberg
   - Example: `AK-1.csv`, `JC-2.csv`
4. **Edit** `order-transformation.py` line 42 to point to your file:
   ```python
   ORDER_SHEET_FILE = INPUT_DIR / "order_sheets/AK-1.csv"
   ```
5. **Update** TAG if needed (line 56) for the current batch/trade show
6. **Run** (from this project directory):
   ```bash
   ../../.venv/bin/python order-transformation.py
   ```
7. **Output** appears at `output/output-{your-input-filename}.csv`

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
│       └── product-variant-export.csv  # Odoo IDs
├── output/                      # Generated files
└── util_and_tests/              # Reusable utilities
    ├── __init__.py
    ├── csv_utils.py             # CSV reading, DataFrame preprocessing
    ├── master_data.py           # Master SKU joining & filtering
    └── transform.py             # Order transformation logic
```

## Input Format

Order sheets from Google Sheets must have these columns:

| Column | Purpose |
|--------|---------|
| `Customer` | Customer name - marks the start of a new order |
| `Parent SKU` | Product identifier (matched against master list) |
| `Ship Date` | Requested ship date |
| `OSFM / QTY` | Quantity for one-size-fits-most items |
| `S (SM)` | Quantity for size Small |
| `M` | Quantity for size Medium |
| `L (LXL)` | Quantity for size Large |
| `XL` | Quantity for size XL |
| `PET-S`, `PET-M`, `PET-L` | Pet product sizes |

**How orders are grouped**: Each row with a non-empty `Customer` value starts a new order. Subsequent rows without a customer belong to the same order.

## Output Format

The generated CSV has these columns for Odoo import:

| Column | Description |
|--------|-------------|
| `Salesperson` | Inferred from filename prefix (only on first line of order) |
| `Sales Team` | Currently hardcoded to "Wholesale" |
| `Name` | Customer name (only on first line of order) |
| `SKU` | Child SKU from master list |
| `Quantity` | Order quantity |
| `External ID` | Odoo external ID from master list |
| `Tags` | Batch tag (e.g., "SURFJAN26") |

## Configuration

Edit these values in `order-transformation.py`:

```python
# Line 42 - Set for each file you process
ORDER_SHEET_FILE = INPUT_DIR / "order_sheets/AK-1.csv"

# Line 55-56 - Update per batch/trade show
SALES_TEAM = "Wholesale"
TAG = "SURFJAN26"

# Lines 63-68 - Sales rep prefix mapping
SALES_REP_PREFIX = {
    "JC": "Jada Claiborne",
    ...
}
```

## Master Data Files

The script joins two files at runtime to build the complete master list:

| File | Source | Purpose |
|------|--------|---------|
| `input/utils/master-sku.csv` | Internal system | Product catalog with SKU, sizes, UPC |
| `input/utils/product-variant-export.csv` | Odoo export | Maps `Internal Reference` → `ID` (External ID) |

**The join happens automatically on every run**, so you don't need to manually merge these files.

### Updating Master Data

1. **master-sku.csv**: Export fresh from your internal product system
2. **product-variant-export.csv**: Export from Odoo (Settings → Data → Export → Product Variants with Internal Reference & ID columns)

### Join Statistics

On every run, you'll see match statistics:
```
INFO: EXT_ID join: 3527/4988 SKUs matched (70.7%), 1461 unmatched
```

- **Matched**: SKUs that have an External ID in Odoo
- **Unmatched**: SKUs without External ID (will be empty in output - may need manual entry in Odoo)

### Filtering Rules

These items are automatically excluded during processing:
- Products with `FAHO25 Status` = "EXCLUSIVE"
- Products in `Collection` = "HAREM PANTS"
- Any SKU containing "HIC"

### Size Fallbacks

If an exact size match fails, the script tries:
- `S` → `SM`
- `L` → `LXL`

Unmatched parent/size combinations are logged as warnings and dropped from output.

## Utility Modules

Located in `util_and_tests/`:

| Module | Purpose |
|--------|---------|
| `csv_utils.py` | Smart CSV reading (handles encoding), DataFrame preprocessing |
| `master_data.py` | Build master with EXT_ID join, filtering & deduplication |
| `transform.py` | Size column detection, wide→long conversion, master enrichment |

Legacy scripts (can be removed):
| Script | Purpose |
|--------|---------|
| `MS-dupe-check.py` | Check master for duplicate (Parent, Size) combos |
| `matrix-breakout.py` | Earlier version (superseded by `transform.py`) |

## Troubleshooting

### "Could not decode file with tried encodings"
The file has an unusual encoding. Try opening in Excel/Sheets and re-exporting as UTF-8 CSV.

### "Could not detect any size-quantity columns"
The order sheet column headers don't match expected size names. Check that columns like `S (SM)`, `M`, `L (LXL)` exist.

### "Unmatched combo: Parent=X, Size=Y"
The parent SKU + size combination doesn't exist in the master list. Either:
- The product is missing from `master-sku.csv`
- The parent SKU has a typo in the order sheet
- The size column wasn't detected correctly

### "X SKUs have no External ID"
The script found SKUs in master-sku.csv that don't have a matching Internal Reference in product-variant-export.csv. The output will have empty External ID fields for these. To fix:
- Export a fresh `product-variant-export.csv` from Odoo
- Or manually add the External IDs in Odoo after import

### Unknown salesperson warning
The filename prefix doesn't match known reps (JC, AK, AG, CF). The output will show "Unknown" as salesperson.

## Post-Processing

After generating the output CSV, there may be manual steps required:
- Verify/update database IDs in Odoo
- Check for any logged warnings about unmatched products
- Validate totals match expected order quantities
