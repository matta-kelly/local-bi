# BI

Centralized data hub for Lotus & Luna analytics. Shared connectors, schemas, and processing — individual projects import what they need.

## Structure
```
BI/
├── connections/          # Shared data connectors
│   ├── processing.py     # DataFrame cleaning & type casting
│   ├── schemas.py        # Column definitions per table
│   ├── landl_db/         # Postgres warehouse
│   ├── klaviyo/          # Klaviyo API
│   ├── searchspring/     # Searchspring API
│   └── local/            # Local file loaders (CSVs, Excel)
├── projects/             # Analysis projects
└── .env                  # Credentials (not committed)
```

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Environment Variables
```
DB_USER=
DB_PASSWORD=
DB_NAME=
KLAVIYO_API_KEY=
SEARCHSPRING_SITE_ID=
SEARCHSPRING_BGFILTER_FIELD=
```

## Connectors

All connectors return processed pandas DataFrames — cleaned headers, typed columns per schema.

### landl_db

Postgres warehouse. Functions accept optional `since` param for incremental pulls.
```python
from connections.landl_db import customers, orders, order_lines, variants, listings

df = customers.get_customers(since="2024-12-01")
df = orders.get_orders(since="2024-12-01")
df = order_lines.get_order_lines(since="2024-12-01")
df = variants.get_variants()
df = listings.get_listings()
```

### klaviyo

Klaviyo API. Segments and profiles with optional date filters.
```python
from connections.klaviyo import segments, profiles

df = segments.get_segments(updated_after="2024-12-01")
profiles_df, membership_df = profiles.get_profiles_by_segment(segment_ids, joined_after="2024-12-01")
```

### searchspring

Searchspring API. Collection product rankings.
```python
from connections.searchspring import collections

df = collections.get_collection_products("best-sellers")
```

### local

Local file loaders.
```python
from connections.local import master_sku

df = master_sku.get_master_sku()
```

## Adding a New Connector

1. Create folder in `connections/` with `__init__.py`
2. Add schema to `connections/schemas.py`
3. Build functions that return `process_table(df, "schema_name")`

## Adding a New Project

1. Create folder in `projects/`
2. Add `extract.py` that imports from `connections/` and writes to `input-data/`
3. Add scripts that read from `input-data/` and write to `featured-data/` or `output/`git a