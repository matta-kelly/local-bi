# data-ops

Centralized data hub for Lotus & Luna analytics and automations. Shared connectors, schemas, and processing — individual projects import what they need.

## Structure

```
data-ops/
├── connections/          # Shared data connectors (import from here)
│   ├── processing.py     # DataFrame cleaning & type casting
│   ├── schemas.py        # Column definitions per table
│   ├── landl_db/         # Postgres warehouse
│   ├── klaviyo/          # Klaviyo API
│   ├── searchspring/     # Searchspring API
│   └── local/            # Local file loaders (CSVs, Excel)
└── projects/             # Analysis projects (each self-contained)
```

## Working in This Repo

### Connectors Pattern

All connectors return processed pandas DataFrames — cleaned headers, typed columns per schema.

```python
# Example imports
from connections.landl_db import customers, orders, order_lines
from connections.klaviyo import segments, profiles
from connections.local import master_sku

df = orders.get_orders(since="2024-12-01")
```

### Project Pattern

Each project in `projects/` is self-contained:
- `extract.py` imports from `connections/` and writes to `input-data/`
- Processing scripts read from `input-data/`, write to `output/`
- Data directories (`input-data/`, `output-data/`, etc.) are gitignored

### Adding a New Connector

1. Create folder in `connections/` with `__init__.py`
2. Add schema to `connections/schemas.py`
3. Build functions that return `process_table(df, "schema_name")`

### Adding a New Project

1. Create folder in `projects/`
2. Add `AGENTS.md` with project-specific instructions
3. Add `extract.py` that imports from `connections/`

## Environment

```bash
# Activate venv from any project subdirectory
source ../../.venv/bin/activate
# Or from root
source .venv/bin/activate
```

Required env vars in `.env`: `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `KLAVIYO_API_KEY`, `SEARCHSPRING_SITE_ID`

## Subproject Instructions

Individual projects may have their own `AGENTS.md` with specific workflows. When working in a subproject, follow those instructions alongside these root guidelines.
