import os
from connections.searchspring import collections
from connections.local import master_sku

OUTPUT_DIR = "projects/merchandising-analysis/input-data"
COLLECTIONS = [
    "best-sellers",
    "new-arrivals",
    "harem-pants",
    "master-healer",
    "holiday-gift-guide-2025",
]

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Master SKU
print("Extracting master_sku...")
df = master_sku.get_master_sku()
df.to_parquet(f"{OUTPUT_DIR}/master_sku.parquet", index=False)
print(f"✓ {len(df)} rows -> master_sku.parquet")

# Searchspring collections
for handle in COLLECTIONS:
    print(f"\nExtracting {handle}...")
    df = collections.get_collection_products(handle)
    df.to_parquet(f"{OUTPUT_DIR}/{handle}_products.parquet", index=False)
    print(f"✓ {len(df)} rows -> {handle}_products.parquet")

print("\n✓ All extracts complete")