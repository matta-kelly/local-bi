import os
from datetime import datetime, timedelta
from connections.searchspring import collections
from connections.local import master_sku
from connections.landl_db import variants, listings, order_lines, orders, stock_daily


OUTPUT_DIR = "projects/merchandising-analysis/data"
SINCE = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
COLLECTIONS = [
    "best-sellers",
    "new-arrivals",
    "harem-pants",
    "master-healer",
    "holiday-gift-guide-2025",
]

os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"Extracting data since {SINCE} (30 days)")

# Master SKU
print("\nExtracting master_sku...")
df = master_sku.get_master_sku()
df.to_parquet(f"{OUTPUT_DIR}/master_sku.parquet", index=False)
print(f"✓ {len(df)} rows -> master_sku.parquet")

# Shopify
print("\nExtracting shopify_variants...")
df = variants.get_variants()
df.to_parquet(f"{OUTPUT_DIR}/shopify_variants.parquet", index=False)
print(f"✓ {len(df)} rows -> shopify_variants.parquet")

print("\nExtracting shopify_listings...")
df = listings.get_listings()
df.to_parquet(f"{OUTPUT_DIR}/shopify_listings.parquet", index=False)
print(f"✓ {len(df)} rows -> shopify_listings.parquet")

print("\nExtracting shopify_orders...")
df = orders.get_orders(since=SINCE)
df.to_parquet(f"{OUTPUT_DIR}/shopify_orders.parquet", index=False)
print(f"✓ {len(df)} rows -> shopify_orders.parquet")

print("\nExtracting shopify_order_lines...")
df = order_lines.get_order_lines(since=SINCE)
df.to_parquet(f"{OUTPUT_DIR}/shopify_order_lines.parquet", index=False)
print(f"✓ {len(df)} rows -> shopify_order_lines.parquet")

# Stock
print("\nExtracting stock_daily...")
df = stock_daily.get_stock_daily(since=SINCE)
df.to_parquet(f"{OUTPUT_DIR}/stock_daily.parquet", index=False)
print(f"✓ {len(df)} rows -> stock_daily.parquet")

# Searchspring collections
for handle in COLLECTIONS:
    print(f"\nExtracting {handle}...")
    df = collections.get_collection_products(handle)
    df.to_parquet(f"{OUTPUT_DIR}/{handle}_products.parquet", index=False)
    print(f"✓ {len(df)} rows -> {handle}_products.parquet")

print("\n✓ All extracts complete")