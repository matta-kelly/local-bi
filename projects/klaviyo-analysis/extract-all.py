import os
from connections.landl_db import customers, orders, order_lines, variants, listings
from connections.klaviyo import segments, profiles

# Config
OUTPUT_DIR = "klaviyo-analysis/input-data"
SINCE = "2025-12-09"

EXTRACT_SHOPIFY = True
EXTRACT_KLAVIYO = True

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Shopify
if EXTRACT_SHOPIFY:
    extractors = [
        ("shopify_customers.parquet", lambda: customers.get_customers(since=SINCE)),
        ("shopify_orders.parquet", lambda: orders.get_orders(since=SINCE)),
        ("shopify_order_lines.parquet", lambda: order_lines.get_order_lines(since=SINCE)),
        ("shopify_variants.parquet", lambda: variants.get_variants()),
        ("shopify_listings.parquet", lambda: listings.get_listings()),
    ]

    for filename, get_data in extractors:
        print(f"Extracting {filename}...")
        df = get_data()
        df.to_parquet(f"{OUTPUT_DIR}/{filename}", index=False)
        print(f"✓ {len(df)} rows -> {filename}")

# Klaviyo
if EXTRACT_KLAVIYO:
    print("Extracting segments...")
    segments_df = segments.get_segments(updated_after=SINCE)
    segments_df.to_parquet(f"{OUTPUT_DIR}/klaviyo_segments.parquet", index=False)
    print(f"✓ {len(segments_df)} rows -> klaviyo_segments.parquet")

    print("Extracting profiles and membership...")
    segment_ids = segments_df['segment_id'].tolist()
    profiles_df, membership_df = profiles.get_profiles_by_segment(segment_ids, joined_after=SINCE)
    profiles_df.to_parquet(f"{OUTPUT_DIR}/klaviyo_profiles.parquet", index=False)
    membership_df.to_parquet(f"{OUTPUT_DIR}/klaviyo_segment_membership.parquet", index=False)
    print(f"✓ {len(profiles_df)} profiles, {len(membership_df)} membership rows")

print("\n✓ All extracts complete")