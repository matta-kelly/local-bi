# projects/shopify-maintenance/stale_new_removal.py
"""
Find and remove stale 'New' tags and badges.
Two separate lists:
  1. Products with 'New' tag, published > X days → remove tag
  2. Products with 'New' badge, published > X days → clear badge
If a product has both, it's on both lists.
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from connections.landl_db.listings import get_listings
from connections.landl_db.get_listing_tags import get_listing_tags
from connections.shopify.tags import remove_tags
from connections.shopify.badges import clear_badge

# ============================================================
# CONFIGURATION
# ============================================================
STALE_THRESHOLD_DAYS = 60
DRY_RUN = False


def get_stale_products_with_new_tag(threshold_days: int) -> pd.DataFrame:
    """Find active products with 'New' tag, published > threshold_days ago."""
    df = get_listings()
    df = df[df['status'] == 'ACTIVE'].copy()
    
    tags_df = get_listing_tags()
    products_with_new_tag = tags_df[tags_df['tag'] == 'New']['product_id'].unique()
    
    cutoff_date = datetime.now() - timedelta(days=threshold_days)
    
    stale = df[
        (df['product_id'].isin(products_with_new_tag)) &
        (df['published_at'].notna()) &
        (df['published_at'] < cutoff_date)
    ].copy()
    
    if len(stale) > 0:
        stale['days_since_published'] = (datetime.now() - stale['published_at']).dt.days
        stale = stale.sort_values('days_since_published', ascending=False)
    
    return stale


def get_stale_products_with_new_badge(threshold_days: int) -> pd.DataFrame:
    """Find active products with 'New' badge, published > threshold_days ago."""
    df = get_listings()
    df = df[df['status'] == 'ACTIVE'].copy()
    
    cutoff_date = datetime.now() - timedelta(days=threshold_days)
    
    stale = df[
        (df['badge'] == 'New') &
        (df['published_at'].notna()) &
        (df['published_at'] < cutoff_date)
    ].copy()
    
    if len(stale) > 0:
        stale['days_since_published'] = (datetime.now() - stale['published_at']).dt.days
        stale = stale.sort_values('days_since_published', ascending=False)
    
    return stale


def main():
    print(f"=== Remove Stale 'New' Tags & Badges ===")
    print(f"Threshold: {STALE_THRESHOLD_DAYS} days")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}\n")
    
    # Get two separate lists
    stale_tags_df = get_stale_products_with_new_tag(STALE_THRESHOLD_DAYS)
    stale_badges_df = get_stale_products_with_new_badge(STALE_THRESHOLD_DAYS)
    
    print(f"Products with stale 'New' tag: {len(stale_tags_df)}")
    print(f"Products with stale 'New' badge: {len(stale_badges_df)}")
    
    # Remove tags
    if len(stale_tags_df) > 0:
        print(f"\n--- Products to remove 'New' tag ---")
        print(stale_tags_df[['product_id', 'handle', 'title', 'days_since_published']].to_string())
        
        tag_ids = stale_tags_df['product_id'].tolist()
        print(f"\n--- Removing 'New' tag from {len(tag_ids)} products ---")
        tag_results = remove_tags(tag_ids, "New", dry_run=DRY_RUN)
        print(f"Tags: {len(tag_results['success'])} success, {len(tag_results['failed'])} failed")
    else:
        print("\nNo stale 'New' tags to remove.")
    
    # Clear badges
    if len(stale_badges_df) > 0:
        print(f"\n--- Products to clear 'New' badge ---")
        print(stale_badges_df[['product_id', 'handle', 'title', 'badge', 'days_since_published']].to_string())
        
        badge_ids = stale_badges_df['product_id'].tolist()
        print(f"\n--- Clearing badge from {len(badge_ids)} products ---")
        badge_results = clear_badge(badge_ids, dry_run=DRY_RUN)
        print(f"Badges: {len(badge_results['success'])} success, {len(badge_results['failed'])} failed")
    else:
        print("\nNo stale 'New' badges to clear.")
    
    if DRY_RUN:
        print(f"\nDRY RUN - No changes made. Set DRY_RUN = False to execute.")


if __name__ == "__main__":
    main()