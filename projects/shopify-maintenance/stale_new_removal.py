# projects/shopify-maintenance/stale_new_removal.py
"""
Remove stale 'New' badges.
Products with 'New' badge published > X days ago → clear badge.
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
from connections.shopify.badges import clear_badge

# ============================================================
# CONFIGURATION
# ============================================================
STALE_THRESHOLD_DAYS = 60
DRY_RUN = True


def get_stale_new_badges(threshold_days: int) -> pd.DataFrame:
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
    print(f"=== Remove Stale 'New' Badges ===")
    print(f"Threshold: {STALE_THRESHOLD_DAYS} days")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}\n")
    
    stale_df = get_stale_new_badges(STALE_THRESHOLD_DAYS)
    print(f"Products with stale 'New' badge: {len(stale_df)}")
    
    if len(stale_df) == 0:
        print("Nothing to update.")
        return
    
    print(f"\n--- Products to clear 'New' badge ---")
    print(stale_df[['product_id', 'handle', 'title', 'badge', 'days_since_published']].to_string())
    
    badge_ids = stale_df['product_id'].tolist()
    print(f"\n--- Clearing badge from {len(badge_ids)} products ---")
    badge_results = clear_badge(badge_ids, dry_run=DRY_RUN)
    print(f"Badges: {len(badge_results['success'])} success, {len(badge_results['failed'])} failed")
    
    if DRY_RUN:
        print(f"\nDRY RUN - No changes made. Set DRY_RUN = False to execute.")


if __name__ == "__main__":
    main()