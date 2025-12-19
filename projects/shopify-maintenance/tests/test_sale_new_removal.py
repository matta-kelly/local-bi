# projects/shopify-maintenance/test_single_update.py
"""
Test: Update a single product to verify mutations work.
"""
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from connections.shopify.tags import remove_tags
from connections.shopify.badges import clear_badge

# ============================================================
# TEST CONFIGURATION
# ============================================================
# Pick one product from the stale list to test
TEST_PRODUCT_ID = "gid://shopify/Product/7829451178083"  # Nightfall Blue Lotus Moon Lounge Set
DRY_RUN = False  # Set False to actually execute


def main():
    print(f"=== Test Single Product Update ===")
    print(f"Product: {TEST_PRODUCT_ID}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'}\n")
    
    print("--- Removing 'New' tag ---")
    tag_results = remove_tags(TEST_PRODUCT_ID, "New", dry_run=DRY_RUN)
    print(f"Result: {tag_results}\n")
    
    print("--- Clearing badge ---")
    badge_results = clear_badge(TEST_PRODUCT_ID, dry_run=DRY_RUN)
    print(f"Result: {badge_results}\n")
    
    if DRY_RUN:
        print("DRY RUN complete. Set DRY_RUN = False to execute.")
    else:
        print("LIVE run complete. Check Shopify admin to verify.")


if __name__ == "__main__":
    main()