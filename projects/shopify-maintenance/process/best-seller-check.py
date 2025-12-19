# projects/one-off/best-seller-check.py
"""
Find top 100 best-selling listings by revenue in the last N days.
Excludes marked-down items (compare_at_price > price).
Aggregates order lines -> variants -> products.
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

# ============================================================
# CONFIGURATION
# ============================================================
LOOKBACK_DAYS = 60
TOP_N = 100

# ============================================================
# SETUP
# ============================================================
project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from connections.landl_db.listings import get_listings
from connections.landl_db.order_lines import get_order_lines
from connections.landl_db.variants import get_variants


def load_data(since: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load order lines, variants, and listings."""
    print(f"Loading order lines since {since}...")
    order_lines = get_order_lines(since=since)
    print(f"  Order lines: {len(order_lines)}")
    
    print("Loading variants...")
    variants = get_variants()
    print(f"  Variants: {len(variants)}")
    
    print("Loading listings...")
    listings = get_listings()
    listings = listings[listings['status'] == 'ACTIVE'].copy()
    print(f"  Active listings: {len(listings)}")
    
    return order_lines, variants, listings


def filter_full_price_variants(variants: pd.DataFrame) -> pd.DataFrame:
    """
    Filter to full-price variants only.
    Excludes items where compare_at_price > price (marked down).
    Keeps items where compare_at_price is null or equal to price.
    """
    full_price = variants[
        (variants['compare_at_price'].isna()) |
        (variants['compare_at_price'] <= variants['price'])
    ].copy()
    
    excluded = len(variants) - len(full_price)
    print(f"  Excluded {excluded} marked-down variants, {len(full_price)} remaining")
    
    return full_price


def aggregate_sales_to_products(
    order_lines: pd.DataFrame, 
    variants: pd.DataFrame
) -> pd.DataFrame:
    """
    Join order lines to variants, then aggregate to product level.
    Returns DataFrame with product_id, total_quantity, total_revenue.
    """
    # Filter to full-price variants only
    variants = filter_full_price_variants(variants)
    
    # Join order lines -> variants to get product_id
    sales = order_lines.merge(
        variants[['variant_id', 'product_id']], 
        on='variant_id', 
        how='inner'  # inner join excludes sales of marked-down variants
    )
    
    # Calculate line revenue
    sales['line_revenue'] = sales['quantity'] * sales['variant_price']
    
    # Aggregate to product level
    product_sales = sales.groupby('product_id').agg(
        total_quantity=('quantity', 'sum'),
        total_revenue=('line_revenue', 'sum'),
        order_count=('order_id', 'nunique')
    ).reset_index()
    
    return product_sales


def get_top_listings(
    product_sales: pd.DataFrame, 
    listings: pd.DataFrame, 
    top_n: int
) -> pd.DataFrame:
    """Join sales to listings and return top N by revenue."""
    top = product_sales.merge(
        listings[['product_id', 'handle', 'title', 'product_type', 'badge']],
        on='product_id',
        how='inner'
    )
    
    top = top.sort_values('total_revenue', ascending=False).head(top_n)
    top['total_revenue'] = top['total_revenue'].round(2)
    top = top.reset_index(drop=True)
    top.index = top.index + 1  # 1-indexed rank
    
    return top


def report_top_sellers(top_df: pd.DataFrame, lookback_days: int) -> None:
    """Print and save top sellers report."""
    print(f"\n--- Top {len(top_df)} Best Sellers by Revenue (Last {lookback_days} Days) ---")
    print(top_df[['handle', 'title', 'badge', 'total_revenue', 'total_quantity', 'order_count']].to_string())
    
    # Badge coverage summary
    print(f"\n--- Badge Distribution in Top {len(top_df)} ---")
    print(top_df['badge'].value_counts(dropna=False))
    
    # Save to CSV
    output_path = Path(__file__).parent / "top_sellers.csv"
    top_df.to_csv(output_path, index=True, index_label='rank')
    print(f"\nSaved to {output_path}")


def main():
    since = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime('%Y-%m-%d')
    print(f"Finding top {TOP_N} best sellers (by revenue, full-price only) from last {LOOKBACK_DAYS} days\n")
    
    order_lines, variants, listings = load_data(since)
    product_sales = aggregate_sales_to_products(order_lines, variants)
    top_df = get_top_listings(product_sales, listings, TOP_N)
    report_top_sellers(top_df, LOOKBACK_DAYS)


if __name__ == "__main__":
    main()