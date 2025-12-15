import pandas as pd

INPUT_DIR = "projects/merchandising-analysis/data"
OUTPUT_DIR = "projects/merchandising-analysis/data"

# Filters
MIN_CURRENT_QTY = 5

def build_listing_sales():
    print("Loading variant_sales...", end=" ")
    variants = pd.read_parquet(f"{INPUT_DIR}/variant_sales.parquet")
    print(f"{len(variants)} rows")
    
    print("Loading shopify_listings...", end=" ")
    listings = pd.read_parquet(f"{INPUT_DIR}/shopify_listings.parquet")
    print(f"{len(listings)} rows")
    
    windows = ["30d", "14d", "7d"]
    
    # Build aggregation dict
    agg_dict = {
        "sku": "count",  # variant_count
        "price": "mean",
        "compare_at_price": "mean",
        "markdown_pct": "mean",
        "current_qty": "sum",
    }
    
    # Add window-specific columns
    for w in windows:
        agg_dict[f"units_sold_{w}"] = "sum"
        agg_dict[f"revenue_{w}"] = "sum"
        agg_dict[f"order_count_{w}"] = "sum"
        agg_dict[f"days_in_stock_{w}"] = "sum"
    
    print("Aggregating to listing level...")
    listing_sales = variants.groupby("product_id").agg(agg_dict).reset_index()
    
    # Rename sku count
    listing_sales = listing_sales.rename(columns={"sku": "variant_count"})
    
    # Recalculate ROS and AUR per window
    for w in windows:
        # ROS = sum(units) / sum(days_in_stock)
        listing_sales[f"rate_of_sale_{w}"] = (
            listing_sales[f"units_sold_{w}"] / listing_sales[f"days_in_stock_{w}"]
        ).replace([float("inf"), -float("inf")], 0).fillna(0)
        
        # AUR = revenue / units
        listing_sales[f"aur_{w}"] = (
            listing_sales[f"revenue_{w}"] / listing_sales[f"units_sold_{w}"]
        ).replace([float("inf"), -float("inf")], 0).fillna(0)
    
    # Weeks of sale using 30d ROS
    print("Calculating weeks of sale...")
    weekly_burn = listing_sales["rate_of_sale_30d"] * 7
    listing_sales["weeks_of_sale"] = (
        listing_sales["current_qty"] / weekly_burn
    ).replace([float("inf"), -float("inf")], 9999).fillna(0)
    
    # Filter low inventory
    print(f"Filtering low inventory (current_qty < {MIN_CURRENT_QTY})...")
    before_count = len(listing_sales)
    listing_sales = listing_sales[listing_sales["current_qty"] >= MIN_CURRENT_QTY]
    print(f"✓ Dropped {before_count - len(listing_sales)} listings")
    
    # Join listing details
    print("Joining listing details...")
    listing_sales = listing_sales.merge(
        listings[["product_id", "title", "product_type", "published_at"]],
        on="product_id",
        how="left"
    )
    
    # Reorder columns
    id_cols = ["product_id", "title", "product_type", "variant_count"]
    other_cols = [c for c in listing_sales.columns if c not in id_cols]
    listing_sales = listing_sales[id_cols + other_cols]
    
    # Save
    listing_sales.to_parquet(f"{OUTPUT_DIR}/listing_sales.parquet", index=False)
    listing_sales.to_csv(f"{OUTPUT_DIR}/listing_sales.csv", index=False)
    print(f"✓ Saved listing_sales.parquet and listing_sales.csv ({len(listing_sales)} rows)")
    
    print(f"\nColumns: {listing_sales.columns.tolist()}")
    print(f"\nPreview:")
    print(listing_sales.head(10))


if __name__ == "__main__":
    build_listing_sales()