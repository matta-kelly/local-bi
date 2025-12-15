import pandas as pd
import sys
sys.path.insert(0, "projects/merchandising-analysis")
from config import COLLECTIONS, LOW_STOCK_WEEKS

INPUT_DIR = "projects/merchandising-analysis/data"
OUTPUT_DIR = "projects/merchandising-analysis/data"

def build_collection_analysis():
    print("Loading listing_sales...", end=" ")
    listings = pd.read_parquet(f"{INPUT_DIR}/listing_sales.parquet")
    print(f"{len(listings)} rows")
    
    for collection in COLLECTIONS:
        print(f"\nProcessing {collection}...")
        
        # Load collection
        collection_df = pd.read_parquet(f"{INPUT_DIR}/{collection}_products.parquet")
        print(f"  Loaded {len(collection_df)} products")
        
        # Keep only position and name
        collection_df = collection_df[["position", "name"]]
        
        # Join to listing_sales on name = title
        merged = collection_df.merge(
            listings[["title", "units_sold_30d", "revenue_30d", "rate_of_sale_30d", "rate_of_sale_7d", "markdown_pct", "margin_pct", "weeks_of_sale", "published_at"]],
            left_on="name",
            right_on="title",
            how="left"
        )
        
        # Calculate velocity trend
        merged["velocity_trend"] = (
            merged["rate_of_sale_7d"] / merged["rate_of_sale_30d"]
        ).replace([float("inf"), -float("inf")], 0).fillna(0)
        
        # Low stock flag
        merged["low_stock"] = (merged["weeks_of_sale"] < LOW_STOCK_WEEKS).astype(int)
        
        # Select final columns
        output = merged[[
            "position",
            "name",
            "published_at",
            "units_sold_30d",
            "revenue_30d",
            "rate_of_sale_30d",
            "velocity_trend",
            "markdown_pct",
            "margin_pct",
            "weeks_of_sale",
            "low_stock"
        ]]
        
        # Save
        output.to_parquet(f"{OUTPUT_DIR}/{collection}_analysis.parquet", index=False)
        #output.to_csv(f"{OUTPUT_DIR}/{collection}_analysis.csv", index=False)
        print(f"  ✓ Saved {collection}_analysis.parquet ({len(output)} rows)")
        
        # Show match rate
        matched = output["rate_of_sale_30d"].notna().sum()
        print(f"  Matched: {matched}/{len(output)} ({matched/len(output)*100:.1f}%)")
    
    print("\n✓ All collections processed")


if __name__ == "__main__":
    build_collection_analysis()