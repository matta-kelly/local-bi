import pandas as pd

INPUT_DIR = "projects/merchandising-analysis/data"
OUTPUT_DIR = "projects/merchandising-analysis/data"

def build_summary_stats():
    print("Loading listing_sales...", end=" ")
    listings = pd.read_parquet(f"{INPUT_DIR}/listing_sales.parquet")
    print(f"{len(listings)} rows")
    
    # 1. Revenue by Product Type (with category group)
    print("\nBuilding revenue by product type...")
    by_type = listings.groupby(["category_group", "product_type"]).agg(
        listing_count=("product_id", "count"),
        units_sold_30d=("units_sold_30d", "sum"),
        revenue_30d=("revenue_30d", "sum"),
    ).reset_index()
    by_type["avg_revenue_per_listing"] = by_type["revenue_30d"] / by_type["listing_count"]
    by_type = by_type.sort_values("revenue_30d", ascending=False)
    
    by_type.to_parquet(f"{OUTPUT_DIR}/summary_by_type.parquet", index=False)
    by_type.to_csv(f"{OUTPUT_DIR}/summary_by_type.csv", index=False)
    print(f"✓ Saved summary_by_type.parquet ({len(by_type)} rows)")
    print(by_type.head(10))
    
    # 2. Revenue by Markdown Bucket
    print("\nBuilding revenue by markdown bucket...")
    def markdown_bucket(pct):
        if pct == 0:
            return "0% (Full Price)"
        elif pct <= 0.20:
            return "1-20%"
        elif pct <= 0.50:
            return "21-50%"
        else:
            return "50%+"
    
    listings["markdown_bucket"] = listings["markdown_pct"].apply(markdown_bucket)
    
    by_markdown = listings.groupby("markdown_bucket").agg(
        listing_count=("product_id", "count"),
        units_sold_30d=("units_sold_30d", "sum"),
        revenue_30d=("revenue_30d", "sum"),
    ).reset_index()
    by_markdown["avg_revenue_per_listing"] = by_markdown["revenue_30d"] / by_markdown["listing_count"]
    
    # Sort by bucket order
    bucket_order = ["0% (Full Price)", "1-20%", "21-50%", "50%+"]
    by_markdown["sort_order"] = by_markdown["markdown_bucket"].apply(lambda x: bucket_order.index(x))
    by_markdown = by_markdown.sort_values("sort_order").drop(columns=["sort_order"])
    
    by_markdown.to_parquet(f"{OUTPUT_DIR}/summary_by_markdown.parquet", index=False)
    by_markdown.to_csv(f"{OUTPUT_DIR}/summary_by_markdown.csv", index=False)
    print(f"✓ Saved summary_by_markdown.parquet ({len(by_markdown)} rows)")
    print(by_markdown)
    
    print("\n✓ All summaries complete")


if __name__ == "__main__":
    build_summary_stats()