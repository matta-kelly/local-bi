import pandas as pd

INPUT_DIR = "projects/merchandising-analysis/data"
OUTPUT_DIR = "projects/merchandising-analysis/data"

def build_variant_sales():
    print("Loading order_lines...", end=" ")
    order_lines = pd.read_parquet(f"{INPUT_DIR}/shopify_order_lines.parquet")
    print(f"{len(order_lines)} rows")
    
    print("Loading variants...", end=" ")
    variants = pd.read_parquet(f"{INPUT_DIR}/shopify_variants.parquet")
    print(f"{len(variants)} rows")
    
    print("Loading master_sku...", end=" ")
    master_sku = pd.read_parquet(f"{INPUT_DIR}/master_sku.parquet")
    print(f"{len(master_sku)} rows")
    
    # Aggregate order_lines to variant level
    print("Aggregating sales by variant...")
    variant_sales = order_lines.groupby("variant_id").agg(
        units_sold=("quantity", "sum"),
        revenue=("variant_price", lambda x: (x * order_lines.loc[x.index, "quantity"]).sum()),
        order_count=("order_id", "nunique"),
    ).reset_index()
    
    # Join variant info
    print("Joining variant details...")
    variant_sales = variant_sales.merge(
        variants[["variant_id", "product_id", "sku", "price", "compare_at_price"]],
        on="variant_id",
        how="left"
    )
    
    # Join master_sku to get EC price for missing compare_at_price
    print("Joining master_sku for EC price...")
    variant_sales = variant_sales.merge(
        master_sku[["sku", "ec"]],
        on="sku",
        how="left"
    )
    
    # Fill compare_at_price: use ec if missing, then fall back to price
    variant_sales["compare_at_price"] = (
        variant_sales["compare_at_price"]
        .fillna(variant_sales["ec"])
        .fillna(variant_sales["price"])
    )
    
    # Drop ec helper column
    variant_sales = variant_sales.drop(columns=["ec"])
    
    # Calculate markdown %
    variant_sales["markdown_pct"] = (
        (variant_sales["compare_at_price"] - variant_sales["price"]) / variant_sales["compare_at_price"]
    ).fillna(0).clip(lower=0)
    
    # Save
    variant_sales.to_parquet(f"{OUTPUT_DIR}/variant_sales.parquet", index=False)
    print(f"✓ Saved variant_sales.parquet ({len(variant_sales)} rows)")
    
    print(f"\nPreview:")
    print(variant_sales.head(10))


if __name__ == "__main__":
    build_variant_sales()