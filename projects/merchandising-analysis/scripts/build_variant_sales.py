import pandas as pd
from datetime import datetime, timedelta

INPUT_DIR = "projects/merchandising-analysis/data"
OUTPUT_DIR = "projects/merchandising-analysis/data"

def build_variant_sales():
    print("Loading order_lines...", end=" ")
    order_lines = pd.read_parquet(f"{INPUT_DIR}/shopify_order_lines.parquet")
    print(f"{len(order_lines)} rows")
    
    print("Loading orders...", end=" ")
    orders_df = pd.read_parquet(f"{INPUT_DIR}/shopify_orders.parquet")
    print(f"{len(orders_df)} rows")
    
    print("Loading variants...", end=" ")
    variants = pd.read_parquet(f"{INPUT_DIR}/shopify_variants.parquet")
    print(f"{len(variants)} rows")
    
    print("Loading master_sku...", end=" ")
    master_sku = pd.read_parquet(f"{INPUT_DIR}/master_sku.parquet")
    print(f"{len(master_sku)} rows")
    
    print("Loading stock_daily...", end=" ")
    stock_daily = pd.read_parquet(f"{INPUT_DIR}/stock_daily.parquet")
    print(f"{len(stock_daily)} rows")
    
    # Join order date to order_lines
    print("Joining order dates...")
    order_lines = order_lines.merge(
        orders_df[["order_id", "created_at"]],
        on="order_id",
        how="left"
    )
    
    # Date windows
    today = datetime.now()
    windows = {
        "30d": (today - timedelta(days=30), 30),
        "14d": (today - timedelta(days=14), 14),
        "7d": (today - timedelta(days=7), 7),
    }
    
    # Start with variant base info
    print("Building variant base...")
    variant_sales = variants[["variant_id", "product_id", "sku", "price", "compare_at_price"]].copy()
    
    # Join master_sku to get EC price, unit_cost, and category_group
    print("Joining master_sku for EC price, unit_cost, and category_group...")
    variant_sales = variant_sales.merge(
        master_sku[["sku", "ec", "unit_cost", "category_group"]],
        on="sku",
        how="left"
    )
    
    # Fill compare_at_price: use ec if missing, then fall back to price
    variant_sales["compare_at_price"] = (
        variant_sales["compare_at_price"]
        .fillna(variant_sales["ec"])
        .fillna(variant_sales["price"])
    )
    variant_sales = variant_sales.drop(columns=["ec"])
    
    # Calculate markdown %
    variant_sales["markdown_pct"] = (
        (variant_sales["compare_at_price"] - variant_sales["price"]) / variant_sales["compare_at_price"]
    ).fillna(0).clip(lower=0)
    
    # Calculate margin %
    variant_sales["margin_pct"] = (
        (variant_sales["price"] - variant_sales["unit_cost"]) / variant_sales["price"]
    ).fillna(0).clip(lower=0)
    
    # Get current qty (latest snapshot)
    print("Getting current stock levels...")
    latest_date = stock_daily["snapshot_date"].max()
    current_stock = stock_daily[stock_daily["snapshot_date"] == latest_date][["internal_reference", "free_qty"]].copy()
    current_stock = current_stock.rename(columns={"internal_reference": "sku", "free_qty": "current_qty"})
    variant_sales = variant_sales.merge(current_stock, on="sku", how="left")
    variant_sales["current_qty"] = variant_sales["current_qty"].fillna(0)
    
    # Aggregate sales and stock for each time window
    for window_name, (cutoff_date, num_days) in windows.items():
        print(f"Aggregating {window_name} sales...")
        
        # Filter order_lines to window
        mask = order_lines["created_at"] >= cutoff_date
        window_lines = order_lines[mask]
        
        # Aggregate sales
        window_agg = window_lines.groupby("variant_id").agg(
            units_sold=("quantity", "sum"),
            revenue=("variant_price", lambda x: (x * window_lines.loc[x.index, "quantity"]).sum()),
            order_count=("order_id", "nunique"),
        ).reset_index()
        
        # Calculate AUR
        window_agg["aur"] = window_agg["revenue"] / window_agg["units_sold"]
        
        # Rename columns with window suffix
        window_agg = window_agg.rename(columns={
            "units_sold": f"units_sold_{window_name}",
            "revenue": f"revenue_{window_name}",
            "order_count": f"order_count_{window_name}",
            "aur": f"aur_{window_name}",
        })
        
        # Merge sales to base
        variant_sales = variant_sales.merge(window_agg, on="variant_id", how="left")
        
        # Aggregate stock for window
        print(f"Aggregating {window_name} stock...")
        stock_mask = stock_daily["snapshot_date"] >= cutoff_date
        window_stock = stock_daily[stock_mask]
        
        stock_agg = window_stock.groupby("internal_reference").agg(
            days_in_stock=("in_stock", lambda x: (x == True).sum() if x.dtype == bool else (x.str.lower() == "true").sum()),
            avg_free_qty=("free_qty", "mean"),
        ).reset_index().rename(columns={"internal_reference": "sku"})
        
        stock_agg = stock_agg.rename(columns={
            "days_in_stock": f"days_in_stock_{window_name}",
            "avg_free_qty": f"avg_free_qty_{window_name}",
        })
        
        # Merge stock to base
        variant_sales = variant_sales.merge(stock_agg, on="sku", how="left")
        
        # Calculate rate of sale (units per day in stock)
        variant_sales[f"rate_of_sale_{window_name}"] = (
            variant_sales[f"units_sold_{window_name}"] / variant_sales[f"days_in_stock_{window_name}"]
        ).replace([float("inf"), -float("inf")], 0).fillna(0)
    
    # Calculate weeks of sale (using 30d ROS)
    print("Calculating weeks of sale...")
    weekly_burn = variant_sales["rate_of_sale_30d"] * 7
    variant_sales["weeks_of_sale"] = (
        variant_sales["current_qty"] / weekly_burn
    ).replace([float("inf"), -float("inf")], 9999).fillna(0)
    
    # Fill NaN sales metrics with 0
    sales_cols = [c for c in variant_sales.columns if any(x in c for x in ["units_sold", "revenue", "order_count", "days_in_stock", "avg_free_qty"])]
    variant_sales[sales_cols] = variant_sales[sales_cols].fillna(0)
    
    # Save
    variant_sales.to_parquet(f"{OUTPUT_DIR}/variant_sales.parquet", index=False)
    print(f"✓ Saved variant_sales.parquet ({len(variant_sales)} rows)")
    
    print(f"\nColumns: {variant_sales.columns.tolist()}")
    print(f"\nPreview:")
    print(variant_sales.head(10))


if __name__ == "__main__":
    build_variant_sales()