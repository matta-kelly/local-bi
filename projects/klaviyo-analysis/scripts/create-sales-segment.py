import pandas as pd
import os

INPUT_DIR = "klaviyo-analysis/input-data"
OUTPUT_DIR = "klaviyo-analysis/featured-data"

# BFCM 2024 window
BFCM_START = "2024-11-24"
BFCM_END = "2024-12-02"

def build_bfcm_only_segment():
    orders = pd.read_parquet(f"{INPUT_DIR}/shopify_orders.parquet")
    customers = pd.read_parquet(f"{INPUT_DIR}/customers.parquet")
    
    # Filter to customers with Klaviyo profile (consented to marketing)
    consented_customers = customers[customers["profile_id"].notna()]["customer_id"]
    orders = orders[orders["customer_id"].isin(consented_customers)]
    
    # Get first order date per customer
    first_orders = orders.groupby("customer_id")["created_at"].min().reset_index()
    first_orders.columns = ["customer_id", "first_order_date"]
    
    # Customers whose first order was during BFCM
    first_orders["first_in_bfcm"] = (
        (first_orders["first_order_date"] >= BFCM_START) & 
        (first_orders["first_order_date"] <= BFCM_END)
    )
    
    # Check if customer has any orders outside BFCM
    orders["in_bfcm"] = (
        (orders["created_at"] >= BFCM_START) & 
        (orders["created_at"] <= BFCM_END)
    )
    orders_outside = orders.groupby("customer_id")["in_bfcm"].all().reset_index()
    orders_outside.columns = ["customer_id", "all_orders_in_bfcm"]
    
    # Merge and filter
    segment = first_orders.merge(orders_outside, on="customer_id")
    segment = segment[segment["first_in_bfcm"] & segment["all_orders_in_bfcm"]]
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    result = segment[["customer_id", "first_order_date"]]
    result.to_parquet(f"{OUTPUT_DIR}/segment_bfcm_only.parquet", index=False)
    
    print(f"✓ BFCM-only segment: {len(result)} customers")

if __name__ == "__main__":
    build_bfcm_only_segment()