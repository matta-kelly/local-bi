import pandas as pd
import holidays
import os

INPUT_DIR = "klaviyo-analysis/input-data"
OUTPUT_DIR = "klaviyo-analysis/featured-data"

us_holidays = holidays.US(years=range(2020, 2026))

def get_day_type(dt):
    if dt.weekday() >= 5:
        return "holiday"
    if dt.date() in us_holidays:
        return "holiday"
    return "workday"

def feature_orders():
    print("Loading orders...", end=" ")
    orders = pd.read_parquet(f"{INPUT_DIR}/shopify_orders.parquet")
    print(f"{len(orders)} rows")
    
    print("Extracting hour...")
    orders["hour"] = orders["created_at"].dt.hour
    
    print("Calculating day_type...")
    orders["day_type"] = orders["created_at"].apply(get_day_type)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    orders.to_parquet(f"{OUTPUT_DIR}/f_orders.parquet", index=False)
    print(f"✓ Saved f_orders.parquet ({len(orders)} rows)")
    
    print(f"\nDay type distribution:")
    print(orders["day_type"].value_counts())
    print(f"\nHour distribution:")
    print(orders["hour"].value_counts().sort_index())

if __name__ == "__main__":
    feature_orders()