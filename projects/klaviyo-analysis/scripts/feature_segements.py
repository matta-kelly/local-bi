import pandas as pd
import numpy as np

INPUT_DIR = "klaviyo-analysis/input-data"
OUTPUT_DIR = "klaviyo-analysis/featured-data"


def bucket_hour(hour):
    """Bucket hour into time of day."""
    if 6 <= hour < 12:
        return "morning"
    elif 12 <= hour < 18:
        return "afternoon"
    elif 18 <= hour < 24:
        return "evening"
    else:
        return "night"


def feature_segments():
    print("Loading data...")
    segments = pd.read_parquet(f"{INPUT_DIR}/klaviyo_segments.parquet")
    segment_membership = pd.read_parquet(f"{INPUT_DIR}/klaviyo_segment_membership.parquet")
    campaigns = pd.read_parquet(f"{INPUT_DIR}/klaviyo_campaigns.parquet")
    customers = pd.read_parquet(f"{INPUT_DIR}/customers.parquet")
    orders = pd.read_parquet(f"{INPUT_DIR}/shopify_orders.parquet")
    f_customers = pd.read_parquet(f"{OUTPUT_DIR}/f_customers.parquet")
    
    print(f"  segments: {len(segments)}")
    print(f"  segment_membership: {len(segment_membership)}")
    print(f"  campaigns: {len(campaigns)}")
    print(f"  customers: {len(customers)}")
    print(f"  orders: {len(orders)}")
    print(f"  f_customers: {len(f_customers)}")
    
    # --- Campaign Stats ---
    print("\nCalculating campaign stats...")
    campaign_stats = campaigns.groupby("list").agg(
        total_campaigns=("campaign_id", "count"),
        total_sends=("total_recipients", "sum"),
        avg_open_rate=("open_rate", "mean"),
        avg_click_rate=("click_rate", "mean"),
        total_campaign_revenue=("revenue", "sum"),
        total_unsubscribes=("unsubscribes", "sum"),
        avg_spam_rate=("spam_complaints_rate", "mean"),
        avg_bounce_rate=("bounce_rate", "mean"),
    ).reset_index()
    campaign_stats = campaign_stats.rename(columns={"list": "segment_name"})
    
    # --- Send Time Distribution ---
    print("Calculating send time distribution...")
    
    member_times = segment_membership.merge(
        f_customers[["profile_id", "holiday_send_hour", "workday_send_hour"]],
        on="profile_id",
        how="left"
    )
    
    member_times["holiday_bucket"] = member_times["holiday_send_hour"].apply(
        lambda x: bucket_hour(x) if pd.notna(x) else None
    )
    member_times["workday_bucket"] = member_times["workday_send_hour"].apply(
        lambda x: bucket_hour(x) if pd.notna(x) else None
    )
    
    def calc_distribution(df, col, prefix):
        counts = df.groupby(["segment_id", col]).size().unstack(fill_value=0)
        totals = counts.sum(axis=1)
        pcts = counts.div(totals, axis=0)
        pcts.columns = [f"{prefix}_pct_{c}" for c in pcts.columns]
        return pcts.reset_index()
    
    holiday_dist = calc_distribution(member_times, "holiday_bucket", "holiday")
    workday_dist = calc_distribution(member_times, "workday_bucket", "workday")
    
    send_time_stats = holiday_dist.merge(workday_dist, on="segment_id", how="outer")
    
    # --- Customer Purchase Stats ---
    print("Calculating customer purchase stats...")
    
    member_customers = segment_membership.merge(
        customers[["profile_id", "customer_id"]],
        on="profile_id",
        how="left"
    )
    
    member_orders = member_customers.merge(
        orders[["order_id", "customer_id", "current_total_price"]],
        on="customer_id",
        how="left"
    )
    
    purchase_stats = member_orders.groupby("segment_id").agg(
        total_customers=("profile_id", "nunique"),
        total_orders=("order_id", "nunique"),
        total_order_revenue=("current_total_price", "sum"),
    ).reset_index()
    
    purchase_stats["avg_order_value"] = (
        purchase_stats["total_order_revenue"] / purchase_stats["total_orders"]
    ).replace([np.inf, -np.inf], 0).fillna(0)
    
    # --- Merge Everything ---
    print("Merging all stats...")
    
    f_segments = segments.copy()
    f_segments = f_segments.merge(campaign_stats, on="segment_name", how="left")
    f_segments = f_segments.merge(send_time_stats, on="segment_id", how="left")
    f_segments = f_segments.merge(purchase_stats, on="segment_id", how="left")
    
    pct_cols = [c for c in f_segments.columns if "_pct_" in c]
    f_segments[pct_cols] = f_segments[pct_cols].fillna(0)
    
    count_cols = ["total_campaigns", "total_sends", "total_unsubscribes", 
                  "total_customers", "total_orders"]
    for col in count_cols:
        if col in f_segments.columns:
            f_segments[col] = f_segments[col].fillna(0).astype(int)
    
    f_segments.to_parquet(f"{OUTPUT_DIR}/f_segments.parquet", index=False)
    print(f"\n✓ Saved f_segments.parquet ({len(f_segments)} rows)")
    
    print("\nPreview:")
    print(f_segments.head())
    print("\nColumns:")
    print(f_segments.columns.tolist())


if __name__ == "__main__":
    feature_segments()