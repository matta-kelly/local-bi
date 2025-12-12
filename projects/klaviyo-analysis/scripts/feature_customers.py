import pandas as pd
import numpy as np

INPUT_DIR = "klaviyo-analysis/input-data"
OUTPUT_DIR = "klaviyo-analysis/featured-data"

STATE_TO_TZ_COHORT = {
    # Eastern
    "CT": "eastern", "DE": "eastern", "FL": "eastern", "GA": "eastern",
    "IN": "eastern", "KY": "eastern", "ME": "eastern", "MD": "eastern",
    "MA": "eastern", "MI": "eastern", "NH": "eastern", "NJ": "eastern",
    "NY": "eastern", "NC": "eastern", "OH": "eastern", "PA": "eastern",
    "RI": "eastern", "SC": "eastern", "VT": "eastern", "VA": "eastern",
    "WV": "eastern", "DC": "eastern",
    # Central
    "AL": "central", "AR": "central", "IL": "central", "IA": "central",
    "KS": "central", "LA": "central", "MN": "central", "MS": "central",
    "MO": "central", "NE": "central", "ND": "central", "OK": "central",
    "SD": "central", "TN": "central", "TX": "central", "WI": "central",
    # Mountain
    "AZ": "mountain", "CO": "mountain", "ID": "mountain", "MT": "mountain",
    "NM": "mountain", "UT": "mountain", "WY": "mountain",
    # Pacific
    "CA": "pacific", "NV": "pacific", "OR": "pacific", "WA": "pacific",
    "AK": "pacific", "HI": "pacific",
}


def get_tz_cohort(province):
    """Map province/state to timezone cohort."""
    if pd.isna(province):
        return None
    province = str(province).strip().upper()
    if len(province) == 2:
        return STATE_TO_TZ_COHORT.get(province)
    return None


def feature_customers():
    print("Loading customers...", end=" ")
    customers = pd.read_parquet(f"{INPUT_DIR}/customers.parquet")
    print(f"{len(customers)} rows")
    
    print("Loading f_orders...", end=" ")
    orders = pd.read_parquet(f"{OUTPUT_DIR}/f_orders.parquet")
    print(f"{len(orders)} rows")
    
    print("Mapping timezone cohorts...")
    customers["tz_cohort"] = customers["default_address_province"].apply(get_tz_cohort)
    
    print("Calculating median hours per customer...")
    customer_hours = orders.groupby(["customer_id", "day_type"])["hour"].median().unstack()
    
    # Handle case where not all day_types exist
    if "holiday" not in customer_hours.columns:
        customer_hours["holiday"] = None
    if "workday" not in customer_hours.columns:
        customer_hours["workday"] = None
    
    customer_hours = customer_hours.rename(columns={
        "holiday": "holiday_send_hour",
        "workday": "workday_send_hour"
    })
    customer_hours = customer_hours.reset_index()
    
    customers = customers.merge(customer_hours, on="customer_id", how="left")
    
    # Pass 1: Fill missing with other day_type
    print("Pass 1: Filling from other day_type...")
    mask_holiday_null = customers["holiday_send_hour"].isna()
    mask_workday_null = customers["workday_send_hour"].isna()
    
    customers.loc[mask_holiday_null, "holiday_send_hour"] = customers.loc[mask_holiday_null, "workday_send_hour"]
    customers.loc[mask_workday_null, "workday_send_hour"] = customers.loc[mask_workday_null, "holiday_send_hour"]
    
    # Pass 2: Fill remaining nulls with timezone cohort median
    print("Pass 2: Filling from timezone cohort defaults...")
    
    cohort_defaults = customers.groupby("tz_cohort").agg({
        "holiday_send_hour": "median",
        "workday_send_hour": "median"
    }).rename(columns={
        "holiday_send_hour": "cohort_holiday_default",
        "workday_send_hour": "cohort_workday_default"
    })
    
    customers = customers.merge(cohort_defaults, on="tz_cohort", how="left")
    
    mask_holiday_null = customers["holiday_send_hour"].isna()
    mask_workday_null = customers["workday_send_hour"].isna()
    
    customers.loc[mask_holiday_null, "holiday_send_hour"] = customers.loc[mask_holiday_null, "cohort_holiday_default"]
    customers.loc[mask_workday_null, "workday_send_hour"] = customers.loc[mask_workday_null, "cohort_workday_default"]
    
    # Pass 3: Global fallback
    print("Pass 3: Global fallback...")
    global_holiday = customers["holiday_send_hour"].median()
    global_workday = customers["workday_send_hour"].median()
    
    customers["holiday_send_hour"] = customers["holiday_send_hour"].fillna(global_holiday)
    customers["workday_send_hour"] = customers["workday_send_hour"].fillna(global_workday)
    
    customers = customers.drop(columns=["cohort_holiday_default", "cohort_workday_default"])
    
    customers["holiday_send_hour"] = customers["holiday_send_hour"].round().astype(int)
    customers["workday_send_hour"] = customers["workday_send_hour"].round().astype(int)
    
    customers.to_parquet(f"{OUTPUT_DIR}/f_customers.parquet", index=False)
    print(f"✓ Saved f_customers.parquet ({len(customers)} rows)")
    
    print(f"\nTimezone cohort distribution:")
    print(customers["tz_cohort"].value_counts(dropna=False))
    print(f"\nHoliday send hour distribution:")
    print(customers["holiday_send_hour"].value_counts().sort_index())
    print(f"\nWorkday send hour distribution:")
    print(customers["workday_send_hour"].value_counts().sort_index())


if __name__ == "__main__":
    feature_customers()