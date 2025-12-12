import pandas as pd

INPUT_DIR = "klaviyo-analysis/input-data"

def build_customers():
    """Join Shopify customers with Klaviyo profiles on email."""
    
    print("Loading shopify_customers...", end=" ")
    shopify = pd.read_parquet(f"{INPUT_DIR}/shopify_customers.parquet")
    print(f"{len(shopify)} rows")
    
    print("Loading profiles...", end=" ")
    profiles = pd.read_parquet(f"{INPUT_DIR}/klaviyo_profiles.parquet")
    print(f"{len(profiles)} rows")
    
    # Normalize email for join
    shopify["email_lower"] = shopify["email"].str.lower().str.strip()
    profiles["email_lower"] = profiles["email"].str.lower().str.strip()
    
    # Left join: keep all Shopify customers, add profile_id where matched
    print("Joining on email...", end=" ")
    customers = shopify.merge(
        profiles[["profile_id", "email_lower"]],
        on="email_lower",
        how="left"
    )
    customers = customers.drop(columns=["email_lower"])
    
    matched = customers["profile_id"].notna().sum()
    print(f"{matched}/{len(customers)} matched")
    
    # Save
    customers.to_parquet(f"{INPUT_DIR}/customers.parquet", index=False)
    print(f"✓ Saved customers.parquet ({len(customers)} rows)")


if __name__ == "__main__":
    build_customers()