import pandas as pd
import sys
from datetime import datetime
from jinja2 import Environment, FileSystemLoader

sys.path.insert(0, "projects/merchandising-analysis")
from config import COLLECTIONS

INPUT_DIR = "projects/merchandising-analysis/data"
OUTPUT_DIR = "projects/merchandising-analysis/data"
TEMPLATE_DIR = "projects/merchandising-analysis/templates"

CATEGORY_GROUPS = ["CLOTHING", "JEWELRY"]

TOP_N = 50
BURIED_N = 5


def load_data():
    """Load listing_sales and collection analysis files."""
    print("Loading listing_sales...", end=" ")
    listings = pd.read_parquet(f"{INPUT_DIR}/listing_sales.parquet")
    print(f"{len(listings)} rows")
    
    collections = []
    for name in COLLECTIONS:
        try:
            df = pd.read_parquet(f"{INPUT_DIR}/{name}_analysis.parquet")
            collections.append({"name": name, "df": df})
            print(f"Loaded {name}: {len(df)} rows")
        except FileNotFoundError:
            print(f"Skipping {name} - file not found")
    
    return listings, collections


def calc_category_summary(listings):
    """Calculate category group summary stats."""
    listings = listings[listings["category_group"].isin(CATEGORY_GROUPS)]
    total_revenue = listings["revenue_30d"].sum()
    
    by_group = listings.groupby("category_group").agg(
        revenue_30d=("revenue_30d", "sum"),
        units_sold_30d=("units_sold_30d", "sum"),
    ).reset_index()
    
    by_group["pct_revenue"] = by_group["revenue_30d"] / total_revenue * 100
    by_group = by_group.sort_values("revenue_30d", ascending=False)
    
    return by_group.to_dict("records")


def calc_type_breakdown(listings):
    """Calculate type breakdown within each category group."""
    filtered = listings[listings["category_group"].isin(CATEGORY_GROUPS)]
    total_revenue = filtered["revenue_30d"].sum()
    
    by_type = filtered.groupby(["category_group", "product_type"]).agg(
        revenue_30d=("revenue_30d", "sum"),
        units_sold_30d=("units_sold_30d", "sum"),
    ).reset_index()
    
    by_type["pct_of_total"] = by_type["revenue_30d"] / total_revenue * 100
    
    breakdown = {}
    for group in CATEGORY_GROUPS:
        group_data = by_type[by_type["category_group"] == group].copy()
        if len(group_data) == 0:
            continue
        group_total = group_data["revenue_30d"].sum()
        group_data["pct_of_group"] = group_data["revenue_30d"] / group_total * 100
        group_data = group_data.sort_values("revenue_30d", ascending=False)
        breakdown[group] = group_data.to_dict("records")
    
    return breakdown


def rank_to_color(rank, invert=False):
    """
    Convert 0-1 rank to color. 
    Only color top 30% (green) and bottom 30% (red).
    Middle 40% is transparent.
    
    If invert=True, low rank = green (for metrics where lower is better).
    """
    if pd.isna(rank):
        return "transparent"
    
    if invert:
        rank = 1 - rank
    
    if rank <= 0.30:
        intensity = (0.30 - rank) / 0.30
        return f"rgba(255, 107, 107, {0.3 + intensity * 0.5})"
    elif rank >= 0.70:
        intensity = (rank - 0.70) / 0.30
        return f"rgba(46, 204, 113, {0.3 + intensity * 0.5})"
    else:
        return "transparent"


def calc_percentile_ranks(df):
    """Calculate percentile ranks and colors for conditional formatting."""
    df = df.copy()
    
    # Revenue rank (higher = better)
    df["revenue_rank"] = df["revenue_30d"].rank(pct=True, na_option="bottom").fillna(0)
    df["revenue_color"] = df["revenue_rank"].apply(lambda x: rank_to_color(x))
    
    # ROS rank (higher = better)
    df["ros_rank"] = df["rate_of_sale_30d"].rank(pct=True, na_option="bottom").fillna(0)
    df["ros_color"] = df["ros_rank"].apply(lambda x: rank_to_color(x))
    
    # Velocity rank (higher = better, heating up)
    df["velocity_rank"] = df["velocity_trend"].rank(pct=True, na_option="bottom").fillna(0)
    df["velocity_color"] = df["velocity_rank"].apply(lambda x: rank_to_color(x))
    
    # Markdown rank (lower = better, so invert)
    df["markdown_rank"] = df["markdown_pct"].rank(pct=True, na_option="bottom").fillna(0)
    df["markdown_color"] = df["markdown_rank"].apply(lambda x: rank_to_color(x, invert=True))
    
    # Margin rank (higher = better, handle zeros as N/A)
    margin_valid = df["margin_pct"] > 0
    df["margin_rank"] = 0.5
    if margin_valid.any():
        df.loc[margin_valid, "margin_rank"] = df.loc[margin_valid, "margin_pct"].rank(pct=True).fillna(0.5)
    df["margin_color"] = df["margin_rank"].apply(lambda x: rank_to_color(x))
    
    return df


def prep_collections(collections):
    """Prepare collection data for template with percentile ranks."""
    prepped = []
    for coll in collections:
        df = coll["df"].copy()
        total_original = len(df)
        
        # Sort by position
        df = df.sort_values("position")
        
        # Split into top 50 and rest
        top_df = df.head(TOP_N).copy()
        rest_df = df.iloc[TOP_N:].copy() if len(df) > TOP_N else pd.DataFrame()
        
        # Get top 5 by revenue from positions 51+
        buried_df = pd.DataFrame()
        if len(rest_df) > 0:
            buried_df = rest_df.nlargest(BURIED_N, "revenue_30d").copy()
        
        # Calculate percentiles on top 50 only
        top_df = calc_percentile_ranks(top_df)
        
        # For buried items, calculate their colors relative to top 50
        if len(buried_df) > 0:
            # Get percentile thresholds from top 50
            for col, color_col, invert in [
                ("revenue_30d", "revenue_color", False),
                ("rate_of_sale_30d", "ros_color", False),
                ("velocity_trend", "velocity_color", False),
                ("markdown_pct", "markdown_color", True),
                ("margin_pct", "margin_color", False),
            ]:
                top_values = top_df[col].dropna()
                if len(top_values) > 0:
                    p30 = top_values.quantile(0.30)
                    p70 = top_values.quantile(0.70)
                    
                    def calc_relative_rank(val):
                        if pd.isna(val):
                            return 0.5
                        if val <= p30:
                            return 0.15 * (val / p30) if p30 > 0 else 0
                        elif val >= p70:
                            return 0.70 + 0.30 * min((val - p70) / (p70 * 0.5), 1) if p70 > 0 else 1
                        else:
                            return 0.30 + 0.40 * (val - p30) / (p70 - p30) if p70 > p30 else 0.5
                    
                    buried_df[col.replace("_30d", "_rank").replace("_pct", "_rank").replace("_trend", "_rank")] = buried_df[col].apply(calc_relative_rank)
                    buried_df[color_col] = buried_df[col].apply(lambda x: rank_to_color(calc_relative_rank(x), invert=invert))
                else:
                    buried_df[color_col] = "transparent"
        
        matched = top_df["rate_of_sale_30d"].notna().sum()
        if len(buried_df) > 0:
            matched += buried_df["rate_of_sale_30d"].notna().sum()
        
        prepped.append({
            "name": coll["name"].replace("-", " ").title(),
            "matched": matched,
            "total": total_original,
            "total_shown": len(top_df) + len(buried_df),
            "match_rate": matched / total_original * 100 if total_original > 0 else 0,
            "top_data": top_df.to_dict("records"),
            "buried_data": buried_df.to_dict("records") if len(buried_df) > 0 else [],
            "has_buried": len(buried_df) > 0,
            "buried_count": len(rest_df),
        })
    
    return prepped


def render_report(category_summary, type_breakdown, collections):
    """Render HTML report using Jinja2 template."""
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))
    template = env.get_template("report.html")
    
    html = template.render(
        report_date=datetime.now().strftime("%Y-%m-%d"),
        report_datetime=datetime.now().strftime("%Y-%m-%d %H:%M"),
        category_summary=category_summary,
        type_breakdown=type_breakdown,
        collections=collections,
    )
    
    return html


def build_report():
    """Main function to build the report."""
    listings, collections = load_data()
    
    print("\nCalculating summaries...")
    category_summary = calc_category_summary(listings)
    type_breakdown = calc_type_breakdown(listings)
    collections_prepped = prep_collections(collections)
    
    print("Rendering report...")
    html = render_report(category_summary, type_breakdown, collections_prepped)
    
    output_path = f"{OUTPUT_DIR}/merchandising_report.html"
    with open(output_path, "w") as f:
        f.write(html)
    
    print(f"\n✓ Saved {output_path}")


if __name__ == "__main__":
    build_report()