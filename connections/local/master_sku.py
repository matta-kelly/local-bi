import pandas as pd
from pathlib import Path
from ..processing import process_table

def get_master_sku():
    """Load master SKU from local CSV."""
    filepath = Path(__file__).parent / "master-sku.csv"
    df = pd.read_csv(filepath, encoding="latin-1")
    return process_table(df, "master_sku")