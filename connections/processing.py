import pandas as pd
from .schemas import SCHEMAS

def normalize_headers(df):
    """Lowercase, replace spaces/special chars with underscores, strip whitespace."""
    import re
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace(".", "")
        .str.replace("/", "_")
        .str.replace("$", "")
        .str.replace(",", "")
        .str.replace("?", "")
        .str.replace("#", "")
        .str.replace("\n", "_")
    )
    # Collapse multiple underscores
    df.columns = df.columns.str.replace(r'_+', '_', regex=True)
    # Strip trailing underscores
    df.columns = df.columns.str.strip('_')
    return df

def to_string(val):
    if pd.isna(val) or val in ("", "NULL", "N/A", "None"):
        return None
    return str(val).strip()

def to_int(val):
    if pd.isna(val) or val in ("", "NULL", "N/A", "None"):
        return None
    try:
        return int(float(str(val).replace(",", "").replace("%", "")))
    except (ValueError, TypeError):
        return None

def to_float(val):
    if pd.isna(val) or val in ("", "NULL", "N/A", "None"):
        return None
    try:
        return float(str(val).replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        return None

def to_percent(val):
    if pd.isna(val) or val in ("", "NULL", "N/A", "None"):
        return None
    try:
        s = str(val).replace("%", "").strip()
        return float(s) / 100
    except (ValueError, TypeError):
        return None

def to_datetime(val):
    if pd.isna(val) or val in ("", "NULL", "N/A", "None"):
        return None
    try:
        return pd.to_datetime(val)
    except:
        return None

TYPE_FUNCS = {
    "string": to_string,
    "int": to_int,
    "float": to_float,
    "percent": to_percent,
    "datetime": to_datetime,
}

def process_table(df, table_name):
    """Apply schema transformations to dataframe."""
    df = normalize_headers(df)
    
    schema = SCHEMAS[table_name]
    
    cols_to_keep = [c for c in schema.keys() if c in df.columns]
    df = df[cols_to_keep].copy()
    
    for col, type_name in schema.items():
        if col in df.columns:
            func = TYPE_FUNCS[type_name]
            df[col] = df[col].apply(func)
    
    return df