import pandas as pd
from . import get_connection
from ..processing import process_table

def get_listings():
    conn = get_connection()
    
    query = "SELECT * FROM raw_shopify_products"
    
    df = pd.read_sql(query, conn)
    conn.close()
    return process_table(df, "shopify_listings")