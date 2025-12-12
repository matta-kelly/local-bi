import pandas as pd
from . import get_connection
from ..processing import process_table

def get_customers(since=None):
    conn = get_connection()
    
    query = "SELECT * FROM raw_shopify_customers"
    if since:
        query += f" WHERE updated_at >= '{since}'"
    
    df = pd.read_sql(query, conn)
    conn.close()
    return process_table(df, "shopify_customers")