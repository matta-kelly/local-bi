import pandas as pd
from . import get_connection
from ..processing import process_table

def get_order_lines(since=None):
    conn = get_connection()
    
    query = "SELECT * FROM raw_shopify_order_lines"
    if since:
        query += f" WHERE created_at >= '{since}'"
    
    df = pd.read_sql(query, conn)
    conn.close()
    return process_table(df, "order_lines")