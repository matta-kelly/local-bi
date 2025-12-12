import pandas as pd
from . import get_connection
from ..processing import process_table

def get_stock_daily(since=None):
    conn = get_connection()
    
    query = "SELECT * FROM stock_daily"
    if since:
        query += f" WHERE snapshot_date >= '{since}'"
    
    df = pd.read_sql(query, conn)
    conn.close()
    return process_table(df, "stock_daily")