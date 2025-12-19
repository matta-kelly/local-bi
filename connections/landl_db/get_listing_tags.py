# connections/landl_db/product_tags.py
import pandas as pd
from . import get_connection


def get_listing_tags():
    conn = get_connection()
    query = "SELECT * FROM shopify_product_tags"
    df = pd.read_sql(query, conn)
    conn.close()
    return df