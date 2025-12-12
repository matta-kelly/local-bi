import pandas as pd
import requests
from . import SITE_ID, BGFILTER_FIELD
from ..processing import process_table

def get_collection_products(collection_handle, per_page=100):
    """Fetch all products for a collection, paginating through results."""
    
    url = f"https://{SITE_ID}.a.searchspring.io/api/search/search.json"
    all_products = []
    page = 1
    
    while True:
        params = {
            "siteId": SITE_ID,
            "resultsPerPage": per_page,
            "page": page,
            f"bgfilter.{BGFILTER_FIELD}": collection_handle,
            "resultsFormat": "native",
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        products = data.get("results", [])
        all_products.extend(products)
        
        pagination = data.get("pagination", {})
        total_pages = pagination.get("totalPages", 1)
        
        print(f"  Page {page}/{total_pages} - fetched {len(products)} products")
        
        if page >= total_pages:
            break
        page += 1
    
    df = pd.DataFrame([{
        "position": i,
        "name": p.get("name"),
        "price": p.get("price"),
        "compare_at_price": p.get("msrp"),
        "available": p.get("ss_available"),
    } for i, p in enumerate(all_products, start=1)])
    
    return process_table(df, "searchspring_collection")