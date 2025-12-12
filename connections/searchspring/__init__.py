import os
from dotenv import load_dotenv

load_dotenv()

SITE_ID = os.getenv("SEARCHSPRING_SITE_ID")
BGFILTER_FIELD = os.getenv("SEARCHSPRING_BGFILTER_FIELD", "collection_handle")

def get_config():
    return {
        "site_id": SITE_ID,
        "bgfilter_field": BGFILTER_FIELD
    }