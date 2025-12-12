import os
from dotenv import load_dotenv
from klaviyo_api import KlaviyoAPI

load_dotenv()

def get_client():
    return KlaviyoAPI(os.getenv("KLAVIYO_API_KEY"))