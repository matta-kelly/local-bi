# connections/shopify/__init__.py
"""
Shopify API client for read/write operations.
"""
import requests
import logging
import time
import json
import os

import shopify
from tenacity import (
    retry, 
    stop_after_attempt, 
    wait_fixed, 
    retry_if_exception_type, 
    before_sleep_log
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ShopifyClient:
    """Client for Shopify REST and GraphQL APIs."""
    
    def __init__(self):
        self.shop_url = os.environ.get("SHOP_URL")
        self.token = os.environ.get("SHOP_TOKEN")
        if not all([self.shop_url, self.token]):
            raise ValueError("Missing SHOP_URL or SHOP_TOKEN environment variables")
        self.api_version = "2025-01"

    def get_headers(self):
        """Return headers with the required Shopify access token."""
        return {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.token
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        before_sleep=before_sleep_log(logger, logging.INFO)
    )
    def fetch(self, endpoint, params=None):
        """Fetch from Shopify REST API with rate limiting."""
        url = f"https://{self.shop_url}/admin/api/{self.api_version}/{endpoint}.json"
        try:
            response = requests.get(url, headers=self.get_headers(), params=params, timeout=30)
            response.raise_for_status()
            
            # Rate limit handling
            call_limit = response.headers.get('X-Shopify-Shop-Api-Call-Limit', '0/40').split('/')
            calls_made = int(call_limit[0])
            calls_max = int(call_limit[1])
            if calls_made > calls_max * 0.8:
                logger.warning(f"Approaching REST rate limit: {calls_made}/{calls_max}, delaying 2 seconds")
                time.sleep(2)
            
            logger.debug(f"Fetched REST data for {endpoint}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch REST {endpoint}: {e}")
            raise

    def execute(self, query, variables=None):
        """Execute a Shopify GraphQL query/mutation."""
        try:
            session = shopify.Session(self.shop_url, self.api_version, self.token)
            shopify.ShopifyResource.activate_session(session)
            response = shopify.GraphQL().execute(query, variables or {})
            shopify.ShopifyResource.clear_session()

            logger.debug(f"Executed GraphQL: {query[:50]}...")
            parsed = response if isinstance(response, dict) else json.loads(response)
            
            return parsed
        except Exception as e:
            logger.error(f"Failed to execute GraphQL: {e}")
            raise

    def mutate(self, mutation, variables=None, dry_run=False):
        """Execute a GraphQL mutation with dry-run support."""
        if dry_run:
            logger.info(f"[DRY RUN] Would execute mutation with variables: {variables}")
            return None
        
        response = self.execute(mutation, variables)
        
        if response and response.get("errors"):
            raise Exception(f"GraphQL error: {response['errors']}")
        
        return response


def get_client() -> ShopifyClient:
    """Get a ShopifyClient instance."""
    return ShopifyClient()


def safe_get(data, *keys, default=None):
    """Safely extract nested values from a dictionary."""
    current = data
    for key in keys:
        try:
            if current is None or key not in current:
                return default
            current = current[key]
        except (TypeError, KeyError, AttributeError):
            return default
    return current if current is not None else default