# connections/shopify/badges.py
"""
Shopify product badge operations via metafields.
Badge is stored at: product.metafields.custom.badge
"""
import logging
from typing import List, Union, Optional

from . import get_client, safe_get

logger = logging.getLogger(__name__)

# ============================================================
# MUTATIONS
# ============================================================

BADGE_UPDATE_MUTATION = """
mutation productUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product {
      id
      metafield(namespace: "custom", key: "badge") {
        value
      }
    }
    userErrors {
      field
      message
    }
  }
}
"""

BADGE_DELETE_MUTATION = """
mutation MetafieldsDelete($metafields: [MetafieldIdentifierInput!]!) {
  metafieldsDelete(metafields: $metafields) {
    deletedMetafields {
      key
      namespace
      ownerId
    }
    userErrors {
      field
      message
    }
  }
}
"""


# ============================================================
# PUBLIC FUNCTIONS
# ============================================================

def update_badge(
    product_ids: Union[str, List[str]], 
    badge: str, 
    dry_run: bool = True
) -> dict:
    """
    Update the badge metafield for one or more products.
    """
    product_ids = _normalize_ids(product_ids)
    
    logger.info(f"Setting badge to '{badge}' for {len(product_ids)} product(s) (dry_run={dry_run})")
    
    client = get_client()
    results = {"success": [], "failed": [], "errors": []}
    
    for product_id in product_ids:
        variables = {
            "input": {
                "id": product_id,
                "metafields": [
                    {
                        "namespace": "custom",
                        "key": "badge",
                        "value": badge,
                        "type": "single_line_text_field"
                    }
                ]
            }
        }
        
        try:
            response = client.mutate(BADGE_UPDATE_MUTATION, variables, dry_run=dry_run)
            
            if dry_run:
                results["success"].append(product_id)
                continue
            
            user_errors = safe_get(response, "data", "productUpdate", "userErrors", default=[])
            
            if user_errors:
                results["failed"].append(product_id)
                results["errors"].append({"product_id": product_id, "errors": user_errors})
                logger.error(f"Failed to update badge for {product_id}: {user_errors}")
            else:
                results["success"].append(product_id)
                logger.info(f"Updated badge to '{badge}' for {product_id}")
                
        except Exception as e:
            results["failed"].append(product_id)
            results["errors"].append({"product_id": product_id, "errors": str(e)})
            logger.error(f"Exception updating badge for {product_id}: {e}")
    
    _log_summary("update_badge", results, dry_run)
    return results


def clear_badge(product_ids: Union[str, List[str]], dry_run: bool = True) -> dict:
    """
    Clear the badge metafield for one or more products.
    Uses metafieldsDelete mutation.
    """
    product_ids = _normalize_ids(product_ids)
    
    logger.info(f"Clearing badge for {len(product_ids)} product(s) (dry_run={dry_run})")
    
    client = get_client()
    results = {"success": [], "failed": [], "errors": []}
    
    for product_id in product_ids:
        variables = {
            "metafields": [
                {
                    "ownerId": product_id,
                    "namespace": "custom",
                    "key": "badge"
                }
            ]
        }
        
        try:
            response = client.mutate(BADGE_DELETE_MUTATION, variables, dry_run=dry_run)
            
            if dry_run:
                results["success"].append(product_id)
                continue
            
            user_errors = safe_get(response, "data", "metafieldsDelete", "userErrors", default=[])
            
            if user_errors:
                results["failed"].append(product_id)
                results["errors"].append({"product_id": product_id, "errors": user_errors})
                logger.error(f"Failed to clear badge for {product_id}: {user_errors}")
            else:
                results["success"].append(product_id)
                logger.info(f"Cleared badge for {product_id}")
                
        except Exception as e:
            results["failed"].append(product_id)
            results["errors"].append({"product_id": product_id, "errors": str(e)})
            logger.error(f"Exception clearing badge for {product_id}: {e}")
    
    _log_summary("clear_badge", results, dry_run)
    return results


# ============================================================
# HELPERS
# ============================================================

def _normalize_ids(product_ids: Union[str, List[str]]) -> List[str]:
    """Ensure product_ids is a list of full GID strings."""
    if isinstance(product_ids, str):
        product_ids = [product_ids]
    
    normalized = []
    for pid in product_ids:
        pid = str(pid).strip()
        if not pid.startswith("gid://"):
            pid = f"gid://shopify/Product/{pid}"
        normalized.append(pid)
    
    return normalized


def _log_summary(operation: str, results: dict, dry_run: bool) -> None:
    """Log operation summary."""
    prefix = "[DRY RUN] " if dry_run else ""
    logger.info(
        f"{prefix}{operation} complete: "
        f"{len(results['success'])} succeeded, {len(results['failed'])} failed"
    )