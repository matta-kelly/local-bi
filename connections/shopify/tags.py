# connections/shopify/tags.py
"""
Shopify product tag operations: add and remove tags.
"""
import logging
from typing import List, Union

from . import get_client, safe_get

logger = logging.getLogger(__name__)

# ============================================================
# MUTATIONS
# ============================================================

TAGS_ADD_MUTATION = """
mutation tagsAdd($id: ID!, $tags: [String!]!) {
  tagsAdd(id: $id, tags: $tags) {
    node {
      id
    }
    userErrors {
      field
      message
    }
  }
}
"""

TAGS_REMOVE_MUTATION = """
mutation tagsRemove($id: ID!, $tags: [String!]!) {
  tagsRemove(id: $id, tags: $tags) {
    node {
      id
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

def add_tags(
    product_ids: Union[str, List[str]], 
    tags: Union[str, List[str]], 
    dry_run: bool = True
) -> dict:
    """
    Add tags to one or more products.
    
    Args:
        product_ids: Single product ID or list of product IDs
                     (e.g., "gid://shopify/Product/123" or just "123")
        tags: Single tag or list of tags to add
        dry_run: If True, log what would happen but don't execute (default: True)
        
    Returns:
        Dict with 'success', 'failed', and 'errors' keys
    """
    product_ids = _normalize_ids(product_ids)
    tags = _normalize_tags(tags)
    
    logger.info(f"Adding tags {tags} to {len(product_ids)} product(s) (dry_run={dry_run})")
    
    client = get_client()
    results = {"success": [], "failed": [], "errors": []}
    
    for product_id in product_ids:
        variables = {"id": product_id, "tags": tags}
        
        try:
            response = client.mutate(TAGS_ADD_MUTATION, variables, dry_run=dry_run)
            
            if dry_run:
                results["success"].append(product_id)
                continue
            
            user_errors = safe_get(response, "data", "tagsAdd", "userErrors", default=[])
            
            if user_errors:
                results["failed"].append(product_id)
                results["errors"].append({"product_id": product_id, "errors": user_errors})
                logger.error(f"Failed to add tags to {product_id}: {user_errors}")
            else:
                results["success"].append(product_id)
                logger.info(f"Added tags {tags} to {product_id}")
                
        except Exception as e:
            results["failed"].append(product_id)
            results["errors"].append({"product_id": product_id, "errors": str(e)})
            logger.error(f"Exception adding tags to {product_id}: {e}")
    
    _log_summary("add_tags", results, dry_run)
    return results


def remove_tags(
    product_ids: Union[str, List[str]], 
    tags: Union[str, List[str]], 
    dry_run: bool = True
) -> dict:
    """
    Remove tags from one or more products.
    
    Args:
        product_ids: Single product ID or list of product IDs
                     (e.g., "gid://shopify/Product/123" or just "123")
        tags: Single tag or list of tags to remove
        dry_run: If True, log what would happen but don't execute (default: True)
        
    Returns:
        Dict with 'success', 'failed', and 'errors' keys
    """
    product_ids = _normalize_ids(product_ids)
    tags = _normalize_tags(tags)
    
    logger.info(f"Removing tags {tags} from {len(product_ids)} product(s) (dry_run={dry_run})")
    
    client = get_client()
    results = {"success": [], "failed": [], "errors": []}
    
    for product_id in product_ids:
        variables = {"id": product_id, "tags": tags}
        
        try:
            response = client.mutate(TAGS_REMOVE_MUTATION, variables, dry_run=dry_run)
            
            if dry_run:
                results["success"].append(product_id)
                continue
            
            user_errors = safe_get(response, "data", "tagsRemove", "userErrors", default=[])
            
            if user_errors:
                results["failed"].append(product_id)
                results["errors"].append({"product_id": product_id, "errors": user_errors})
                logger.error(f"Failed to remove tags from {product_id}: {user_errors}")
            else:
                results["success"].append(product_id)
                logger.info(f"Removed tags {tags} from {product_id}")
                
        except Exception as e:
            results["failed"].append(product_id)
            results["errors"].append({"product_id": product_id, "errors": str(e)})
            logger.error(f"Exception removing tags from {product_id}: {e}")
    
    _log_summary("remove_tags", results, dry_run)
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


def _normalize_tags(tags: Union[str, List[str]]) -> List[str]:
    """Ensure tags is a list of strings."""
    if isinstance(tags, str):
        return [tags]
    return [str(t) for t in tags]


def _log_summary(operation: str, results: dict, dry_run: bool) -> None:
    """Log operation summary."""
    prefix = "[DRY RUN] " if dry_run else ""
    logger.info(
        f"{prefix}{operation} complete: "
        f"{len(results['success'])} succeeded, {len(results['failed'])} failed"
    )