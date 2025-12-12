import pandas as pd
from . import get_client
from ..processing import process_table

def get_segments(created_after=None, created_before=None, updated_after=None):
    """Fetch segments, optionally filtered by date."""
    klaviyo = get_client()
    segments = []
    cursor = None
    
    filters = []
    if created_after:
        filters.append(f"greater-than(created,{created_after})")
    if created_before:
        filters.append(f"less-than(created,{created_before})")
    if updated_after:
        filters.append(f"greater-than(updated,{updated_after})")
    
    filter_str = ",".join(filters) if filters else None
    
    while True:
        response = klaviyo.Segments.get_segments(page_cursor=cursor, filter=filter_str)
        
        for seg in response.data:
            segments.append({
                'segment_id': seg.id,
                'segment_name': seg.attributes.name
            })
        
        if response.links and response.links.next:
            cursor = response.links.next
        else:
            break
    
    return process_table(pd.DataFrame(segments), "segments")