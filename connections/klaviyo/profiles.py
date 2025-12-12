import pandas as pd
from . import get_client
from ..processing import process_table

def get_profiles_by_segment(segment_ids, joined_after=None):
    """Fetch profiles and membership for given segment IDs."""
    klaviyo = get_client()
    profiles = {}
    membership = []
    
    filter_str = None
    if joined_after:
        filter_str = f"greater-than(joined_group_at,{joined_after})"
    
    for seg_id in segment_ids:
        cursor = None
        
        while True:
            response = klaviyo.Segments.get_segment_profiles(
                seg_id, 
                page_cursor=cursor,
                filter=filter_str
            )
            
            for profile in response.data:
                profile_id = profile.id
                email = profile.attributes.email
                
                if profile_id not in profiles:
                    profiles[profile_id] = {'profile_id': profile_id, 'email': email}
                
                membership.append({
                    'profile_id': profile_id,
                    'segment_id': seg_id
                })
            
            if response.links and response.links.next:
                cursor = response.links.next
            else:
                break
    
    profiles_df = process_table(pd.DataFrame(list(profiles.values())), "profiles")
    membership_df = process_table(pd.DataFrame(membership), "segment_membership")
    
    return profiles_df, membership_df