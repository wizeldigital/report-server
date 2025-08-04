"""
Stats merger functions to merge Klaviyo campaign and flow statistics with metadata
"""
from typing import List, Dict, Any


def merge_campaign_stats_with_meta(
    campaign_values: List[Dict[str, Any]],
    email_campaigns: List[Dict[str, Any]],
    sms_campaigns: List[Dict[str, Any]],
    segments: List[Dict[str, Any]],
    lists: List[Dict[str, Any]],
    tags: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Merge campaign statistics with campaign metadata for both email and sms."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Debug: Log the first result to see the data structure
    if campaign_values:
        first_result = campaign_values[0]
        logger.debug(f"🔍 Debug campaign stats structure: {{"
                    f"firstResult: {first_result}, "
                    f"statisticsKeys: {list(first_result.get('statistics', {}).keys())}, "
                    f"hasUnsubscribes: {'unsubscribes' in first_result.get('statistics', {})}, "
                    f"hasSpamComplaints: {'spam_complaints' in first_result.get('statistics', {})}, "
                    f"totalResults: {len(campaign_values)}}}")
    
    # Build lookup maps for both email and sms campaigns
    campaign_map = {}
    for c in email_campaigns:
        campaign_map[c["id"]] = c
    for c in sms_campaigns:
        campaign_map[c["id"]] = c
    
    # Build tag name lookup
    tag_name_map = {}
    for tag in tags:
        tag_name_map[tag["id"]] = tag.get("attributes", {}).get("name")
    
    # Build audience lookup with type information from segments and lists
    audience_map = {}
    for seg in segments:
        audience_map[seg["id"]] = {
            "id": seg["id"],
            "type": "segment",
            "name": seg.get("attributes", {}).get("name", "Unknown Segment")
        }
    for lst in lists:
        audience_map[lst["id"]] = {
            "id": lst["id"],
            "type": "list",
            "name": lst.get("attributes", {}).get("name", "Unknown List")
        }
    
    # Merge stats with campaign meta
    merged_stats = []
    
    for r in campaign_values:
        if r["groupings"].get("send_channel") not in ["email", "sms"]:
            continue
            
        campaign_id = r["groupings"]["campaign_id"]
        campaign = campaign_map.get(campaign_id)
        
        if not campaign:
            continue
        
        # Extract campaign attributes
        attributes = campaign.get("attributes", {})
        relationships = campaign.get("relationships", {})
        
        # Extract tag IDs
        tag_ids = [tag["id"] for tag in relationships.get("tags", {}).get("data", [])]
        
        # Extract audience IDs from attributes
        included_audience_ids = attributes.get("audiences", {}).get("included", [])
        excluded_audience_ids = attributes.get("audiences", {}).get("excluded", [])
        
        # Extract campaign message ID from the campaign data
        campaign_message_data = relationships.get("campaign-messages", {}).get("data", [])
        campaign_message_id = campaign_message_data[0]["id"] if campaign_message_data else None
        
        # Ensure all required statistics fields are present with defaults
        statistics = {
            "opens": 0,
            "open_rate": 0,
            "bounced": 0,
            "clicks": 0,
            "clicks_unique": 0,
            "click_rate": 0,
            "delivered": 0,
            "bounced_or_failed": 0,
            "bounced_or_failed_rate": 0,
            "delivery_rate": 0,
            "failed": 0,
            "failed_rate": 0,
            "recipients": 0,
            "opens_unique": 0,
            "bounce_rate": 0,
            "unsubscribe_rate": 0,
            "unsubscribe_uniques": 0,
            "unsubscribes": 0,
            "spam_complaint_rate": 0,
            "spam_complaints": 0,
            "click_to_open_rate": 0,
            "conversions": 0,
            "conversion_uniques": 0,
            "conversion_value": 0,
            "conversion_rate": 0,
            "average_order_value": 0,
            "revenue_per_recipient": 0,
            **r["statistics"]  # Override with actual values from API
        }
        
        # Build merged stat
        merged_stat = {
            **r,  # all original data
            "groupings": {
                **r["groupings"],
                "campaign_message_id": campaign_message_id or r["groupings"].get("campaign_message_id")
            },
            "statistics": statistics,  # Use our normalized statistics object
            "campaign_name": attributes.get("name"),
            "included_audiences": [audience_map[id] for id in included_audience_ids if id in audience_map],
            "excluded_audiences": [audience_map[id] for id in excluded_audience_ids if id in audience_map],
            "tagIds": tag_ids,
            "tagNames": [tag_name_map[id] for id in tag_ids if id in tag_name_map],
            "send_time": attributes.get("send_time"),
            "created_at": attributes.get("created_at"),
            "scheduled_at": attributes.get("scheduled_at"),
            "updated_at": attributes.get("updated_at")
        }
        
        merged_stats.append(merged_stat)
    
    return merged_stats


def merge_flow_stats_with_meta(
    flow_series_results: List[Dict[str, Any]],
    flows: List[Dict[str, Any]],
    tags: List[Dict[str, Any]],
    message_details_map: Dict[str, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Merge flow statistics with flow metadata and message details."""
    
    # Build lookup map for flows
    flow_map = {flow["id"]: flow for flow in flows}
    
    # Build tag name lookup
    tag_name_map = {}
    for tag in tags:
        tag_name_map[tag["id"]] = tag.get("attributes", {}).get("name")
    
    # Process each result individually (no merging by flow_id)
    merged_stats = []
    
    for stat in flow_series_results:
        flow_id = stat["groupings"]["flow_id"]
        flow_message_id = stat["groupings"]["flow_message_id"]
        flow = flow_map.get(flow_id)
        
        if not flow:
            continue
        
        # Extract flow attributes
        attributes = flow.get("attributes", {})
        relationships = flow.get("relationships", {})
        
        # Extract tag IDs
        tag_ids = [tag["id"] for tag in relationships.get("tags", {}).get("data", [])]
        
        # Get message details from the definitions map
        message_details = message_details_map.get(flow_message_id, {})
        
        # Build merged stat
        merged_stat = {
            "groupings": {
                "flow_id": flow_id,
                "flow_message_id": flow_message_id,
                "send_channel": stat["groupings"].get("send_channel", "email"),
            },
            "flow_name": attributes.get("name"),
            "flow_status": attributes.get("status"),
            "flow_archived": attributes.get("archived", False),
            "flow_created": attributes.get("created"),
            "flow_updated": attributes.get("updated"),
            "flow_trigger_type": attributes.get("trigger_type"),
            "tag_ids": tag_ids,
            "tag_names": [tag_name_map[id] for id in tag_ids if id in tag_name_map],
            "statistics": stat["statistics"],  # Direct statistics object (no merging)
            "date_times": stat.get("date_times", []),
            
            # Message details from flow definitions
            **message_details
        }
        
        merged_stats.append(merged_stat)
    
    return merged_stats