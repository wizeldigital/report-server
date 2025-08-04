import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from bson import ObjectId
import logging

from ..models import Store, CampaignStats, FlowStats
from ..core.config import settings
from .klaviyo_api import create_klaviyo_client

logger = logging.getLogger(__name__)


async def is_data_fresh(store: Store) -> bool:
    """Check if dashboard data is fresh enough"""
    if not store.last_dashboard_sync:
        return False
    
    threshold = timedelta(minutes=settings.SYNC_FRESHNESS_THRESHOLD_MINUTES)
    return datetime.utcnow() - store.last_dashboard_sync < threshold


async def sync_campaign_stats(store: Store, client) -> Dict[str, int]:
    """Sync campaign statistics for a store"""
    try:
        logger.info(f"Starting campaign sync for store {store.public_id}")
        
        # Get campaigns
        campaigns = await client.get_campaigns()
        
        # Get tags for mapping
        tags = await client.get_tags()
        tag_map = {tag["id"]: tag["attributes"]["name"] for tag in tags}
        
        # Get lists and segments for audience info
        lists = await client.get_lists()
        segments = await client.get_segments()
        
        audience_map = {}
        for lst in lists:
            audience_map[lst["id"]] = {
                "name": lst["attributes"]["name"],
                "type": "list"
            }
        for segment in segments:
            audience_map[segment["id"]] = {
                "name": segment["attributes"]["name"],
                "type": "segment"
            }
        
        created_count = 0
        updated_count = 0
        
        for campaign in campaigns:
            campaign_id = campaign["id"]
            attributes = campaign["attributes"]
            
            # Get campaign metrics
            metrics = await client.get_campaign_metrics(
                campaign_id,
                [
                    "opens", "open_rate", "clicks", "click_rate",
                    "delivered", "bounced", "unsubscribes", "spam_complaints",
                    "conversions", "conversion_value"
                ]
            )
            
            # Prepare campaign stat data
            stat_data = {
                "groupings": {
                    "send_channel": attributes.get("send_channel", "email"),
                    "campaign_id": campaign_id,
                    "campaign_message_id": attributes.get("message_id", campaign_id)
                },
                "campaign_name": attributes.get("name"),
                "store_id": store.id,
                "store_public_id": store.public_id,
                "send_time": datetime.fromisoformat(attributes["send_time"].replace("Z", "+00:00")) if attributes.get("send_time") else None,
                "created_at": datetime.fromisoformat(attributes["created_at"].replace("Z", "+00:00")) if attributes.get("created_at") else None,
                "scheduled_at": datetime.fromisoformat(attributes["scheduled_at"].replace("Z", "+00:00")) if attributes.get("scheduled_at") else None,
                "updated_at": datetime.fromisoformat(attributes["updated_at"].replace("Z", "+00:00")) if attributes.get("updated_at") else None
            }
            
            # Add statistics
            if metrics:
                stat_data["statistics"] = metrics.get("attributes", {})
            
            # Add tag information
            campaign_tags = campaign.get("relationships", {}).get("tags", {}).get("data", [])
            tag_ids = [tag["id"] for tag in campaign_tags]
            tag_names = [tag_map.get(tag_id, "") for tag_id in tag_ids if tag_id in tag_map]
            
            stat_data["tag_ids"] = tag_ids
            stat_data["tag_names"] = tag_names
            
            # Add audience information
            audiences = campaign.get("relationships", {}).get("audiences", {}).get("data", [])
            included_audiences = []
            excluded_audiences = []
            
            for audience in audiences:
                audience_id = audience["id"]
                if audience_id in audience_map:
                    audience_info = {
                        "id": audience_id,
                        "name": audience_map[audience_id]["name"],
                        "type": audience_map[audience_id]["type"]
                    }
                    
                    if audience.get("included", True):
                        included_audiences.append(audience_info)
                    else:
                        excluded_audiences.append(audience_info)
            
            stat_data["included_audiences"] = included_audiences
            stat_data["excluded_audiences"] = excluded_audiences
            
            # Check if stat exists
            existing = await CampaignStats.find_one({
                "groupings.campaign_id": campaign_id,
                "store_public_id": store.public_id
            })
            
            if existing:
                # Update existing
                for key, value in stat_data.items():
                    setattr(existing, key, value)
                await existing.save()
                updated_count += 1
            else:
                # Create new
                new_stat = CampaignStats(**stat_data)
                await new_stat.save()
                created_count += 1
        
        logger.info(f"Campaign sync completed for store {store.public_id}: {created_count} created, {updated_count} updated")
        return {"created": created_count, "updated": updated_count}
        
    except Exception as e:
        logger.error(f"Error syncing campaigns for store {store.public_id}: {str(e)}")
        raise


async def sync_flow_stats(store: Store, client) -> Dict[str, int]:
    """Sync flow statistics for a store"""
    try:
        logger.info(f"Starting flow sync for store {store.public_id}")
        
        # Get flows
        flows = await client.get_flows()
        
        # Get tags for mapping
        tags = await client.get_tags()
        tag_map = {tag["id"]: tag["attributes"]["name"] for tag in tags}
        
        flow_stats_to_upsert = []
        
        for flow in flows:
            flow_id = flow["id"]
            flow_attributes = flow["attributes"]
            
            # Get flow messages
            messages = await client.get_flow_messages(flow_id)
            
            for message in messages:
                message_id = message["id"]
                
                # Get message metrics
                metrics = await client.get_flow_message_metrics(flow_id, message_id)
                
                # Prepare flow stat data
                stat_data = {
                    "store": store.id,
                    "store_public_id": store.public_id,
                    "flow_id": flow_id,
                    "flow_message_id": message_id,
                    "flow_name": flow_attributes.get("name"),
                    "flow_status": flow_attributes.get("status"),
                    "flow_archived": flow_attributes.get("archived", False),
                    "flow_created": datetime.fromisoformat(flow_attributes["created"].replace("Z", "+00:00")) if flow_attributes.get("created") else None,
                    "flow_updated": datetime.fromisoformat(flow_attributes["updated"].replace("Z", "+00:00")) if flow_attributes.get("updated") else None,
                    "flow_trigger_type": flow_attributes.get("trigger_type"),
                    "send_channel": message.get("attributes", {}).get("channel", "email"),
                    "last_updated": datetime.utcnow()
                }
                
                # Add message details
                message_attrs = message.get("attributes", {})
                stat_data.update({
                    "message_name": message_attrs.get("name"),
                    "message_from_email": message_attrs.get("from_email"),
                    "message_from_label": message_attrs.get("from_label"),
                    "message_subject_line": message_attrs.get("subject"),
                    "message_preview_text": message_attrs.get("preview_text"),
                    "message_template_id": message_attrs.get("template_id")
                })
                
                # Add statistics (as arrays for time series)
                if metrics and "attributes" in metrics:
                    metric_attrs = metrics["attributes"]
                    stat_data["statistics"] = {
                        "opens": metric_attrs.get("opens", []),
                        "clicks": metric_attrs.get("clicks", []),
                        "delivered": metric_attrs.get("delivered", []),
                        "bounced": metric_attrs.get("bounced", []),
                        "unsubscribes": metric_attrs.get("unsubscribes", []),
                        "spam_complaints": metric_attrs.get("spam_complaints", []),
                        "conversions": metric_attrs.get("conversions", []),
                        "conversion_value": metric_attrs.get("conversion_value", [])
                    }
                    
                    # Add date times from metrics
                    stat_data["date_times"] = [
                        datetime.fromisoformat(dt.replace("Z", "+00:00"))
                        for dt in metric_attrs.get("datetime", [])
                    ]
                
                # Add tag information
                flow_tags = flow.get("relationships", {}).get("tags", {}).get("data", [])
                tag_ids = [tag["id"] for tag in flow_tags]
                tag_names = [tag_map.get(tag_id, "") for tag_id in tag_ids if tag_id in tag_map]
                
                stat_data["tag_ids"] = tag_ids
                stat_data["tag_names"] = tag_names
                
                flow_stats_to_upsert.append(stat_data)
        
        # Bulk upsert flow stats
        result = await FlowStats.bulk_upsert_flow_stats(flow_stats_to_upsert)
        
        logger.info(f"Flow sync completed for store {store.public_id}: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error syncing flows for store {store.public_id}: {str(e)}")
        raise


async def sync_store_stats(store_id: str):
    """Main sync function for a store"""
    try:
        # Get store
        store = await Store.get(store_id)
        if not store:
            logger.error(f"Store not found: {store_id}")
            return
        
        # Check if already updating
        if store.is_updating_dashboard:
            logger.info(f"Store {store.public_id} is already updating")
            return
        
        # Check if data is fresh
        if await is_data_fresh(store):
            logger.info(f"Data is fresh for store {store.public_id}, skipping sync")
            store.is_updating_dashboard = False
            await store.save()
            return
        
        # Mark as updating
        store.is_updating_dashboard = True
        await store.save()
        
        try:
            # Get Klaviyo API key
            api_key = store.klaviyo_integration.api_key or settings.KLAVIYO_API_KEY
            if not api_key:
                raise ValueError("No Klaviyo API key available")
            
            async with await create_klaviyo_client(api_key) as client:
                # Sync campaigns and flows in parallel
                campaign_task = asyncio.create_task(sync_campaign_stats(store, client))
                flow_task = asyncio.create_task(sync_flow_stats(store, client))
                
                campaign_result, flow_result = await asyncio.gather(
                    campaign_task, flow_task, return_exceptions=True
                )
                
                if isinstance(campaign_result, Exception):
                    logger.error(f"Campaign sync failed: {campaign_result}")
                
                if isinstance(flow_result, Exception):
                    logger.error(f"Flow sync failed: {flow_result}")
                
                # Update sync timestamp
                store.last_dashboard_sync = datetime.utcnow()
                store.is_updating_dashboard = False
                await store.save()
                
                logger.info(f"Sync completed for store {store.public_id}")
                
        except Exception as e:
            logger.error(f"Error during sync for store {store.public_id}: {str(e)}")
            store.is_updating_dashboard = False
            await store.save()
            raise
            
    except Exception as e:
        logger.error(f"Fatal error in sync_store_stats: {str(e)}")
        raise