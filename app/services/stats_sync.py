import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from bson import ObjectId
import logging

from ..models import Store, CampaignStats
from ..core.config import settings
from .klaviyo_api import klaviyo_get_all, klaviyo_report_post
from .stats_merger import merge_campaign_stats_with_meta, merge_flow_stats_with_meta
from .flow_definitions import get_flow_definitions, build_message_details_map
from ..utils.memory_monitor import MemoryMonitor

logger = logging.getLogger(__name__)


async def klaviyo_update_stats(store: Store, date: Optional[datetime] = None) -> Store:
    """Update store statistics from Klaviyo API - matching Node.js version"""
    
    # Log memory at sync start
    MemoryMonitor.log_memory_usage(f"Sync Start - Store {store.public_id}")
    
    # Freshness threshold (temporarily reduced to force sync for debugging)
    FRESHNESS_THRESHOLD_MS = 1 * 60 * 1000  # 1 minute instead of 15 minutes
    now = datetime.utcnow()
    last_sync = store.klaviyo_integration.campaign_values_last_update.timestamp() * 1000 if store.klaviyo_integration.campaign_values_last_update else 0
    now_ms = now.timestamp() * 1000
    
    # If already updating, or data is fresh, do nothing
    if store.klaviyo_integration.is_updating_dashboard or (now_ms - last_sync < FRESHNESS_THRESHOLD_MS):
        logger.info('🔄 Sync skipped - data is fresh or already updating')
        return store
    
    from_date = date.isoformat() if date else (store.klaviyo_integration.campaign_values_last_update.isoformat() if store.klaviyo_integration.campaign_values_last_update else datetime(2024, 1, 1).isoformat())
    
    # Set updating flag
    store.klaviyo_integration.is_updating_dashboard = True
    await store.save()
    
    try:
        # Get API key (support both field names)
        api_key = store.klaviyo_integration.apiKey or store.klaviyo_integration.api_key
        if not api_key:
            raise ValueError("No Klaviyo API key available")
        
        # Parallel API calls matching Node.js version
        results = await asyncio.gather(
            klaviyo_get_all(f"campaigns?filter=equals(messages.channel,'email'),greater-or-equal(created_at,{from_date}),equals(status,'Sent')&include=campaign-messages,tags", api_key),
            klaviyo_get_all(f"campaigns?filter=equals(messages.channel,'sms'),greater-or-equal(created_at,{from_date}),equals(status,'Sent')&include=tags", api_key),
            klaviyo_get_all("tags", api_key),
            klaviyo_get_all("flows", api_key),
            klaviyo_get_all("segments", api_key),
            klaviyo_get_all("lists", api_key),
            klaviyo_report_post("campaign-values-reports", {
                "data": {
                    "type": "campaign-values-report",
                    "attributes": {
                        "statistics": [
                            "average_order_value",
                            "bounce_rate",
                            "bounced",
                            "bounced_or_failed",
                            "bounced_or_failed_rate",
                            "click_rate",
                            "click_to_open_rate",
                            "clicks",
                            "clicks_unique",
                            "conversion_rate",
                            "conversion_uniques",
                            "conversion_value",
                            "conversions",
                            "delivered",
                            "delivery_rate",
                            "failed",
                            "failed_rate",
                            "open_rate",
                            "opens",
                            "opens_unique",
                            "recipients",
                            "revenue_per_recipient",
                            "spam_complaint_rate",
                            "spam_complaints",
                            "unsubscribe_rate",
                            "unsubscribe_uniques",
                            "unsubscribes"
                        ],
                        "timeframe": {
                            "key": "last_12_months"
                        },
                        "conversion_metric_id": store.klaviyo_integration.conversion_metric_id,
                        "filter": "contains-any(send_channel,['email','sms'])"

                    }
                }
            }, api_key),
            klaviyo_report_post("flow-series-reports", {
                "data": {
                    "type": "flow-series-report",
                    "attributes": {
                        "statistics": [
                            "average_order_value",
                            "bounce_rate",
                            "bounced",
                            "bounced_or_failed",
                            "bounced_or_failed_rate",
                            "click_rate",
                            "click_to_open_rate",
                            "clicks",
                            "clicks_unique",
                            "conversion_rate",
                            "conversion_uniques",
                            "conversion_value",
                            "conversions",
                            "delivered",
                            "delivery_rate",
                            "failed",
                            "failed_rate",
                            "open_rate",
                            "opens",
                            "opens_unique",
                            "recipients",
                            "revenue_per_recipient",
                            "spam_complaint_rate",
                            "spam_complaints",
                            "unsubscribe_rate",
                            "unsubscribe_uniques",
                            "unsubscribes"
                        ],
                        "timeframe": {
                            "start": (datetime.utcnow() - timedelta(days=60)).isoformat(),
                            "end": datetime.utcnow().isoformat()
                        },
                        "interval": "daily",
                        "conversion_metric_id": store.klaviyo_integration.conversion_metric_id
                    }
                }
            }, api_key)
        )
        
        # Unpack results
        campaigns, sms_campaigns, tags, flows, segments, lists, campaign_values_reports, flow_series_reports = results
        
        # Merge campaign stats with metadata
        merged = merge_campaign_stats_with_meta(
            campaign_values_reports["data"]["attributes"]["results"],
            campaigns["data"],
            sms_campaigns["data"],
            segments["data"],
            lists["data"],
            tags["data"]
        )
        
        # Get unique flow IDs from the results to fetch their definitions
        unique_flow_ids = list(set(
            result["groupings"]["flow_id"] 
            for result in flow_series_reports["data"]["attributes"]["results"]
        ))
        
        logger.info(f"🔍 Fetching definitions for {len(unique_flow_ids)} flows")
        
        # Fetch flow definitions with message details (uses additional-fields parameter)
        flow_definitions = await get_flow_definitions(unique_flow_ids, api_key)
        
        # Build message details map from flow definitions
        message_details_map = build_message_details_map(flow_definitions)
        
        logger.info(f"📋 Found message details for {len(message_details_map)} flow messages")
        
        # Merge flow stats with metadata
        merged_flows = merge_flow_stats_with_meta(
            flow_series_reports["data"]["attributes"]["results"],
            flows["data"],
            tags["data"],
            message_details_map
        )
        
        # Extract tag names
        tag_names = [tag["attributes"]["name"] for tag in tags["data"] if tag.get("attributes", {}).get("name")]
        
        # Convert date_times strings to Date objects for storage
        flow_date_times = [
            datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            for date_str in flow_series_reports["data"]["attributes"].get("date_times", [])
        ]
        
        # Update store with Klaviyo integration
        now = datetime.utcnow()
        store.klaviyo_integration.campaign_values_last_update = now
        store.klaviyo_integration.flow_values_last_update = now
        store.tagNames = tag_names
        store.klaviyo_integration.is_updating_dashboard = False
        store.klaviyo_integration.flow_date_times = flow_date_times
        await store.save()
        
        # Upsert campaign and flow stats
        await upsert_campaign_stats(merged, store.klaviyo_integration.public_id)
        await upsert_flow_stats(merged_flows, store.klaviyo_integration.public_id)
        
        # Log memory after sync completion
        MemoryMonitor.log_memory_usage(f"Sync Complete - Store {store.public_id}")
        
        return store
        
    except Exception as e:
        # On error, clear updating flag
        store.klaviyo_integration.is_updating_dashboard = False
        await store.save()
        logger.error(f"Error during sync for store {store.public_id}: {str(e)}")
        
        # Log memory on error
        MemoryMonitor.log_memory_usage(f"Sync Error - Store {store.public_id}")
        raise


async def upsert_campaign_stats(merged: List[Dict[str, Any]], klaviyo_public_id: str):
    """Upsert campaign statistics - matching Node.js version"""
    for stat in merged:
        # Find existing stat
        existing = await CampaignStats.find_one({
            "klaviyo_public_id": klaviyo_public_id,
            "groupings.campaign_id": stat["groupings"]["campaign_id"]
        })
        
        if existing:
            # Update existing
            for key, value in stat.items():
                setattr(existing, key, value)
            existing.klaviyo_public_id = klaviyo_public_id
            await existing.save()
        else:
            # Create new
            new_stat = CampaignStats(
                **stat,
                klaviyo_public_id=klaviyo_public_id
            )
            await new_stat.save()
    
    logger.info(f"✅ Completed upserting campaign stats for klaviyo account {klaviyo_public_id}")


async def upsert_flow_stats(merged: List[Dict[str, Any]], klaviyo_public_id: str):
    """Upsert flow statistics to FlowRecentStats for daily data"""
    logger.info(f"🔄 Upserting {len(merged)} flow stats for klaviyo account {klaviyo_public_id}")
    
    # Import FlowRecentStats model
    try:
        from ..models import FlowRecentStats
        
        # Process the flow stats for FlowRecentStats
        flow_stats_to_upsert = []
        
        for stat in merged:
            # Extract from groupings like the merger creates them
            groupings = stat.get("groupings", {})
            flow_id = groupings.get("flow_id")
            flow_message_id = groupings.get("flow_message_id")
            send_channel = groupings.get("send_channel")
            
            logger.info(f"📊 Flow: {flow_id} - Message: {flow_message_id} ({send_channel})")
            
            # Prepare document for FlowRecentStats with arrays of statistics
            doc = {
                "klaviyo_public_id": klaviyo_public_id,
                "flow_id": flow_id,
                "flow_message_id": flow_message_id,
                "send_channel": send_channel,
                "flow_name": stat.get("flow_name"),
                "flow_message_name": stat.get("flow_message_name"),
                "flow_message_subject": stat.get("flow_message_content"),
                "tag_ids": stat.get("tag_ids", []),
                "tag_names": stat.get("tag_names", []),
                "statistics": stat.get("statistics", {}),
                "date_times": stat.get("date_times", []),
                "last_updated": datetime.utcnow()
            }
            
            flow_stats_to_upsert.append(doc)
        
        # Use bulk upsert method if available
        if hasattr(FlowRecentStats, 'bulk_upsert_flow_stats'):
            result = await FlowRecentStats.bulk_upsert_flow_stats(flow_stats_to_upsert)
            logger.info(f"✅ Bulk upserted flow stats: {result}")
        else:
            # Fallback to individual upserts
            for doc in flow_stats_to_upsert:
                existing = await FlowRecentStats.find_one({
                    "klaviyo_public_id": doc["klaviyo_public_id"],
                    "flow_id": doc["flow_id"],
                    "flow_message_id": doc["flow_message_id"],
                    "send_channel": doc["send_channel"]
                })
                
                if existing:
                    # Update existing
                    for key, value in doc.items():
                        setattr(existing, key, value)
                    await existing.save()
                else:
                    # Create new
                    new_stat = FlowRecentStats(**doc)
                    await new_stat.save()
        
        logger.info(f"✅ Completed upserting flow stats for klaviyo account {klaviyo_public_id}")
        
    except ImportError:
        # Fallback to direct MongoDB operations if FlowRecentStats is not available
        logger.warning("FlowRecentStats model not available, using direct MongoDB operations")
        from ..db.database import get_database
        db = await get_database()
        collection = db.flowrecentstats
        
        for stat in merged:
            # Extract from groupings like the merger creates them
            groupings = stat.get("groupings", {})
            flow_id = groupings.get("flow_id")
            flow_message_id = groupings.get("flow_message_id")
            send_channel = groupings.get("send_channel")
            
            logger.info(f"📊 Flow: {flow_id} - Message: {flow_message_id} ({send_channel})")
            
            # Prepare document with klaviyo reference
            doc = {
                **stat,
                "klaviyo_public_id": klaviyo_public_id,
                "last_updated": datetime.utcnow(),
                "updatedAt": datetime.utcnow()
            }
            
            # Upsert using MongoDB directly
            await collection.update_one(
                {
                    "klaviyo_public_id": klaviyo_public_id,
                    "flow_id": flow_id,
                    "flow_message_id": flow_message_id,
                    "send_channel": send_channel
                },
                {"$set": doc},
                upsert=True
            )
        
        logger.info(f"✅ Completed upserting flow stats for klaviyo account {klaviyo_public_id}")


# Legacy functions for compatibility
async def is_data_fresh(store: Store) -> bool:
    """Check if dashboard data is fresh enough"""
    if not store.klaviyo_integration.campaign_values_last_update:
        return False
    
    threshold = timedelta(minutes=settings.SYNC_FRESHNESS_THRESHOLD_MINUTES)
    return datetime.utcnow() - store.klaviyo_integration.campaign_values_last_update < threshold


async def sync_store_stats(klaviyo_public_id: str):
    """Main sync function for a klaviyo account"""
    try:
        # Get store
        store = await Store.find_one({"klaviyo_integration.public_id": klaviyo_public_id})
        if not store:
            logger.error(f"Store not found for klaviyo_public_id: {klaviyo_public_id}")
            return
        
        # Check if already updating
        if store.klaviyo_integration.is_updating_dashboard:
            logger.info(f"Store {store.public_id} is already updating")
            return
        
        # Check if data is fresh
        if await is_data_fresh(store):
            logger.info(f"Data is fresh for store {store.public_id}, skipping sync")
            store.klaviyo_integration.is_updating_dashboard = False
            await store.save()
            return
        
        # Run the sync
        await klaviyo_update_stats(store)
        logger.info(f"Sync completed for store {store.public_id}")
        
    except Exception as e:
        logger.error(f"Fatal error in sync_store_stats: {str(e)}")
        raise