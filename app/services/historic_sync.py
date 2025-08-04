import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dateutil.relativedelta import relativedelta
from ..models import Store, CampaignStats
from ..utils.memory_monitor import MemoryMonitor

logger = logging.getLogger(__name__)

# Rate limits for campaign values endpoint
BURST_LIMIT = 1  # 1 request per second
STEADY_LIMIT_DELAY = 30  # 30 seconds between requests (2/minute)
DAILY_LIMIT = 225


class HistoricSyncService:
    """Service for performing historic data sync from Klaviyo"""
    
    def __init__(self, store: Store):
        self.store = store
        self.requests_made = 0
        self.last_request_time: Optional[datetime] = None
        self.tag_map: Dict[str, str] = {}
        self.audience_map: Dict[str, Dict[str, str]] = {}
        self.email_campaigns: List[Dict[str, Any]] = []
        self.sms_campaigns: List[Dict[str, Any]] = []
        self.segments: List[Dict[str, Any]] = []
        self.lists: List[Dict[str, Any]] = []
        self.tags: List[Dict[str, Any]] = []
        self._metadata_loaded = False
        
    async def _apply_rate_limit(self):
        """Apply rate limiting based on burst and steady limits"""
        if self.last_request_time:
            elapsed = (datetime.utcnow() - self.last_request_time).total_seconds()
            
            # Ensure we respect the burst limit (1/s)
            if elapsed < BURST_LIMIT:
                await asyncio.sleep(BURST_LIMIT - elapsed)
            
            # For steady state, ensure 30s between requests
            if self.requests_made > 0 and elapsed < STEADY_LIMIT_DELAY:
                await asyncio.sleep(STEADY_LIMIT_DELAY - elapsed)
        
        self.last_request_time = datetime.utcnow()
        self.requests_made += 1
        
        # Check daily limit
        if self.requests_made >= DAILY_LIMIT:
            raise Exception(f"Daily rate limit of {DAILY_LIMIT} requests reached")
    
    async def _load_metadata(self):
        """Load tags, lists, and segments metadata once at the beginning"""
        if self._metadata_loaded:
            return
            
        logger.info("Loading metadata for historic sync")
        
        from .klaviyo_api import klaviyo_get_all
        api_key = self.store.klaviyo_integration.apiKey or self.store.klaviyo_integration.api_key
        
        # Load metadata in parallel - but NOT campaigns (we'll fetch those per period)
        tags_task = klaviyo_get_all("tags", api_key)
        lists_task = klaviyo_get_all("lists", api_key)
        segments_task = klaviyo_get_all("segments", api_key)
        
        try:
            tags_response, lists_response, segments_response = await asyncio.gather(
                tags_task, lists_task, segments_task
            )
        except Exception as e:
            logger.error(f"Error loading metadata: {str(e)}")
            raise
        
        # Store raw responses for merge function
        self.tags = tags_response.get("data", [])
        self.lists = lists_response.get("data", [])
        self.segments = segments_response.get("data", [])
        
        # Build tag map
        self.tag_map = {tag["id"]: tag["attributes"]["name"] for tag in self.tags}
        
        # Build audience map
        for lst in self.lists:
            self.audience_map[lst["id"]] = {
                "name": lst["attributes"]["name"],
                "type": "list"
            }
        for segment in self.segments:
            self.audience_map[segment["id"]] = {
                "name": segment["attributes"]["name"],
                "type": "segment"
            }
        
        self._metadata_loaded = True
        logger.info(f"Loaded {len(self.tag_map)} tags and {len(self.audience_map)} audiences")
    
    async def query_campaign_values(
        self, 
        start_date: datetime, 
        end_date: datetime,
        page_cursor: Optional[str] = None
    ) -> Dict[str, Any]:
        """Query campaign values from Klaviyo API"""
        await self._apply_rate_limit()
        
        # Get API key from store
        api_key = self.store.klaviyo_integration.apiKey or self.store.klaviyo_integration.api_key
        if not api_key:
            raise ValueError("No Klaviyo API key available")
        
        # Use klaviyo_report_post for campaign values
        from .klaviyo_api import klaviyo_report_post
        
        payload = {
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
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat()
                    },
                    "filter": "contains-any(send_channel,['email','sms'])"
                }
            }
        }
        
        # Add conversion metric ID if available
        if self.store.klaviyo_integration.conversion_metric_id:
            payload["data"]["attributes"]["conversion_metric_id"] = self.store.klaviyo_integration.conversion_metric_id
        
        try:
            response = await klaviyo_report_post("campaign-values-reports", payload, api_key)
            return response
        except Exception as e:
            logger.error(f"Error querying campaign values: {str(e)}")
            raise
    
    async def sync_time_period(self, start_date: datetime, end_date: datetime) -> int:
        """Sync campaign data for a specific time period"""
        logger.info(f"Syncing campaigns from {start_date.isoformat()} to {end_date.isoformat()}")
        
        try:
            # Fetch campaigns for this time period
            from .klaviyo_api import klaviyo_get_all
            api_key = self.store.klaviyo_integration.apiKey or self.store.klaviyo_integration.api_key
            
            # Format date for API filter with 1 week buffer on each side
            from_date = (start_date - timedelta(weeks=1)).isoformat()
            to_date = (end_date + timedelta(weeks=1)).isoformat()
            
            # Fetch campaigns created within the buffered period for maximum coverage
            email_campaigns_task = klaviyo_get_all(
                f"campaigns?filter=equals(messages.channel,'email'),greater-or-equal(created_at,{from_date}),less-than(created_at,{to_date}),equals(status,'Sent')&include=campaign-messages,tags",
                api_key
            )
            sms_campaigns_task = klaviyo_get_all(
                f"campaigns?filter=equals(messages.channel,'sms'),greater-or-equal(created_at,{from_date}),less-than(created_at,{to_date}),equals(status,'Sent')&include=campaign-messages,tags",
                api_key
            )
            
            # Fetch all in parallel (campaigns might timeout, but that's OK)
            try:
                (email_campaigns_response, sms_campaigns_response, response) = await asyncio.gather(
                    email_campaigns_task, 
                    sms_campaigns_task, 
                    self.query_campaign_values(start_date, end_date),
                    return_exceptions=True  # Don't fail if one fails
                )
                
                # Handle potential exceptions
                if isinstance(email_campaigns_response, Exception):
                    logger.warning(f"Failed to fetch email campaigns: {str(email_campaigns_response)}")
                    email_campaigns_response = {"data": []}
                    
                if isinstance(sms_campaigns_response, Exception):
                    logger.warning(f"Failed to fetch SMS campaigns: {str(sms_campaigns_response)}")
                    sms_campaigns_response = {"data": []}
                    
                if isinstance(response, Exception):
                    logger.error(f"Failed to fetch campaign values report: {str(response)}")
                    raise response
                    
            except Exception as e:
                logger.error(f"Unexpected error fetching data for period: {str(e)}")
                raise
            
            # Store campaigns for this period
            self.email_campaigns = email_campaigns_response.get("data", [])
            self.sms_campaigns = sms_campaigns_response.get("data", [])
            
            # Process the campaign data from the report results
            # The campaign-values-report returns data in attributes.results
            results = response.get("data", {}).get("attributes", {}).get("results", [])
            
            if results:
                await self.process_campaign_data(results)
                
            return len(results)
                    
        except Exception as e:
            logger.error(f"Error syncing time period {start_date} to {end_date}: {str(e)}", exc_info=True)
            raise
    
    async def process_campaign_data(self, results: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process and store campaign data from campaign-values-report results"""
        logger.info(f"Processing {len(results)} campaign results")
        
        # Use the merge function directly like Express does
        from .stats_merger import merge_campaign_stats_with_meta
        
        merged_stats = merge_campaign_stats_with_meta(
            results,  # campaign_values results
            self.email_campaigns,  # email campaigns data
            self.sms_campaigns,  # sms campaigns data
            self.segments,  # segments data
            self.lists,  # lists data
            self.tags  # tags data
        )
        
        # Add klaviyo_public_id to each merged stat
        for stat in merged_stats:
            stat["klaviyo_public_id"] = self.store.klaviyo_integration.public_id
        
        # Now prepare campaign stats for processing
        campaign_stats_to_process = []
        
        for stat_data in merged_stats:
            try:
                # Convert ISO strings to datetime objects for MongoDB
                if stat_data.get("send_time"):
                    stat_data["send_time"] = datetime.fromisoformat(stat_data["send_time"].replace("Z", "+00:00"))
                if stat_data.get("created_at"):
                    stat_data["created_at"] = datetime.fromisoformat(stat_data["created_at"].replace("Z", "+00:00"))
                if stat_data.get("scheduled_at"):
                    stat_data["scheduled_at"] = datetime.fromisoformat(stat_data["scheduled_at"].replace("Z", "+00:00"))
                if stat_data.get("updated_at"):
                    stat_data["updated_at"] = datetime.fromisoformat(stat_data["updated_at"].replace("Z", "+00:00"))
                
                campaign_stats_to_process.append(stat_data)
                    
            except Exception as e:
                campaign_id = stat_data.get("groupings", {}).get("campaign_id", "unknown")
                logger.error(f"Error processing campaign result for {campaign_id}: {str(e)}")
                continue
        
        # Extract campaign IDs from merged stats
        campaign_ids = [stat["groupings"]["campaign_id"] for stat in campaign_stats_to_process]
        
        # Batch check for existing campaigns
        existing_campaigns = await CampaignStats.find({
            "groupings.campaign_id": {"$in": campaign_ids},
            "klaviyo_public_id": self.store.klaviyo_integration.public_id
        }).to_list()
        
        existing_campaign_ids = {
            camp.groupings.campaign_id for camp in existing_campaigns
        }
        
        # Separate new and existing campaigns
        new_campaigns = []
        updates_to_perform = []
        
        for stat_data in campaign_stats_to_process:
            campaign_id = stat_data["groupings"]["campaign_id"]
            
            if campaign_id in existing_campaign_ids:
                # Find the existing campaign to update
                existing = next(
                    camp for camp in existing_campaigns 
                    if camp.groupings.campaign_id == campaign_id
                )
                updates_to_perform.append((existing, stat_data))
            else:
                new_campaigns.append(CampaignStats(**stat_data))
        
        # Perform batch operations
        created_count = 0
        updated_count = 0
        
        # Batch insert new campaigns
        if new_campaigns:
            await CampaignStats.insert_many(new_campaigns)
            created_count = len(new_campaigns)
        
        # Batch update existing campaigns (still need to do individually for Beanie)
        if updates_to_perform:
            update_tasks = []
            for existing, stat_data in updates_to_perform:
                for key, value in stat_data.items():
                    # Handle field aliases - map from serialized names to model field names
                    if key == "tagIds":
                        key = "tag_ids"
                    elif key == "tagNames":
                        key = "tag_names"
                    
                    # Only set fields that exist on the model
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                    else:
                        logger.debug(f"Skipping unknown field: {key}")
                        
                update_tasks.append(existing.save())
            
            # Run updates concurrently in batches of 10
            for i in range(0, len(update_tasks), 10):
                batch = update_tasks[i:i+10]
                await asyncio.gather(*batch)
            
            updated_count = len(updates_to_perform)
        
        logger.info(f"Campaign processing completed: {created_count} created, {updated_count} updated")
        
        return {"created": created_count, "updated": updated_count}
    
    async def run_historic_sync(self) -> Dict[str, Any]:
        """Run the complete historic sync process"""
        try:
            # Log memory at start
            MemoryMonitor.log_memory_usage(f"Historic Sync Start - Store {self.store.public_id}")
            
            # Set updating flag
            self.store.klaviyo_integration.is_updating_dashboard = True
            await self.store.save()
            
            # Load metadata once at the beginning
            await self._load_metadata()
            
            # Start from current date and work backwards in 11-month chunks (to stay under API limit)
            end_date = datetime.utcnow()
            total_campaigns_synced = 0
            periods_synced = 0
            
            # Keep going back in time until we get no results
            while True:
                # Calculate start date (11 months before end date to stay under 1 year API limit)
                start_date = end_date - relativedelta(months=11)
                
                logger.info(f"Syncing period {periods_synced + 1}: {start_date.date()} to {end_date.date()}")
                
                # Sync this period
                campaigns_count = await self.sync_time_period(start_date, end_date)
                
                # Log memory after each period
                MemoryMonitor.log_memory_usage(f"Historic Sync Period {periods_synced + 1} Complete")
                
                if campaigns_count == 0:
                    logger.info("No more campaigns found in this period, stopping sync")
                    break
                
                total_campaigns_synced += campaigns_count
                periods_synced += 1
                
                # Move to the previous period with 1 day overlap to ensure no gaps
                end_date = start_date + timedelta(days=1)
                
                # Safety check to prevent infinite loops
                if periods_synced > 10:  # Max 10 years of data
                    logger.warning("Reached maximum sync period limit (10 years)")
                    break
            
            # Update sync completion
            now = datetime.utcnow()
            self.store.klaviyo_integration.campaign_values_last_update = now
            self.store.klaviyo_integration.flow_values_last_update = now
            self.store.klaviyo_integration.is_updating_dashboard = False
            await self.store.save()
            
            # Log final memory usage
            MemoryMonitor.log_memory_usage(f"Historic Sync Complete - Store {self.store.public_id}")
            
            return {
                "status": "completed",
                "total_campaigns_synced": total_campaigns_synced,
                "periods_synced": periods_synced,
                "requests_made": self.requests_made
            }
            
        except Exception as e:
            # Ensure we reset the updating flag on error
            self.store.klaviyo_integration.is_updating_dashboard = False
            await self.store.save()
            
            logger.error(f"Historic sync failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "requests_made": self.requests_made
            }


async def run_historic_sync(klaviyo_public_id: str) -> Dict[str, Any]:
    """Entry point for running historic sync for a klaviyo account"""
    store = await Store.find_one({"klaviyo_integration.public_id": klaviyo_public_id})
    if not store:
        raise ValueError(f"Store not found for klaviyo_public_id: {klaviyo_public_id}")
    
    service = HistoricSyncService(store)
    return await service.run_historic_sync()