import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dateutil.relativedelta import relativedelta
from ..models import Store, FlowStats
from .klaviyo_api import klaviyo_report_post, klaviyo_get_all
from .stats_merger import merge_flow_stats_with_meta
from .flow_definitions import get_flow_definitions, build_message_details_map
from ..utils.memory_monitor import MemoryMonitor
from .flow_stats_aggregator import FlowStatsAggregator

logger = logging.getLogger(__name__)

# Rate limits for flow-series-reports endpoint
# Flow series reports has a limit of 2/minute
BURST_LIMIT = 30  # 30 seconds between requests (2/minute)
STEADY_LIMIT_DELAY = 30  # 30 seconds between requests (2/minute)
DAILY_LIMIT = 225


class FlowHistoricSyncService:
    """Service for performing historic flow data sync from Klaviyo"""
    
    def __init__(self, store: Store):
        self.store = store
        self.requests_made = 0
        self.last_request_time: Optional[datetime] = None
        self.tag_map: Dict[str, str] = {}
        self.flows: List[Dict[str, Any]] = []
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
        """Load tags and flows metadata once at the beginning"""
        if self._metadata_loaded:
            return
            
        logger.info("Loading metadata for flow historic sync")
        
        api_key = self.store.klaviyo_integration.apiKey or self.store.klaviyo_integration.api_key
        
        # Load metadata in parallel
        tags_task = klaviyo_get_all("tags", api_key)
        flows_task = klaviyo_get_all("flows", api_key)
        
        try:
            tags_response, flows_response = await asyncio.gather(
                tags_task, flows_task
            )
        except Exception as e:
            logger.error(f"Error loading metadata: {str(e)}")
            raise
        
        # Store raw responses for merge function
        self.tags = tags_response.get("data", [])
        self.flows = flows_response.get("data", [])
        
        # Build tag map
        self.tag_map = {tag["id"]: tag["attributes"]["name"] for tag in self.tags}
        
        self._metadata_loaded = True
        logger.info(f"Loaded {len(self.tag_map)} tags and {len(self.flows)} flows")
    
    
    async def query_flow_series(
        self, 
        start_date: datetime, 
        end_date: datetime
    ) -> Dict[str, Any]:
        """Query flow series from Klaviyo API"""
        await self._apply_rate_limit()
        
        # Get API key from store
        api_key = self.store.klaviyo_integration.apiKey or self.store.klaviyo_integration.api_key
        if not api_key:
            raise ValueError("No Klaviyo API key available")
        
        payload = {
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
                        "start": start_date.isoformat(),
                        "end": end_date.isoformat()
                    },
                    "interval": "daily"  # Daily aggregation
                }
            }
        }
        
        # Add conversion metric ID if available
        if self.store.klaviyo_integration.conversion_metric_id:
            payload["data"]["attributes"]["conversion_metric_id"] = self.store.klaviyo_integration.conversion_metric_id
        
        try:
            response = await klaviyo_report_post("flow-series-reports", payload, api_key)
            
            # Log the response structure
            if response and "data" in response:
                attributes = response.get("data", {}).get("attributes", {})
                results = attributes.get("results", [])
                
                # Log unique flow IDs in this response
                unique_flows = set()
                for result in results:
                    flow_id = result.get("groupings", {}).get("flow_id")
                    if flow_id:
                        unique_flows.add(flow_id)
                
                logger.info(f"📊 API Response - Unique flows: {len(unique_flows)}, Total results: {len(results)}")
                logger.info(f"📊 Flow IDs in response: {list(unique_flows)}")
                
                # Check if we're getting empty statistics
                if results:
                    sample = results[0]
                    stats = sample.get("statistics", {})
                    non_zero_stats = any(
                        any(v > 0 for v in values) 
                        for values in stats.values() 
                        if isinstance(values, list)
                    )
                    logger.info(f"📊 Sample result has non-zero stats: {non_zero_stats}")
            
            return response
        except Exception as e:
            logger.error(f"Error querying flow series: {str(e)}")
            raise
    
    async def sync_time_period(self, start_date: datetime, end_date: datetime) -> int:
        """Sync flow data for a specific time period"""
        logger.info(f"Syncing flows from {start_date.isoformat()} to {end_date.isoformat()}")
        
        try:
            # Query flow series data for the period
            response = await self.query_flow_series(start_date, end_date)
            
            # Process the flow data from the report results
            results = response.get("data", {}).get("attributes", {}).get("results", [])
            date_times = response.get("data", {}).get("attributes", {}).get("date_times", [])
            
            logger.info(f"📈 Flow series report returned {len(results)} results with {len(date_times)} date points")
            
            if results:
                await self.process_flow_data(results, date_times)
            else:
                logger.warning(f"⚠️ No flow results returned for period {start_date} to {end_date}")
                
            return len(results)
                    
        except Exception as e:
            logger.error(f"Error syncing time period {start_date} to {end_date}: {str(e)}", exc_info=True)
            raise
    
    async def process_flow_data(self, results: List[Dict[str, Any]], date_times: List[str]) -> Dict[str, int]:
        """Process and store flow data from flow-series-report results"""
        logger.info(f"Processing {len(results)} flow results with {len(date_times)} date periods")
        
        # Get unique flow IDs from the results to fetch their definitions
        unique_flow_ids = list(set(
            result["groupings"]["flow_id"] 
            for result in results
        ))
        
        logger.info(f"🔍 Fetching definitions for {len(unique_flow_ids)} flows")
        
        # Get API key
        api_key = self.store.klaviyo_integration.apiKey or self.store.klaviyo_integration.api_key
        
        # Fetch flow definitions with message details
        flow_definitions = await get_flow_definitions(unique_flow_ids, api_key)
        
        # Build message details map from flow definitions
        message_details_map = build_message_details_map(flow_definitions)
        
        logger.info(f"📋 Found message details for {len(message_details_map)} flow messages")
        
        # Use the merge function to enrich the results
        merged_stats = merge_flow_stats_with_meta(
            results,
            self.flows,
            self.tags,
            message_details_map
        )
        
        logger.info(f"📊 Merged {len(merged_stats)} flow stats after enrichment")
        
        # Log unique flow_message_ids in the merged results
        unique_in_results = set()
        unique_combinations = set()
        for stat in merged_stats:
            groupings = stat.get("groupings", {})
            fmid = groupings.get("flow_message_id")
            channel = groupings.get("send_channel")
            if fmid:
                unique_in_results.add(fmid)
                unique_combinations.add((fmid, channel))
        logger.info(f"📊 Unique flow_message_ids in API response: {len(unique_in_results)} - {list(unique_in_results)}")
        logger.info(f"📊 Unique (flow_message_id, channel) combinations: {len(unique_combinations)}")
        
        # If we have fewer unique IDs than results, log the duplicates
        if len(unique_combinations) < len(merged_stats):
            logger.warning(f"⚠️ Found duplicate combinations in API response")
            # Count occurrences
            from collections import Counter
            combinations = [(s.get("groupings", {}).get("flow_message_id"), 
                           s.get("groupings", {}).get("send_channel")) 
                          for s in merged_stats]
            counts = Counter(combinations)
            for combo, count in counts.items():
                if count > 1:
                    logger.warning(f"  - {combo}: {count} occurrences")
        
        # Convert date_times strings to datetime objects
        parsed_date_times = []
        for dt_str in date_times:
            try:
                parsed_date_times.append(datetime.fromisoformat(dt_str.replace("Z", "+00:00")))
            except Exception as e:
                logger.error(f"Error parsing date time {dt_str}: {str(e)}")
                parsed_date_times.append(datetime.utcnow())
        
        # Process flow data - store as single document with arrays
        flow_stats_to_insert = []
        unique_message_ids = set()
        
        for stat_data in merged_stats:
            try:
                # Extract groupings
                groupings = stat_data.get("groupings", {})
                
                # Validate required fields
                flow_id = groupings.get("flow_id")
                flow_message_id = groupings.get("flow_message_id")
                send_channel = groupings.get("send_channel")
                
                # Skip if any required field is missing
                if not flow_id or not flow_message_id or not send_channel:
                    logger.warning(
                        f"Skipping flow stat with missing required fields: "
                        f"flow_id={flow_id}, flow_message_id={flow_message_id}, send_channel={send_channel}"
                    )
                    continue
                
                # Track unique message IDs
                unique_message_ids.add(flow_message_id)
                
                # Get statistics and ensure it's a FlowStatistics object
                from ..models.flow_stats import FlowStatistics
                raw_statistics = stat_data.get("statistics", {})
                
                # Create FlowStatistics object with arrays from the API response
                statistics_obj = FlowStatistics(**raw_statistics) if raw_statistics else FlowStatistics()
                
                # Create flow stat entry with arrays
                klaviyo_public_id = self.store.klaviyo_integration.public_id
                if not klaviyo_public_id:
                    logger.error(f"❌ No klaviyo_public_id found for store {self.store.public_id}")
                    continue
                    
                flow_stat = {
                    "klaviyo_public_id": klaviyo_public_id,
                    "flow_id": flow_id,
                    "flow_message_id": flow_message_id,
                    "send_channel": send_channel,
                    "flow_name": stat_data.get("flow_name"),
                    "flow_message_name": stat_data.get("flow_message_name"),
                    "flow_message_subject": stat_data.get("flow_message_content"),
                    "flow_status": stat_data.get("flow_status"),
                    "flow_archived": stat_data.get("flow_archived", False),
                    "flow_created": stat_data.get("flow_created"),
                    "flow_updated": stat_data.get("flow_updated"),
                    "flow_trigger_type": stat_data.get("flow_trigger_type"),
                    "tag_ids": stat_data.get("tag_ids", []),
                    "tag_names": stat_data.get("tag_names", []),
                    "statistics": statistics_obj,  # Now it's a FlowStatistics object
                    "date_times": parsed_date_times,
                    "created_at": datetime.utcnow(),  # Add unique timestamp
                    "updated_at": datetime.utcnow()
                }
                
                # Create FlowStats object and add to insert list
                flow_stat_obj = FlowStats(**flow_stat)
                flow_stats_to_insert.append(flow_stat_obj)
                
                # Log each stat being prepared for insert
                logger.debug(f"Preparing to insert: flow_message_id={flow_message_id}, channel={send_channel}")
                
            except Exception as e:
                flow_id = stat_data.get("groupings", {}).get("flow_id", "unknown")
                logger.error(f"Error processing flow result for {flow_id}: {str(e)}")
                continue
        
        # Always insert all flow stats as new records - no updates during sync
        # The aggregation step at the end will combine them properly
        created_count = 0
        
        logger.info(f"📊 Flow stats to insert: {len(flow_stats_to_insert)} total")
        logger.info(f"📊 Unique flow message IDs in this period: {len(unique_message_ids)} - {list(unique_message_ids)[:5]}...")
        
        # Batch insert all stats
        if flow_stats_to_insert:
            logger.info(f"🔵 Inserting {len(flow_stats_to_insert)} flow stats")
            try:
                # Insert with ordered=False to continue on errors
                result = await FlowStats.insert_many(flow_stats_to_insert, ordered=False)
                created_count = len(result.inserted_ids) if hasattr(result, 'inserted_ids') else len(flow_stats_to_insert)
                logger.info(f"✅ Successfully inserted {created_count} flow stats (attempted: {len(flow_stats_to_insert)})")
                
                # Check if all were inserted
                if created_count < len(flow_stats_to_insert):
                    logger.warning(f"⚠️ Only {created_count} out of {len(flow_stats_to_insert)} were inserted - possible duplicates?")
                # Log a sample to verify
                if flow_stats_to_insert:
                    sample = flow_stats_to_insert[0]
                    logger.info(f"Sample inserted stat - klaviyo_public_id: {sample.klaviyo_public_id}, flow_message_id: {sample.flow_message_id}")
                
                # Check total count in database
                total_count = await FlowStats.find({"klaviyo_public_id": self.store.klaviyo_integration.public_id}).count()
                logger.info(f"📊 Total flow stats in database for this account: {total_count}")
            except Exception as e:
                logger.error(f"❌ Error inserting flow stats: {str(e)}")
                raise
        
        logger.info(f"Flow processing completed: {created_count} created")
        
        # If no flow stats were processed, log a warning
        if not flow_stats_to_insert:
            logger.warning("No valid flow stats were processed - check if flow data has correct groupings")
        
        return {"created": created_count, "updated": 0}
    
    async def _aggregate_and_replace_flow_stats(self):
        """Aggregate flow stats by flow_message_id and replace existing records"""
        klaviyo_public_id = self.store.klaviyo_integration.public_id
        
        logger.info(f"🔍 Starting aggregation for klaviyo_public_id: {klaviyo_public_id}")
        
        # Get all flow stats for this klaviyo account
        existing_stats = await FlowStats.find({"klaviyo_public_id": klaviyo_public_id}).to_list()
        logger.info(f"📊 Found {len(existing_stats)} flow stat records to aggregate")
        
        if not existing_stats:
            logger.warning("❌ No flow stats found to aggregate - checking if any stats exist at all")
            all_stats = await FlowStats.find({}).to_list()
            logger.warning(f"Total flow stats in database: {len(all_stats)}")
            if all_stats and len(all_stats) > 0:
                logger.warning(f"Sample stat klaviyo_public_id: {all_stats[0].klaviyo_public_id}")
            return
        
        # Convert to dict format for aggregator
        stats_dicts = []
        for stat in existing_stats:
            stat_dict = stat.dict()
            # Handle the statistics object
            if hasattr(stat.statistics, 'dict'):
                stat_dict['statistics'] = stat.statistics.dict()
            stats_dicts.append(stat_dict)
        
        # Use aggregator to combine by flow_message_id
        aggregator = FlowStatsAggregator()
        aggregator.add_flow_stats(stats_dicts)
        aggregated_stats = aggregator.aggregate_by_flow_message_id()
        
        logger.info(f"Aggregated {len(existing_stats)} records into {len(aggregated_stats)} flow message records")
        
        # Log aggregation details
        for agg_stat in aggregated_stats[:3]:  # Log first 3 as samples
            logger.info(f"Sample aggregated stat - flow_message_id: {agg_stat.get('flow_message_id')}, "
                       f"has {len(agg_stat.get('date_times', []))} dates")
        
        # Delete all existing flow stats for this klaviyo account
        try:
            delete_result = await FlowStats.find({"klaviyo_public_id": klaviyo_public_id}).delete()
            logger.info(f"🗑️ Deleted {delete_result.deleted_count} existing flow stat records")
        except Exception as e:
            logger.error(f"❌ Error deleting existing stats: {str(e)}")
            raise
        
        # Insert aggregated stats
        if aggregated_stats:
            logger.info(f"📝 Preparing to insert {len(aggregated_stats)} aggregated stats")
            new_flow_stats = []
            
            try:
                for i, agg_stat in enumerate(aggregated_stats):
                    # Convert statistics dict back to FlowStatistics object
                    from ..models.flow_stats import FlowStatistics
                    statistics_dict = agg_stat.pop('statistics', {})
                    
                    # Log sample data
                    if i == 0:
                        logger.info(f"Sample aggregated stat - flow_message_id: {agg_stat.get('flow_message_id')}, date_times count: {len(agg_stat.get('date_times', []))}")
                        logger.info(f"Sample statistics keys: {list(statistics_dict.keys())[:5]}")
                    
                    statistics_obj = FlowStatistics(**statistics_dict)
                    
                    # Create new FlowStats document
                    flow_stat = FlowStats(
                        statistics=statistics_obj,
                        **agg_stat
                    )
                    new_flow_stats.append(flow_stat)
                
                # Bulk insert
                insert_result = await FlowStats.insert_many(new_flow_stats)
                logger.info(f"✅ Successfully inserted {len(new_flow_stats)} aggregated flow stat records")
                
                # Verify insertion
                verify_count = await FlowStats.find({"klaviyo_public_id": klaviyo_public_id}).count()
                logger.info(f"🔍 Verification: Found {verify_count} flow stats after aggregation")
                
            except Exception as e:
                logger.error(f"❌ Error inserting aggregated stats: {str(e)}")
                logger.error(f"Error details: {type(e).__name__}")
                raise
        else:
            logger.warning("⚠️ No aggregated stats to insert")
    
    async def run_historic_sync(self) -> Dict[str, Any]:
        """Run the complete historic sync process for flows (past 2 years)"""
        try:
            # Log memory at start
            MemoryMonitor.log_memory_usage(f"Flow Historic Sync Start - Store {self.store.public_id}")
            
            # Note: Don't set updating flag here as it's managed by the caller
            
            # Load metadata once at the beginning
            await self._load_metadata()
            
            # Calculate date ranges for 420 days of data
            # We'll fetch in 60-day chunks (maximum allowed for daily interval)
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=420)
            
            total_flows_synced = 0
            periods_synced = 0
            
            # Create list of 60-day periods from oldest to newest
            periods = []
            current_start = start_date
            while current_start < end_date:
                current_end = min(current_start + timedelta(days=60), end_date)
                periods.append((current_start, current_end))
                current_start = current_end
            
            logger.info(f"Will sync {len(periods)} periods to cover 420 days of flow data")
            
            # Sync each period
            for idx, (period_start, period_end) in enumerate(periods):
                days_in_period = (period_end - period_start).days
                logger.info(f"\n{'='*60}")
                logger.info(f"📅 Syncing period {idx + 1}/{len(periods)}: {period_start.date()} to {period_end.date()} ({days_in_period} days, daily)")
                logger.info(f"{'='*60}")
                
                # Sync this period with daily interval
                flows_count = await self.sync_time_period(period_start, period_end)
                
                # Log memory after each period
                MemoryMonitor.log_memory_usage(f"Flow Historic Sync Period {idx + 1} Complete")
                
                total_flows_synced += flows_count
                periods_synced += 1
                
                # Log current status
                logger.info(f"✅ Period {idx + 1} complete. Total flow results synced so far: {total_flows_synced}")
            
            # Log memory before aggregation
            MemoryMonitor.log_memory_usage(f"Flow Historic Sync Complete - Starting Aggregation - Store {self.store.public_id}")
            
            # Summary of sync phase
            logger.info(f"\n{'='*60}")
            logger.info(f"🎆 SYNC PHASE COMPLETE")
            logger.info(f"  - Periods synced: {periods_synced}")
            logger.info(f"  - Total flow results processed: {total_flows_synced}")
            logger.info(f"{'='*60}\n")
            
            # After all periods are synced, aggregate the flow stats by flow_message_id
            logger.info("🔄 Starting flow stats aggregation by flow_message_id")
            
            # Check stats before aggregation
            pre_agg_count = await FlowStats.find({"klaviyo_public_id": self.store.klaviyo_integration.public_id}).count()
            logger.info(f"📊 Flow stats count before aggregation: {pre_agg_count}")
            
            try:
                # Delete existing flow stats and replace with aggregated ones
                await self._aggregate_and_replace_flow_stats()
                logger.info("✅ Flow stats aggregation completed successfully")
                
                # Check stats after aggregation
                post_agg_count = await FlowStats.find({"klaviyo_public_id": self.store.klaviyo_integration.public_id}).count()
                logger.info(f"📊 Flow stats count after aggregation: {post_agg_count}")
                
                # Final summary
                logger.info(f"\n{'='*60}")
                logger.info(f"🎉 AGGREGATION COMPLETE")
                logger.info(f"  - Stats before aggregation: {pre_agg_count}")
                logger.info(f"  - Stats after aggregation: {post_agg_count}")
                logger.info(f"  - Reduction: {pre_agg_count - post_agg_count} duplicate records removed")
                logger.info(f"{'='*60}\n")
                
            except Exception as e:
                logger.error(f"❌ Error during flow stats aggregation: {str(e)}")
                logger.error(f"Aggregation error type: {type(e).__name__}")
                import traceback
                logger.error(f"Aggregation traceback: {traceback.format_exc()}")
                # Don't fail the whole sync if aggregation fails - stats are still saved
            
            # Log final memory usage
            MemoryMonitor.log_memory_usage(f"Flow Historic Sync with Aggregation Complete - Store {self.store.public_id}")
            
            return {
                "status": "completed",
                "total_flows_synced": total_flows_synced,
                "periods_synced": periods_synced,
                "requests_made": self.requests_made
            }
            
        except Exception as e:
            logger.error(f"Flow historic sync failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "requests_made": self.requests_made
            }


async def run_flow_historic_sync(klaviyo_public_id: str) -> Dict[str, Any]:
    """Entry point for running flow historic sync for a klaviyo account"""
    store = await Store.find_one({"klaviyo_integration.public_id": klaviyo_public_id})
    if not store:
        raise ValueError(f"Store not found for klaviyo_public_id: {klaviyo_public_id}")
    
    service = FlowHistoricSyncService(store)
    return await service.run_historic_sync()