"""
Flow Stats Aggregator - Combines flow statistics by flow_id with 420 days of data
"""
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class FlowStatsAggregator:
    """Aggregates flow statistics by flow_message_id, ensuring 420 days of complete data"""
    
    DAYS_OF_DATA = 420
    
    def __init__(self):
        self.message_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
    def add_flow_stats(self, flow_stats: List[Dict[str, Any]]):
        """Group flow stats by flow_message_id"""
        for stat in flow_stats:
            flow_message_id = stat.get("flow_message_id")
            if flow_message_id:
                self.message_groups[flow_message_id].append(stat)
    
    def _get_date_range(self) -> List[datetime]:
        """Generate list of dates for past 420 days"""
        end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        dates = []
        
        for i in range(self.DAYS_OF_DATA):
            date = end_date - timedelta(days=i)
            dates.append(date)
        
        # Return in chronological order (oldest first)
        return list(reversed(dates))
    
    def _create_empty_statistics(self) -> Dict[str, List[float]]:
        """Create statistics object with 420 zeros for each metric"""
        metrics = [
            "average_order_value", "bounce_rate", "bounced", "bounced_or_failed",
            "bounced_or_failed_rate", "click_rate", "click_to_open_rate", "clicks",
            "clicks_unique", "conversion_rate", "conversion_uniques", "conversion_value",
            "conversions", "delivered", "delivery_rate", "failed", "failed_rate",
            "open_rate", "opens", "opens_unique", "recipients", "revenue_per_recipient",
            "spam_complaint_rate", "spam_complaints", "unsubscribe_rate",
            "unsubscribe_uniques", "unsubscribes"
        ]
        
        return {metric: [0] * self.DAYS_OF_DATA for metric in metrics}
    
    def _merge_statistics(self, stats_list: List[Dict[str, Any]], date_range: List[datetime]) -> Dict[str, Any]:
        """Merge multiple flow stats into single record with 420 days of data"""
        # Create base structure with zeros
        merged_stats = self._create_empty_statistics()
        
        # Create date to index mapping for the 420-day range
        date_to_index = {date.date(): i for i, date in enumerate(date_range)}
        
        # Process each stat record
        for stat in stats_list:
            stat_dates = stat.get("date_times", [])
            statistics = stat.get("statistics", {})
            
            # Map each date's data to the correct position in the 420-day array
            for i, date in enumerate(stat_dates):
                # Handle both datetime objects and date strings
                if isinstance(date, str):
                    try:
                        date_obj = datetime.fromisoformat(date.replace("Z", "+00:00")).date()
                    except:
                        continue
                elif isinstance(date, dict) and "$date" in date:
                    try:
                        date_obj = datetime.fromisoformat(date["$date"].replace("Z", "+00:00")).date()
                    except:
                        continue
                else:
                    date_obj = date.date() if hasattr(date, 'date') else date
                
                # Find the index in our 420-day range
                idx = date_to_index.get(date_obj)
                if idx is not None and i < len(list(statistics.values())[0]) if statistics else 0:
                    # Copy data from this position to the merged array
                    for metric, values in statistics.items():
                        if isinstance(values, list) and i < len(values) and metric in merged_stats:
                            merged_stats[metric][idx] = values[i]
        
        return merged_stats
    
    def aggregate_by_flow_message_id(self) -> List[Dict[str, Any]]:
        """Aggregate all flow stats by flow_message_id with 420 days of data"""
        date_range = self._get_date_range()
        aggregated_messages = []
        
        for flow_message_id, stats_list in self.message_groups.items():
            if not stats_list:
                continue
            
            # Use the first stat as base for metadata
            base_stat = stats_list[0]
            
            # Merge all statistics for this flow message
            merged_statistics = self._merge_statistics(stats_list, date_range)
            
            # Combine all channels (should typically be just one per message)
            all_channels = set()
            for stat in stats_list:
                all_channels.add(stat.get("send_channel", "email"))
            
            # Create the aggregated flow message stat
            aggregated_message = {
                "klaviyo_public_id": base_stat.get("klaviyo_public_id"),
                "flow_id": base_stat.get("flow_id"),
                "flow_message_id": flow_message_id,
                "send_channel": list(all_channels)[0] if len(all_channels) == 1 else "multiple",  # Usually just one channel per message
                "flow_name": base_stat.get("flow_name"),
                "flow_message_name": base_stat.get("flow_message_name"),
                "flow_message_subject": base_stat.get("flow_message_subject"),
                "flow_status": base_stat.get("flow_status"),
                "flow_archived": base_stat.get("flow_archived", False),
                "flow_created": base_stat.get("flow_created"),
                "flow_updated": base_stat.get("flow_updated"),
                "flow_trigger_type": base_stat.get("flow_trigger_type"),
                "tag_ids": base_stat.get("tag_ids", []),
                "tag_names": base_stat.get("tag_names", []),
                "statistics": merged_statistics,
                "date_times": date_range,  # Always 420 dates
                "last_updated": datetime.utcnow(),
                "created_at": base_stat.get("created_at", datetime.utcnow()),
                "updated_at": datetime.utcnow()
            }
            
            aggregated_messages.append(aggregated_message)
            
        logger.info(f"Aggregated {len(self.message_groups)} flow messages into {len(aggregated_messages)} combined records")
        return aggregated_messages
    
    @staticmethod
    async def aggregate_flow_stats_for_klaviyo(klaviyo_public_id: str) -> List[Dict[str, Any]]:
        """Convenience method to aggregate all flow stats for a klaviyo account"""
        from ..models import FlowStats
        
        # Fetch all flow stats for this klaviyo account
        flow_stats = await FlowStats.find({"klaviyo_public_id": klaviyo_public_id}).to_list()
        
        # Convert to dict format
        stats_dicts = []
        for stat in flow_stats:
            stat_dict = stat.dict()
            # Handle the statistics object
            if hasattr(stat.statistics, 'dict'):
                stat_dict['statistics'] = stat.statistics.dict()
            stats_dicts.append(stat_dict)
        
        # Aggregate
        aggregator = FlowStatsAggregator()
        aggregator.add_flow_stats(stats_dicts)
        
        return aggregator.aggregate_by_flow_message_id()