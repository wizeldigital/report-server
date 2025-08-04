"""
Flow definitions helper to fetch flow details and build message details map
"""
from typing import List, Dict, Any, Optional
import asyncio
import logging
from datetime import datetime
from .klaviyo_api import klaviyo_get

logger = logging.getLogger(__name__)


async def get_flow_definition(flow_id: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Fetch flow definition with message details
    """
    try:
        response = await klaviyo_get(f"flows/{flow_id}?additional-fields[flow]=definition", api_key)
        return response
    except Exception as e:
        logger.error(f"Error fetching flow definition {flow_id}: {e}")
        return None


async def get_flow_definitions(
    flow_ids: List[str], 
    api_key: str,
    max_burst: int = 3,
    burst_window_ms: int = 1000
) -> List[Dict[str, Any]]:
    """
    Fetch multiple flow definitions with rate limiting
    Respects Klaviyo's rate limits: 3/s burst (max 10 seconds), 60/m steady
    """
    results = []
    errors = []
    
    logger.info(f"🔍 Fetching {len(flow_ids)} flow definitions with rate limiting (3/s burst, 60/m steady)")
    
    # Calculate batches based on burst limits
    batch_size = max_burst
    batch_delay_ms = burst_window_ms  # 1 second between batches
    
    # For steady rate: 60/minute = 1 request per second average
    # But we can burst up to 3/second for max 10 seconds (30 requests)
    # After burst, we need to throttle to 1/second to maintain 60/minute average
    
    total_requests = 0
    start_time = datetime.now()
    
    i = 0
    while i < len(flow_ids):
        batch = flow_ids[i:i + batch_size]
        batch_start_time = datetime.now()
        
        # Check if we're exceeding burst capacity (30 requests in 10 seconds)
        elapsed_seconds = (datetime.now() - start_time).total_seconds()
        burst_limit = min(30, 3 * max(1, elapsed_seconds))  # Max 30 requests in burst
        
        effective_batch_size = len(batch)
        should_throttle = False
        
        if total_requests + len(batch) > burst_limit and elapsed_seconds < 10:
            # We're hitting burst limits, reduce batch size
            effective_batch_size = max(1, int(burst_limit - total_requests))
            should_throttle = True
        elif elapsed_seconds >= 10:
            # After 10 seconds, throttle to steady rate (1/second)
            effective_batch_size = 1
            should_throttle = True
        
        actual_batch = batch[:effective_batch_size]
        
        logger.info(f"📊 Processing batch {i//batch_size + 1}: {len(actual_batch)} flows (total: {total_requests}/{len(flow_ids)})")
        
        # Process current batch
        batch_tasks = []
        for idx, flow_id in enumerate(actual_batch):
            # Small stagger within batch to avoid exact simultaneous requests
            async def fetch_with_delay(fid, delay):
                if delay > 0:
                    await asyncio.sleep(delay)
                return await get_flow_definition(fid, api_key)
            
            batch_tasks.append(fetch_with_delay(flow_id, 0.1 * idx))
        
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        
        for flow_id, result in zip(actual_batch, batch_results):
            if isinstance(result, Exception):
                logger.error(f"Error fetching flow definition {flow_id}: {result}")
                errors.append({"flow_id": flow_id, "error": str(result)})
            elif result:
                results.append(result)
        
        total_requests += len(actual_batch)
        
        # If we didn't process the full batch, add remaining items back to queue
        if effective_batch_size < len(batch):
            remaining = batch[effective_batch_size:]
            flow_ids = flow_ids[:i + effective_batch_size] + remaining + flow_ids[i + batch_size:]
        
        # Calculate delay for next batch
        batch_duration = (datetime.now() - batch_start_time).total_seconds()
        delay_s = 0
        
        if should_throttle or elapsed_seconds >= 10:
            # Steady rate: ensure we don't exceed 60/minute (1 request per second average)
            delay_s = max(1.0, 1.0 - batch_duration)  # At least 1 second between requests
        else:
            # Burst rate: 3/second (can go faster during burst)
            delay_s = max(batch_delay_ms / 1000 - batch_duration, 0)
        
        # Wait before next batch if needed
        if i + batch_size < len(flow_ids) and delay_s > 0:
            logger.info(f"⏳ Waiting {delay_s:.1f}s before next batch...")
            await asyncio.sleep(delay_s)
        
        i += effective_batch_size
    
    if errors:
        logger.warning(f"Failed to fetch {len(errors)} flow definitions: {errors}")
    
    total_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"✅ Fetched {len(results)} flow definitions in {total_time:.1f}s ({len(results)/total_time:.1f} req/s average)")
    
    return results


async def get_flow(flow_id: str, api_key: str) -> Optional[Dict[str, Any]]:
    """Get a single flow by ID with error handling."""
    try:
        response = await klaviyo_get(f"flows/{flow_id}", api_key)
        return response
    except Exception as e:
        logger.error(f"Error fetching flow {flow_id}: {e}")
        return None


def extract_message_details(action: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract message details from a flow action
    """
    if not action or not action.get("data") or not action["data"].get("message"):
        return {}
    
    message = action["data"]["message"]
    
    return {
        "message_name": message.get("name"),
        "message_from_email": message.get("from_email"),
        "message_from_label": message.get("from_label"),
        "message_subject_line": message.get("subject_line"),
        "message_preview_text": message.get("preview_text"),
        "message_template_id": message.get("template_id"),
        "message_transactional": message.get("transactional", False),
        "message_smart_sending_enabled": message.get("smart_sending_enabled", False)
    }


def extract_experiment_details(action: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract experiment details from an AB test action
    """
    if not action or action.get("type") != "ab-test" or not action.get("data"):
        return {"has_experiment": False}
    
    experiment = action["data"].get("current_experiment")
    if not experiment:
        return {"has_experiment": False}
    
    variations = []
    for variation in experiment.get("variations", []):
        variation_data = {
            "variation_id": variation.get("id"),
            "variation_name": variation.get("data", {}).get("message", {}).get("name"),
            "allocation": experiment.get("allocations", {}).get(variation.get("id"), 0),
            "message_name": variation.get("data", {}).get("message", {}).get("name"),
            "message_subject_line": variation.get("data", {}).get("message", {}).get("subject_line"),
            "message_template_id": variation.get("data", {}).get("message", {}).get("template_id")
        }
        variations.append(variation_data)
    
    return {
        "has_experiment": True,
        "experiment_id": experiment.get("id"),
        "experiment_name": experiment.get("name"),
        "experiment_status": action["data"].get("experiment_status"),
        "experiment_winner_metric": experiment.get("winner_metric"),
        "experiment_variations": variations
    }


def find_message_in_flow_definition(flow_definition: Dict[str, Any], flow_message_id: str) -> Dict[str, Any]:
    """
    Find message details for a specific flow_message_id in flow definition
    """
    if not flow_definition or not flow_definition.get("data"):
        return {}
    
    attributes = flow_definition["data"].get("attributes", {})
    if not attributes.get("definition") or not attributes["definition"].get("actions"):
        return {}
    
    actions = attributes["definition"]["actions"]
    
    # Search through all actions to find the matching message ID
    for action in actions:
        # Check main action message
        if action.get("data", {}).get("message", {}).get("id") == flow_message_id:
            message_details = extract_message_details(action)
            experiment_details = extract_experiment_details(action)
            return {**message_details, **experiment_details}
        
        # Check main action in AB test
        if (action.get("type") == "ab-test" and 
            action.get("data", {}).get("main_action", {}).get("data", {}).get("message", {}).get("id") == flow_message_id):
            message_details = extract_message_details(action["data"]["main_action"])
            experiment_details = extract_experiment_details(action)
            return {**message_details, **experiment_details}
        
        # Check experiment variations
        if action.get("type") == "ab-test" and action.get("data", {}).get("current_experiment", {}).get("variations"):
            for variation in action["data"]["current_experiment"]["variations"]:
                if variation.get("data", {}).get("message", {}).get("id") == flow_message_id:
                    message_details = extract_message_details(variation)
                    experiment_details = extract_experiment_details(action)
                    return {**message_details, **experiment_details}
    
    return {}


def build_message_details_map(flow_definitions: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Build a lookup map of flow message details from flow definitions
    """
    message_map = {}
    
    for flow_def in flow_definitions:
        if not flow_def or not flow_def.get("data"):
            continue
        
        attributes = flow_def["data"].get("attributes", {})
        if not attributes.get("definition") or not attributes["definition"].get("actions"):
            continue
        
        actions = attributes["definition"]["actions"]
        
        # Ensure actions is a list and not None
        if not isinstance(actions, list):
            logger.warning(f"Actions is not a list: {type(actions)}")
            continue
            
        for action in actions:
            # Skip None actions or non-dict actions
            if action is None or not isinstance(action, dict):
                logger.debug(f"Skipping invalid action: {action}")
                continue
                
            # Process main action messages
            action_data = action.get("data", {})
            if not isinstance(action_data, dict):
                continue
                
            message_data = action_data.get("message", {})
            if message_data.get("id"):
                message_id = message_data["id"]
                message_details = extract_message_details(action)
                experiment_details = extract_experiment_details(action)
                message_map[message_id] = {**message_details, **experiment_details}
            
            # Process main action in AB test
            if action.get("type") == "ab-test":
                main_action = action_data.get("main_action", {})
                if isinstance(main_action, dict):
                    main_action_data = main_action.get("data", {})
                    if isinstance(main_action_data, dict):
                        main_message = main_action_data.get("message", {})
                        if isinstance(main_message, dict) and main_message.get("id"):
                            message_id = main_message["id"]
                            message_details = extract_message_details(main_action)
                            experiment_details = extract_experiment_details(action)
                            message_map[message_id] = {**message_details, **experiment_details}
                
                # Process experiment variations
                experiment = action_data.get("current_experiment", {})
                if isinstance(experiment, dict):
                    variations = experiment.get("variations", [])
                    if isinstance(variations, list):
                        for variation in variations:
                            if variation is None or not isinstance(variation, dict):
                                continue
                            var_data = variation.get("data", {})
                            if isinstance(var_data, dict):
                                var_message = var_data.get("message", {})
                                if isinstance(var_message, dict) and var_message.get("id"):
                                    message_id = var_message["id"]
                                    message_details = extract_message_details(variation)
                                    experiment_details = extract_experiment_details(action)
                                    message_map[message_id] = {**message_details, **experiment_details}
    
    return message_map