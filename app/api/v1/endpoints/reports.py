from fastapi import APIRouter, Depends, HTTPException, Query, Body
from typing import Optional, Dict, Any, List
from datetime import datetime
from bson import ObjectId
from ....models import CampaignStats, Store, User
from ....middleware.auth import validate_private_key
from ....core.permissions import has_permission, VIEW_ANALYTICS
import asyncio
import logging

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/")
async def search_campaign_stats(
    user_id: str = Query(..., description="User ID"),
    klaviyo_public_id: Optional[str] = Query(None),
    campaign_id: Optional[str] = Query(None),
    send_channel: Optional[str] = Query(None),
    tag_ids: Optional[List[str]] = Query(None, alias="tagIds"),
    start_date: Optional[datetime] = Query(None, alias="startDate"),
    end_date: Optional[datetime] = Query(None, alias="endDate"),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    _: bool = Depends(validate_private_key)
):
    """Search campaign statistics with access control"""
    query = {}
    
    if klaviyo_public_id:
        query["klaviyo_public_id"] = klaviyo_public_id
    
    if campaign_id:
        query["groupings.campaign_id"] = campaign_id
    
    if send_channel:
        query["groupings.send_channel"] = send_channel
    
    if tag_ids:
        query["tagIds"] = {"$in": tag_ids}
    
    if start_date or end_date:
        date_query = {}
        if start_date:
            date_query["$gte"] = start_date
        if end_date:
            date_query["$lte"] = end_date
        query["send_time"] = date_query
    
    options = {
        "page": page,
        "limit": limit
    }
    
    result = await CampaignStats.search_with_access_control(user_id, query, options)
    return result


@router.post("/")
async def create_campaign_stat(
    campaign_stat: Dict[str, Any] = Body(...),
    _: bool = Depends(validate_private_key)
):
    """Create a new campaign statistic"""
    try:
        # Ensure klaviyo_public_id is provided
        if "klaviyo_public_id" not in campaign_stat:
            raise HTTPException(status_code=400, detail="klaviyo_public_id is required")
        
        stat = CampaignStats(**campaign_stat)
        await stat.save()
        return stat
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{stat_id}")
async def get_campaign_stat_by_id(
    stat_id: str,
    user_id: str = Query(...),
    _: bool = Depends(validate_private_key)
):
    """Get a specific campaign statistic by ID"""
    try:
        stat = await CampaignStats.get(stat_id)
        if not stat:
            raise HTTPException(status_code=404, detail="Campaign statistic not found")
        
        # Check user access by mapping klaviyo_public_id to store
        user = await User.get(user_id)
        if not user:
            raise HTTPException(status_code=403, detail="User not found")
        
        # Find store with this klaviyo_public_id
        store = await Store.find_one({"klaviyo_integration.public_id": stat.klaviyo_public_id})
        if not store:
            raise HTTPException(status_code=404, detail="Store not found for this stat")
        
        has_access = False
        for store_access in user.stores:
            if store_access.store_public_id == store.public_id:
                if has_permission(store_access.permissions, VIEW_ANALYTICS):
                    has_access = True
                    break
        
        if not has_access:
            raise HTTPException(status_code=403, detail="Access denied")
        
        return stat
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/sync")
async def sync_store_stats(
    data: Dict[str, Any] = Body(...),
    _: bool = Depends(validate_private_key)
):
    """Trigger stats synchronization for a store"""
    try:
        from ....services.stats_sync import sync_store_stats as sync_service
        
        # Log the incoming data
        logger.info(f"Sync request received with data: {data}")
        
        # Get both IDs from request
        klaviyo_public_id = data.get("klaviyo_public_id")
        store_public_id = data.get("store_public_id")
        logger.info(f"Klaviyo public ID: {klaviyo_public_id}, Store public ID: {store_public_id}")
        
        if not klaviyo_public_id or not store_public_id:
            raise HTTPException(
                status_code=400, 
                detail="Both klaviyo_public_id and store_public_id are required"
            )
        
        logger.info(f"Looking up store with store_public_id: {store_public_id}")
        store = await Store.find_one({"public_id": store_public_id})
        if store:
            # Verify klaviyo_public_id matches
            if store.klaviyo_integration.public_id != klaviyo_public_id:
                raise HTTPException(
                    status_code=400,
                    detail="klaviyo_public_id does not match store's klaviyo integration"
                )
            store.klaviyo_integration.is_updating_dashboard = True
            await store.save()
        
        if not store:
            raise HTTPException(status_code=404, detail="Store not found")
        
        # Run sync in background
        asyncio.create_task(sync_service(klaviyo_public_id))
        
        return {"message": "Sync started", "store_id": str(store.id)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/sync/status/{store_id}")
async def get_sync_status(
    store_id: str,
    _: bool = Depends(validate_private_key)
):
    """Get sync status for a store"""
    try:
        store = await Store.find_by_id_or_public_id_and_update(store_id, {})
        if not store:
            raise HTTPException(status_code=404, detail="Store not found")
        
        return {
            "store_id": str(store.id),
            "is_updating": store.is_updating_dashboard,
            "last_sync": store.last_dashboard_sync
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/sync/bulk")
async def sync_bulk_stores(
    data: Dict[str, Any] = Body(...),
    _: bool = Depends(validate_private_key)
):
    """Trigger sync for multiple stores"""
    try:
        from ....services.stats_sync import sync_store_stats as sync_service
        
        # Get klaviyo_public_ids from request
        klaviyo_public_ids = data.get("klaviyo_public_ids", [])
        if not klaviyo_public_ids:
            raise HTTPException(status_code=400, detail="klaviyo_public_ids array is required")
        
        results = []
        tasks = []
        
        for klaviyo_public_id in klaviyo_public_ids:
            store = await Store.find_one({"klaviyo_integration.public_id": klaviyo_public_id})
            if store:
                store.klaviyo_integration.is_updating_dashboard = True
                await store.save()
                
                task = asyncio.create_task(sync_service(klaviyo_public_id))
                tasks.append(task)
                results.append({
                    "klaviyo_public_id": klaviyo_public_id,
                    "status": "sync_started"
                })
            else:
                results.append({
                    "klaviyo_public_id": klaviyo_public_id,
                    "status": "store_not_found"
                })
        
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/historic-sync")
async def historic_sync(
    data: Dict[str, Any] = Body(...),
    _: bool = Depends(validate_private_key)
):
    """Trigger historic sync from Klaviyo for a store"""
    try:
        from ....services.historic_sync import run_historic_sync
        
        # Log the incoming data
        logger.info(f"Historic sync request received with data: {data}")
        
        # Get both IDs from request
        klaviyo_public_id = data.get("klaviyo_public_id")
        store_public_id = data.get("store_public_id")
        logger.info(f"Klaviyo public ID: {klaviyo_public_id}, Store public ID: {store_public_id}")
        
        if not klaviyo_public_id or not store_public_id:
            raise HTTPException(
                status_code=400, 
                detail="Both klaviyo_public_id and store_public_id are required"
            )
        
        logger.info(f"Looking up store with store_public_id: {store_public_id}")
        store = await Store.find_one({"public_id": store_public_id})
        if store:
            # Verify klaviyo_public_id matches
            if store.klaviyo_integration.public_id != klaviyo_public_id:
                raise HTTPException(
                    status_code=400,
                    detail="klaviyo_public_id does not match store's klaviyo integration"
                )
            store.klaviyo_integration.is_updating_dashboard = True
            await store.save()
        
        if not store:
            raise HTTPException(status_code=404, detail="Store not found")
        
        # Check if Klaviyo integration is configured
        api_key = store.klaviyo_integration.apiKey or store.klaviyo_integration.api_key
        if not api_key and not store.klaviyo_integration.oauth_token:
            raise HTTPException(
                status_code=400, 
                detail="Store does not have Klaviyo integration configured"
            )
        
        # Run historic sync in background
        asyncio.create_task(run_historic_sync(klaviyo_public_id))
        
        return {
            "message": "Historic sync started",
            "store_id": str(store.id),
            "status": "initiated"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/combined-historic-sync")
async def combined_historic_sync(
    data: Dict[str, Any] = Body(...),
    _: bool = Depends(validate_private_key)
):
    """Trigger both campaign and flow historic sync concurrently"""
    try:
        from ....services.historic_sync import run_historic_sync
        from ....services.flow_historic_sync import run_flow_historic_sync
        
        # Log the incoming data
        logger.info(f"Combined historic sync request received with data: {data}")
        
        # Get both IDs from request
        klaviyo_public_id = data.get("klaviyo_public_id")
        store_public_id = data.get("store_public_id")
        logger.info(f"Klaviyo public ID: {klaviyo_public_id}, Store public ID: {store_public_id}")
        
        if not klaviyo_public_id or not store_public_id:
            raise HTTPException(
                status_code=400, 
                detail="Both klaviyo_public_id and store_public_id are required"
            )
        
        logger.info(f"Looking up store with store_public_id: {store_public_id}")
        store = await Store.find_one({"public_id": store_public_id})
        if store:
            # Verify klaviyo_public_id matches
            if store.klaviyo_integration.public_id != klaviyo_public_id:
                raise HTTPException(
                    status_code=400,
                    detail="klaviyo_public_id does not match store's klaviyo integration"
                )
            store.klaviyo_integration.is_updating_dashboard = True
            await store.save()
        
        if not store:
            raise HTTPException(status_code=404, detail="Store not found")
        
        # Check if Klaviyo integration is configured
        api_key = store.klaviyo_integration.apiKey or store.klaviyo_integration.api_key
        if not api_key and not store.klaviyo_integration.oauth_token:
            raise HTTPException(
                status_code=400, 
                detail="Store does not have Klaviyo integration configured"
            )
        
        # Run both syncs concurrently in background
        async def run_combined_sync():
            try:
                # Run campaign and flow syncs concurrently
                campaign_task = asyncio.create_task(run_historic_sync(klaviyo_public_id))
                flow_task = asyncio.create_task(run_flow_historic_sync(klaviyo_public_id))
                
                # Wait for both to complete
                campaign_result, flow_result = await asyncio.gather(
                    campaign_task, 
                    flow_task,
                    return_exceptions=True
                )
                
                # Get fresh store instance to update
                fresh_store = await Store.get(store.id)
                if fresh_store:
                    # Update sync completion regardless of individual results
                    fresh_store.klaviyo_integration.is_updating_dashboard = False
                    if not isinstance(campaign_result, Exception):
                        fresh_store.klaviyo_integration.campaign_values_last_update = datetime.utcnow()
                    if not isinstance(flow_result, Exception):
                        fresh_store.klaviyo_integration.flow_values_last_update = datetime.utcnow()
                    await fresh_store.save()
                
                logger.info(f"Combined historic sync completed for store {store.public_id}")
                logger.info(f"Campaign sync result: {campaign_result if not isinstance(campaign_result, Exception) else str(campaign_result)}")
                logger.info(f"Flow sync result: {flow_result if not isinstance(flow_result, Exception) else str(flow_result)}")
                
            except Exception as e:
                logger.error(f"Combined historic sync failed: {str(e)}")
                # Get fresh store instance to update
                fresh_store = await Store.get(store.id)
                if fresh_store:
                    fresh_store.klaviyo_integration.is_updating_dashboard = False
                    await fresh_store.save()
        
        # Start combined sync in background
        asyncio.create_task(run_combined_sync())
        
        return {
            "message": "Combined historic sync started (campaigns + flows)",
            "store_id": str(store.id),
            "status": "initiated",
            "details": {
                "campaigns": "Syncing all historic campaign data",
                "flows": "Syncing 2 years of weekly flow data"
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/flow-historic-sync")
async def flow_historic_sync(
    data: Dict[str, Any] = Body(...),
    _: bool = Depends(validate_private_key)
):
    """Trigger historic sync for flows (past 2 years of weekly data)"""
    try:
        from ....services.flow_historic_sync import run_flow_historic_sync
        
        # Log the incoming data
        logger.info(f"Flow historic sync request received with data: {data}")
        
        # Get both IDs from request
        klaviyo_public_id = data.get("klaviyo_public_id")
        store_public_id = data.get("store_public_id")
        logger.info(f"Klaviyo public ID: {klaviyo_public_id}, Store public ID: {store_public_id}")
        
        if not klaviyo_public_id or not store_public_id:
            raise HTTPException(
                status_code=400, 
                detail="Both klaviyo_public_id and store_public_id are required"
            )
        
        logger.info(f"Looking up store with store_public_id: {store_public_id}")
        store = await Store.find_one({"public_id": store_public_id})
        if store:
            # Verify klaviyo_public_id matches
            if store.klaviyo_integration.public_id != klaviyo_public_id:
                raise HTTPException(
                    status_code=400,
                    detail="klaviyo_public_id does not match store's klaviyo integration"
                )
            store.klaviyo_integration.is_updating_dashboard = True
            await store.save()
        
        if not store:
            raise HTTPException(status_code=404, detail="Store not found")
        
        # Check if Klaviyo integration is configured
        api_key = store.klaviyo_integration.apiKey or store.klaviyo_integration.api_key
        if not api_key and not store.klaviyo_integration.oauth_token:
            raise HTTPException(
                status_code=400, 
                detail="Store does not have Klaviyo integration configured"
            )
        
        # Run flow historic sync in background
        async def run_flow_sync_with_update():
            try:
                result = await run_flow_historic_sync(klaviyo_public_id)
                
                # Update store sync timestamp if successful
                if result.get("status") == "completed":
                    fresh_store = await Store.get(store.id)
                    if fresh_store:
                        fresh_store.klaviyo_integration.flow_values_last_update = datetime.utcnow()
                        fresh_store.klaviyo_integration.is_updating_dashboard = False
                        await fresh_store.save()
                
                logger.info(f"Flow historic sync completed for store {store.public_id}: {result}")
            except Exception as e:
                logger.error(f"Flow historic sync failed for store {store.public_id}: {str(e)}")
                # Reset updating flag on error
                fresh_store = await Store.get(store.id)
                if fresh_store:
                    fresh_store.klaviyo_integration.is_updating_dashboard = False
                    await fresh_store.save()
        
        asyncio.create_task(run_flow_sync_with_update())
        
        return {
            "message": "Flow historic sync started (2 years of weekly data)",
            "store_id": str(store.id),
            "status": "initiated"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/flow-stats")
async def get_flow_stats(
    klaviyo_public_id: str = Query(..., description="Klaviyo public ID"),
    user_id: str = Query(..., description="User ID for access control"),
    _: bool = Depends(validate_private_key)
):
    """Get flow statistics (already aggregated by flow_id with 420 days of data)"""
    try:
        from ....models import FlowStats
        
        # Verify user has access to this klaviyo account
        user = await User.get(user_id)
        if not user:
            raise HTTPException(status_code=403, detail="User not found")
        
        # Find store with this klaviyo_public_id
        store = await Store.find_one({"klaviyo_integration.public_id": klaviyo_public_id})
        if not store:
            raise HTTPException(status_code=404, detail="Store not found for this klaviyo account")
        
        # Check user access
        has_access = False
        for store_access in user.stores:
            if store_access.store_public_id == store.public_id:
                if has_permission(store_access.permissions, VIEW_ANALYTICS):
                    has_access = True
                    break
        
        if not has_access:
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Get flow stats (already aggregated during historic sync)
        flow_stats = await FlowStats.find({"klaviyo_public_id": klaviyo_public_id}).to_list()
        
        # Convert to dict format with proper serialization
        flows = []
        for stat in flow_stats:
            flow_dict = stat.dict()
            # Ensure statistics is properly serialized
            if hasattr(stat.statistics, 'dict'):
                flow_dict['statistics'] = stat.statistics.dict()
            flows.append(flow_dict)
        
        return {
            "klaviyo_public_id": klaviyo_public_id,
            "total_flows": len(flows),
            "flows": flows
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting flow stats: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
