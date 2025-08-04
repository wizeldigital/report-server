from fastapi import APIRouter, Depends, HTTPException, Query, Body
from typing import Optional, Dict, Any, List
from datetime import datetime
from bson import ObjectId
from ....models import CampaignStats, Store, User
from ....middleware.auth import validate_private_key
from ....core.permissions import has_permission, VIEW_ANALYTICS, MANAGE_CAMPAIGNS
import asyncio

router = APIRouter()


@router.get("/")
async def search_campaign_stats(
    user_id: str = Query(..., description="User ID"),
    store_public_id: Optional[str] = Query(None),
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
    
    if store_public_id:
        query["store_public_id"] = store_public_id
    
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
        # Convert store_id string to ObjectId if needed
        if "store_id" in campaign_stat and isinstance(campaign_stat["store_id"], str):
            campaign_stat["store_id"] = ObjectId(campaign_stat["store_id"])
        
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
        
        # Check user access
        user = await User.get(user_id)
        if not user:
            raise HTTPException(status_code=403, detail="User not found")
        
        has_access = False
        for store_access in user.stores:
            if store_access.store_public_id == stat.store_public_id:
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
    store_id: str = Body(..., embed=True),
    _: bool = Depends(validate_private_key)
):
    """Trigger stats synchronization for a store"""
    try:
        from ....services.stats_sync import sync_store_stats as sync_service
        
        store = await Store.find_by_id_or_public_id_and_update(
            store_id,
            {"is_updating_dashboard": True}
        )
        
        if not store:
            raise HTTPException(status_code=404, detail="Store not found")
        
        # Run sync in background
        asyncio.create_task(sync_service(store.id))
        
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
    store_ids: List[str] = Body(..., embed=True),
    _: bool = Depends(validate_private_key)
):
    """Trigger sync for multiple stores"""
    try:
        from ....services.stats_sync import sync_store_stats as sync_service
        
        results = []
        tasks = []
        
        for store_id in store_ids:
            store = await Store.find_by_id_or_public_id_and_update(
                store_id,
                {"is_updating_dashboard": True}
            )
            
            if store:
                task = asyncio.create_task(sync_service(store.id))
                tasks.append(task)
                results.append({
                    "store_id": str(store.id),
                    "status": "sync_started"
                })
            else:
                results.append({
                    "store_id": store_id,
                    "status": "store_not_found"
                })
        
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))