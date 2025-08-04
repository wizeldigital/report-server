from fastapi import APIRouter, Depends, HTTPException, Query, Body
from typing import Optional, Dict, Any, List
from datetime import datetime
from bson import ObjectId
from ....models import FlowStats, User
from ....middleware.auth import validate_private_key
from ....core.permissions import has_permission, VIEW_ANALYTICS

router = APIRouter()


@router.get("/")
async def search_flow_stats(
    user_id: str = Query(..., description="User ID"),
    store_public_id: Optional[str] = Query(None),
    flow_id: Optional[str] = Query(None),
    flow_message_id: Optional[str] = Query(None),
    send_channel: Optional[str] = Query(None),
    tag_ids: Optional[List[str]] = Query(None, alias="tagIds"),
    start_date: Optional[datetime] = Query(None, alias="startDate"),
    end_date: Optional[datetime] = Query(None, alias="endDate"),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    _: bool = Depends(validate_private_key)
):
    """Search flow statistics with access control"""
    query = {}
    
    if store_public_id:
        query["store_public_id"] = store_public_id
    
    if flow_id:
        query["flow_id"] = flow_id
    
    if flow_message_id:
        query["flow_message_id"] = flow_message_id
    
    if send_channel:
        query["send_channel"] = send_channel
    
    if tag_ids:
        query["tagIds"] = {"$in": tag_ids}
    
    if start_date or end_date:
        date_query = {}
        if start_date:
            date_query["$gte"] = start_date
        if end_date:
            date_query["$lte"] = end_date
        query["date_times"] = {"$elemMatch": date_query}
    
    options = {
        "page": page,
        "limit": limit
    }
    
    result = await FlowStats.search_with_access_control(user_id, query, options)
    return result


@router.post("/")
async def create_flow_stat(
    flow_stat: Dict[str, Any] = Body(...),
    _: bool = Depends(validate_private_key)
):
    """Create a new flow statistic"""
    try:
        # Convert store string to ObjectId if needed
        if "store" in flow_stat and isinstance(flow_stat["store"], str):
            flow_stat["store"] = ObjectId(flow_stat["store"])
        
        stat = FlowStats(**flow_stat)
        await stat.save()
        return stat
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{stat_id}")
async def get_flow_stat_by_id(
    stat_id: str,
    user_id: str = Query(...),
    _: bool = Depends(validate_private_key)
):
    """Get a specific flow statistic by ID"""
    try:
        stat = await FlowStats.get(stat_id)
        if not stat:
            raise HTTPException(status_code=404, detail="Flow statistic not found")
        
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


@router.post("/bulk")
async def bulk_upsert_flow_stats(
    flow_stats: List[Dict[str, Any]] = Body(...),
    _: bool = Depends(validate_private_key)
):
    """Bulk upsert flow statistics"""
    try:
        # Convert store strings to ObjectIds if needed
        for stat in flow_stats:
            if "store" in stat and isinstance(stat["store"], str):
                stat["store"] = ObjectId(stat["store"])
        
        result = await FlowStats.bulk_upsert_flow_stats(flow_stats)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))