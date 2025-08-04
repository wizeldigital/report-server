from datetime import datetime
from typing import Optional, List, Dict, Any
from beanie import Document, Link, Indexed
from pydantic import Field, BaseModel
from .store import Store
from .user import User


class FlowStatistics(BaseModel):
    opens: List[float] = Field(default_factory=list)
    open_rate: List[float] = Field(default_factory=list)
    bounced: List[float] = Field(default_factory=list)
    clicks: List[float] = Field(default_factory=list)
    clicks_unique: List[float] = Field(default_factory=list)
    click_rate: List[float] = Field(default_factory=list)
    delivered: List[float] = Field(default_factory=list)
    bounced_or_failed: List[float] = Field(default_factory=list)
    bounced_or_failed_rate: List[float] = Field(default_factory=list)
    delivery_rate: List[float] = Field(default_factory=list)
    failed: List[float] = Field(default_factory=list)
    failed_rate: List[float] = Field(default_factory=list)
    recipients: List[float] = Field(default_factory=list)
    opens_unique: List[float] = Field(default_factory=list)
    bounce_rate: List[float] = Field(default_factory=list)
    unsubscribe_rate: List[float] = Field(default_factory=list)
    unsubscribe_uniques: List[float] = Field(default_factory=list)
    unsubscribes: List[float] = Field(default_factory=list)
    spam_complaint_rate: List[float] = Field(default_factory=list)
    spam_complaints: List[float] = Field(default_factory=list)
    click_to_open_rate: List[float] = Field(default_factory=list)
    conversions: List[float] = Field(default_factory=list)
    conversion_uniques: List[float] = Field(default_factory=list)
    conversion_value: List[float] = Field(default_factory=list)
    conversion_rate: List[float] = Field(default_factory=list)
    average_order_value: List[float] = Field(default_factory=list)
    revenue_per_recipient: List[float] = Field(default_factory=list)


class ExperimentVariation(BaseModel):
    variation_id: str
    variation_name: str
    allocation: float
    message_name: str
    message_subject_line: str
    message_template_id: str


class FlowRecentStats(Document):
    # Klaviyo public ID as primary identifier
    klaviyo_public_id: Indexed(str)
    flow_id: Indexed(str)
    flow_message_id: Indexed(str)
    flow_name: Optional[str] = None
    flow_message_name: Optional[str] = None
    flow_message_subject: Optional[str] = None
    flow_status: Optional[str] = None
    flow_archived: bool = False
    flow_created: Optional[datetime] = None
    flow_updated: Optional[datetime] = None
    flow_trigger_type: Optional[str] = None
    send_channel: str
    tag_ids: List[str] = Field(default_factory=list, alias="tagIds")
    tag_names: List[str] = Field(default_factory=list, alias="tagNames")
    statistics: FlowStatistics = Field(default_factory=FlowStatistics)
    date_times: List[datetime] = Field(default_factory=list)
    message_name: Optional[str] = None
    message_from_email: Optional[str] = None
    message_from_label: Optional[str] = None
    message_subject_line: Optional[str] = None
    message_preview_text: Optional[str] = None
    message_template_id: Optional[str] = None
    message_transactional: Optional[bool] = None
    message_smart_sending_enabled: Optional[bool] = None
    has_experiment: bool = False
    experiment_id: Optional[str] = None
    experiment_name: Optional[str] = None
    experiment_status: Optional[str] = None
    experiment_winner_metric: Optional[str] = None
    experiment_variations: List[ExperimentVariation] = Field(default_factory=list)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    created_at: datetime = Field(default_factory=datetime.utcnow, alias="createdAt")
    updated_at: datetime = Field(default_factory=datetime.utcnow, alias="updatedAt")

    class Settings:
        name = "flowrecentstats"
        use_state_management = True
        indexes = [
            [("klaviyo_public_id", 1), ("flow_id", 1), ("flow_message_id", 1), ("send_channel", 1)],
            "date_times",
            [("klaviyo_public_id", 1), ("flow_id", 1)],
            "klaviyo_public_id",
            "flow_id",
            "flow_message_id"
        ]

    @classmethod
    async def search_with_access_control(
        cls,
        user_id: str,
        query: dict = None,
        options: dict = None
    ):
        from ..core.permissions import has_permission, VIEW_ANALYTICS
        
        user = await User.get(user_id)
        if not user:
            return {"docs": [], "totalDocs": 0, "totalPages": 0, "page": 1}
        
        # Map accessible stores to their Klaviyo public IDs
        from ..models import Store
        accessible_klaviyo_ids = []
        for store_access in user.stores:
            if has_permission(store_access.permissions, VIEW_ANALYTICS):
                # Get store to find its klaviyo_public_id
                store = await Store.find_one({"public_id": store_access.store_public_id})
                if store and store.klaviyo_integration.public_id:
                    accessible_klaviyo_ids.append(store.klaviyo_integration.public_id)
        
        if not accessible_klaviyo_ids:
            return {"docs": [], "totalDocs": 0, "totalPages": 0, "page": 1}
        
        search_query = query or {}
        search_query["klaviyo_public_id"] = {"$in": accessible_klaviyo_ids}
        
        page = options.get("page", 1) if options else 1
        limit = options.get("limit", 10) if options else 10
        skip = (page - 1) * limit
        
        total_docs = await cls.find(search_query).count()
        docs = await cls.find(search_query).skip(skip).limit(limit).to_list()
        
        return {
            "docs": docs,
            "totalDocs": total_docs,
            "totalPages": (total_docs + limit - 1) // limit,
            "page": page
        }
    
    @classmethod
    async def bulk_upsert_flow_stats(cls, flow_stats: List[Dict[str, Any]]):
        from ..db.database import get_database
        
        if not flow_stats:
            return {"modifiedCount": 0, "upsertedCount": 0}
        
        db = await get_database()
        collection = db.flowrecentstats
        
        bulk_ops = []
        for stat in flow_stats:
            filter_query = {
                "klaviyo_public_id": stat["klaviyo_public_id"],
                "flow_id": stat["flow_id"],
                "flow_message_id": stat["flow_message_id"],
                "send_channel": stat["send_channel"]
            }
            
            update_data = {
                "$set": stat,
                "$currentDate": {"updated_at": True}
            }
            
            bulk_ops.append({
                "updateOne": {
                    "filter": filter_query,
                    "update": update_data,
                    "upsert": True
                }
            })
        
        result = await collection.bulk_write(bulk_ops)
        
        return {
            "modifiedCount": result.modified_count,
            "upsertedCount": result.upserted_count
        }