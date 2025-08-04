from datetime import datetime
from typing import Optional, List
from enum import Enum
from beanie import Document, Indexed
from pydantic import Field, BaseModel
from bson import ObjectId
from .user import User


class SendChannel(str, Enum):
    EMAIL = "email"
    SMS = "sms"
    PUSH_NOTIFICATION = "push-notification"


class AudienceType(str, Enum):
    LIST = "list"
    SEGMENT = "segment"


class Groupings(BaseModel):
    send_channel: SendChannel
    campaign_id: str
    campaign_message_id: str


class Statistics(BaseModel):
    opens: int = 0
    open_rate: float = 0
    bounced: int = 0
    clicks: int = 0
    clicks_unique: int = 0
    click_rate: float = 0
    delivered: int = 0
    bounced_or_failed: int = 0
    bounced_or_failed_rate: float = 0
    delivery_rate: float = 0
    failed: int = 0
    failed_rate: float = 0
    recipients: int = 0
    opens_unique: int = 0
    bounce_rate: float = 0
    unsubscribe_rate: float = 0
    unsubscribe_uniques: int = 0
    unsubscribes: int = 0
    spam_complaint_rate: float = 0
    spam_complaints: int = 0
    click_to_open_rate: float = 0
    conversions: int = 0
    conversion_uniques: int = 0
    conversion_value: float = 0
    conversion_rate: float = 0
    average_order_value: float = 0
    revenue_per_recipient: float = 0


class Audience(BaseModel):
    id: str
    type: AudienceType
    name: str


class CampaignStats(Document):
    # Klaviyo public ID as primary identifier
    klaviyo_public_id: Indexed(str)
    
    # Campaign data
    groupings: Groupings
    statistics: Statistics = Field(default_factory=Statistics)
    campaign_name: Optional[str] = None
    included_audiences: List[Audience] = Field(default_factory=list)
    excluded_audiences: List[Audience] = Field(default_factory=list)
    tag_ids: List[str] = Field(default_factory=list, alias="tagIds")
    tag_names: List[str] = Field(default_factory=list, alias="tagNames")
    send_time: Optional[datetime] = None
    created_at: Optional[datetime] = None
    scheduled_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Settings:
        name = "campaignstats"
        use_state_management = True
        indexes = [
            "klaviyo_public_id",
            [("klaviyo_public_id", 1), ("groupings.campaign_id", 1)]
        ]
    
    class Config:
        arbitrary_types_allowed = True  # Allow ObjectId type

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