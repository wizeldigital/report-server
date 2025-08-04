from datetime import datetime
from typing import Optional, List
from enum import Enum
from beanie import Document, Link, Indexed
from pydantic import Field, BaseModel
from bson import ObjectId
from .store import Store
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
    opens: float = 0
    open_rate: float = 0
    bounced: float = 0
    clicks: float = 0
    clicks_unique: float = 0
    click_rate: float = 0
    delivered: float = 0
    bounced_or_failed: float = 0
    bounced_or_failed_rate: float = 0
    delivery_rate: float = 0
    failed: float = 0
    failed_rate: float = 0
    recipients: float = 0
    opens_unique: float = 0
    bounce_rate: float = 0
    unsubscribe_rate: float = 0
    unsubscribe_uniques: float = 0
    unsubscribes: float = 0
    spam_complaint_rate: float = 0
    spam_complaints: float = 0
    click_to_open_rate: float = 0
    conversions: float = 0
    conversion_uniques: float = 0
    conversion_value: float = 0
    conversion_rate: float = 0
    average_order_value: float = 0
    revenue_per_recipient: float = 0


class Audience(BaseModel):
    id: str
    type: AudienceType
    name: str


class CampaignStats(Document):
    groupings: Groupings
    statistics: Statistics = Field(default_factory=Statistics)
    campaign_name: Optional[str] = None
    included_audiences: List[Audience] = Field(default_factory=list)
    excluded_audiences: List[Audience] = Field(default_factory=list)
    tag_ids: List[str] = Field(default_factory=list, alias="tagIds")
    tag_names: List[str] = Field(default_factory=list, alias="tagNames")
    store_public_id: Indexed(str)
    store_id: Link[Store]
    send_time: Optional[datetime] = None
    created_at: Optional[datetime] = None
    scheduled_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Settings:
        name = "campaignstats"
        use_state_management = True
        indexes = [
            "store_public_id"
        ]

    @classmethod
    async def search_with_access_control(
        cls,
        user_id: str,
        query: dict = None,
        options: dict = None
    ):
        from ..core.permissions import has_permission, ADMIN, VIEW_ANALYTICS
        
        user = await User.get(user_id)
        if not user:
            return {"docs": [], "totalDocs": 0, "totalPages": 0, "page": 1}
        
        accessible_stores = []
        for store_access in user.stores:
            if has_permission(store_access.permissions, VIEW_ANALYTICS):
                accessible_stores.append(store_access.store_public_id)
        
        if not accessible_stores:
            return {"docs": [], "totalDocs": 0, "totalPages": 0, "page": 1}
        
        search_query = query or {}
        search_query["store_public_id"] = {"$in": accessible_stores}
        
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