from datetime import datetime
from typing import Optional, List
from beanie import Document, Indexed
from pydantic import Field, BaseModel
from bson import ObjectId


class FlowStatistics(BaseModel):
    """Flow statistics matching Klaviyo API response structure with arrays for time series"""
    average_order_value: List[float] = Field(default_factory=list)
    bounce_rate: List[float] = Field(default_factory=list)
    bounced: List[float] = Field(default_factory=list)
    bounced_or_failed: List[float] = Field(default_factory=list)
    bounced_or_failed_rate: List[float] = Field(default_factory=list)
    click_rate: List[float] = Field(default_factory=list)
    click_to_open_rate: List[float] = Field(default_factory=list)
    clicks: List[float] = Field(default_factory=list)
    clicks_unique: List[float] = Field(default_factory=list)
    conversion_rate: List[float] = Field(default_factory=list)
    conversion_uniques: List[float] = Field(default_factory=list)
    conversion_value: List[float] = Field(default_factory=list)
    conversions: List[float] = Field(default_factory=list)
    delivered: List[float] = Field(default_factory=list)
    delivery_rate: List[float] = Field(default_factory=list)
    failed: List[float] = Field(default_factory=list)
    failed_rate: List[float] = Field(default_factory=list)
    open_rate: List[float] = Field(default_factory=list)
    opens: List[float] = Field(default_factory=list)
    opens_unique: List[float] = Field(default_factory=list)
    recipients: List[float] = Field(default_factory=list)
    revenue_per_recipient: List[float] = Field(default_factory=list)
    spam_complaint_rate: List[float] = Field(default_factory=list)
    spam_complaints: List[float] = Field(default_factory=list)
    unsubscribe_rate: List[float] = Field(default_factory=list)
    unsubscribe_uniques: List[float] = Field(default_factory=list)
    unsubscribes: List[float] = Field(default_factory=list)


class FlowGroupings(BaseModel):
    """Flow groupings from Klaviyo API"""
    flow_id: str
    flow_message_id: str
    send_channel: str


class FlowStats(Document):
    """Flow statistics model matching Express schema"""
    # Klaviyo public ID as primary identifier
    klaviyo_public_id: Indexed(str)
    
    # Flow identifiers - matching the groupings from API
    flow_id: str
    flow_message_id: str
    send_channel: str
    
    # Flow metadata
    flow_name: Optional[str] = None
    flow_message_name: Optional[str] = None
    flow_message_subject: Optional[str] = None
    flow_status: Optional[str] = None
    flow_archived: bool = False
    flow_created: Optional[datetime] = None
    flow_updated: Optional[datetime] = None
    flow_trigger_type: Optional[str] = None
    
    # Tags
    tag_ids: List[str] = Field(default_factory=list, alias="tagIds")
    tag_names: List[str] = Field(default_factory=list, alias="tagNames")
    
    # Statistics - arrays for time series data
    statistics: FlowStatistics
    
    # Date times array - corresponds to the statistics arrays
    date_times: List[datetime] = Field(default_factory=list)
    
    # Timestamps
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        arbitrary_types_allowed = True
    
    class Settings:
        name = "flowstats"
        use_state_management = True
        indexes = [
            # Compound index for efficient queries
            [("klaviyo_public_id", 1), ("flow_id", 1), ("flow_message_id", 1), ("send_channel", 1)],
            # Index for date-based queries
            "date_times",
            # Additional indexes
            [("klaviyo_public_id", 1), ("flow_id", 1)],
            "klaviyo_public_id",
            "flow_id",
            "flow_message_id"
        ]