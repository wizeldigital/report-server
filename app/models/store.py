from datetime import datetime
from typing import Optional, List, Dict, Any
from beanie import Document, Indexed
from pydantic import Field, BaseModel
from bson import ObjectId


class KlaviyoAccount(BaseModel):
    data: List[Dict[str, Any]] = Field(default_factory=list)
    links: Dict[str, Any] = Field(default_factory=dict)
    included: List[Any] = Field(default_factory=list)


class KlaviyoIntegration(BaseModel):
    status: Optional[str] = None
    account: Optional[KlaviyoAccount] = None
    public_id: Optional[str] = None
    conversion_metric_id: Optional[str] = None
    conversion_type: Optional[str] = None
    apiKey: Optional[str] = Field(None, alias="api_key")
    api_key: Optional[str] = Field(None, alias="apiKey")  # Support both field names
    oauth_token: Optional[str] = None
    refresh_token: Optional[str] = None
    connected_at: Optional[datetime] = None
    flow_date_times: List[datetime] = Field(default_factory=list)
    
    # Sync tracking fields
    is_updating_dashboard: bool = False
    campaign_values_last_update: Optional[datetime] = None
    segment_values_last_update: Optional[datetime] = None
    flow_values_last_update: Optional[datetime] = None
    form_values_last_update: Optional[datetime] = None
    
    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True


class ShopifyIntegration(BaseModel):
    status: Optional[str] = None
    access_token: Optional[str] = None
    webhook_secret: Optional[str] = None
    installed_at: Optional[datetime] = None
    last_sync: Optional[datetime] = None
    sync_status: Optional[str] = None
    scopes: List[str] = Field(default_factory=list)


class StoreSettings(BaseModel):
    timezone: Optional[str] = None
    currency: Optional[str] = None


class Subscription(BaseModel):
    plan_id: Optional[str] = None
    status: Optional[str] = None
    trial_ends_at: Optional[datetime] = None
    billing_cycle_anchor: Optional[datetime] = None


class UserPermissions(BaseModel):
    canEditStore: bool = False
    canEditBrand: bool = False
    canEditContent: bool = False
    canApproveContent: bool = False
    canViewAnalytics: bool = False
    canManageIntegrations: bool = False
    canManageUsers: bool = False


class StoreUser(BaseModel):
    userId: ObjectId
    role: str
    permissions: UserPermissions
    addedAt: datetime
    _id: Optional[ObjectId] = None
    
    class Config:
        arbitrary_types_allowed = True


class Brand(BaseModel):
    _id: Optional[ObjectId] = None
    name: str
    slug: str
    
    class Config:
        arbitrary_types_allowed = True


class Store(Document):
    model_config = {"arbitrary_types_allowed": True}
    
    name: str
    public_id: Indexed(str, unique=True)
    owner_id: Optional[ObjectId] = None
    shopify_domain: Optional[str] = None
    url: Optional[str] = None
    domain: Optional[str] = None  # For compatibility
    settings: StoreSettings = Field(default_factory=StoreSettings)
    shopify_integration: Optional[ShopifyIntegration] = None
    klaviyo_integration: KlaviyoIntegration = Field(default_factory=KlaviyoIntegration)
    subscription: Optional[Subscription] = None
    tagNames: List[str] = Field(default_factory=list)
    tag_names: List[str] = Field(default_factory=list, alias="tagNames")
    isActive: Optional[bool] = True
    deletedAt: Optional[datetime] = None
    deletedBy: Optional[ObjectId] = None
    users: List[StoreUser] = Field(default_factory=list)
    shared_with: List[Any] = Field(default_factory=list)
    brands: List[Brand] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    __v: Optional[int] = None

    class Settings:
        name = "stores"
        use_state_management = True

    @classmethod
    async def find_by_id_or_public_id_and_update(
        cls, id_or_public_id: str, update: dict, options: dict = None
    ):
        from bson import ObjectId
        import logging
        
        logger = logging.getLogger(__name__)
        logger.info(f"find_by_id_or_public_id_and_update called with: {id_or_public_id}")
        
        filter_query = {}
        try:
            # Try to parse as ObjectId
            oid = ObjectId(id_or_public_id)
            filter_query = {"_id": oid}
            logger.info(f"Using ObjectId query: {filter_query}")
        except Exception:
            # Use as public_id
            filter_query = {"public_id": id_or_public_id}
            logger.info(f"Using public_id query: {filter_query}")
        
        logger.info(f"Searching for store with query: {filter_query}")
        store = await cls.find_one(filter_query)
        
        if store:
            logger.info(f"Found store: {store.name} (id: {store.id})")
            for key, value in update.items():
                if "." in key:
                    # Handle nested updates using dot notation
                    parts = key.split(".")
                    obj = store
                    for part in parts[:-1]:
                        obj = getattr(obj, part)
                    setattr(obj, parts[-1], value)
                else:
                    setattr(store, key, value)
            store.updated_at = datetime.utcnow()
            await store.save()
        else:
            logger.warning(f"No store found with query: {filter_query}")
            
        return store