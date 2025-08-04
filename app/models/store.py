from datetime import datetime
from typing import Optional, List
from beanie import Document, Indexed
from pydantic import Field


class KlaviyoIntegration(Document):
    api_key: Optional[str] = None
    oauth_token: Optional[str] = None
    refresh_token: Optional[str] = None
    conversion_metric_id: Optional[str] = None
    flow_date_times: List[datetime] = Field(default_factory=list)


class StoreSettings(Document):
    timezone: str = "UTC"
    currency: str = "USD"


class Store(Document):
    name: str
    public_id: Indexed(str, unique=True)
    domain: Optional[str] = None
    settings: StoreSettings = Field(default_factory=StoreSettings)
    klaviyo_integration: KlaviyoIntegration = Field(default_factory=KlaviyoIntegration)
    last_dashboard_sync: Optional[datetime] = None
    is_updating_dashboard: bool = False
    tag_names: List[str] = Field(default_factory=list, alias="tagNames")
    created_at: datetime = Field(default_factory=datetime.utcnow, alias="createdAt")
    updated_at: datetime = Field(default_factory=datetime.utcnow, alias="updatedAt")

    class Settings:
        name = "stores"
        use_state_management = True

    @classmethod
    async def find_by_id_or_public_id_and_update(
        cls, id_or_public_id: str, update: dict, options: dict = None
    ):
        from bson import ObjectId
        
        filter_query = {}
        try:
            filter_query = {"_id": ObjectId(id_or_public_id)}
        except:
            filter_query = {"public_id": id_or_public_id}
        
        store = await cls.find_one(filter_query)
        if store:
            for key, value in update.items():
                setattr(store, key, value)
            store.updated_at = datetime.utcnow()
            await store.save()
        return store