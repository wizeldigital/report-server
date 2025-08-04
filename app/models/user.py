from datetime import datetime
from typing import List
from beanie import Document, Link, Indexed
from pydantic import Field, EmailStr
from bson import ObjectId
from .store import Store


class StoreAccess(Document):
    store_public_id: str
    store_id: Link[Store]
    permissions: List[str] = Field(default_factory=list)

    class Config:
        arbitrary_types_allowed = True


class User(Document):
    email: Indexed(EmailStr, unique=True)
    name: str
    stores: List[StoreAccess] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow, alias="createdAt")
    updated_at: datetime = Field(default_factory=datetime.utcnow, alias="updatedAt")

    class Settings:
        name = "users"
        use_state_management = True