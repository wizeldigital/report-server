#!/usr/bin/env python3
"""
Restore the original store document structure
"""

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from datetime import datetime

MONGODB_URI = "mongodb+srv://dthan:dxks62vCCt5vrSqU@rfp.avj0rfi.mongodb.net/cmo?retryWrites=true&w=majority&appName=rfp"

async def restore_store():
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client.cmo
    stores = db.stores
    
    store_id = ObjectId("688196b8457f56e9e43250fe")
    
    # Restore to original structure
    update_result = await stores.update_one(
        {"_id": store_id},
        {
            "$set": {
                "shopify_domain": "www.balmainbody.com.au",
                "url": "https://www.balmainbody.com.au/",
                "name": "balmain5",
                "owner_id": ObjectId("68377bb958be27a3a6e9702b"),
                "tagNames": [],
                "last_dashboard_sync": None,
                "is_updating_dashboard": False,
                "shopify_integration": {
                    "status": "disconnected",
                    "access_token": None,
                    "webhook_secret": None,
                    "installed_at": None,
                    "last_sync": None,
                    "sync_status": None,
                    "scopes": []
                },
                "klaviyo_integration": {
                    "status": "connected",
                    "account": {
                        "data": [
                            {
                                "type": "account",
                                "id": "Pe5Xw6",
                                "attributes": {
                                    "test_account": False,
                                    "contact_information": {
                                        "default_sender_name": "Shopify [[DEMO]]",
                                        "default_sender_email": "hello@klaviyo-demo.com",
                                        "website_url": "https://klaviyo-swag.myshopify.com/",
                                        "organization_name": "Shopify [[DEMO]]",
                                        "street_address": {
                                            "address1": "125 summer st",
                                            "address2": None,
                                            "city": "Boston",
                                            "region": "Massachusetts",
                                            "country": "United States",
                                            "zip": "02114"
                                        }
                                    },
                                    "industry": "Ecommerce, Health & Beauty, Fragrance",
                                    "timezone": "Europe/Berlin",
                                    "preferred_currency": "USD",
                                    "public_api_key": "Pe5Xw6",
                                    "locale": "en-US"
                                },
                                "links": {
                                    "self": "https://a.klaviyo.com/api/accounts/Pe5Xw6/"
                                }
                            }
                        ],
                        "links": {
                            "self": "https://a.klaviyo.com/api/accounts",
                            "next": None,
                            "prev": None
                        },
                        "included": []
                    },
                    "public_id": "Pe5Xw6",
                    "conversion_metric_id": "PhqdJM",
                    "conversion_type": "value",
                    "apiKey": "pk_25c6ff002ab9df7b78f57fbdba26f4c63c",
                    "connected_at": datetime.fromisoformat("2025-08-04T02:03:55.556Z".replace("Z", "+00:00"))
                },
                "subscription": {
                    "plan_id": None,
                    "status": None,
                    "trial_ends_at": None,
                    "billing_cycle_anchor": None
                },
                "settings": {
                    "timezone": None,
                    "currency": None
                },
                "isActive": True,
                "deletedAt": None,
                "deletedBy": None,
                "users": [
                    {
                        "userId": ObjectId("68377bb958be27a3a6e9702b"),
                        "role": "owner",
                        "permissions": {
                            "canEditStore": True,
                            "canEditBrand": True,
                            "canEditContent": True,
                            "canApproveContent": True,
                            "canViewAnalytics": True,
                            "canManageIntegrations": True,
                            "canManageUsers": True
                        },
                        "addedAt": datetime.fromisoformat("2025-07-24T02:13:12.137Z".replace("Z", "+00:00")),
                        "_id": ObjectId("688196b8457f56e9e4325100")
                    }
                ],
                "shared_with": [],
                "brands": [
                    {
                        "_id": ObjectId("688196b8457f56e9e4325102"),
                        "name": "Default",
                        "slug": "default"
                    }
                ],
                "created_at": datetime.fromisoformat("2025-07-24T02:13:12.030Z".replace("Z", "+00:00")),
                "updated_at": datetime.fromisoformat("2025-07-24T02:13:12.483Z".replace("Z", "+00:00")),
                "public_id": "Y05y7N6",
                "__v": 1
            },
            "$unset": {
                "domain": "",
                "tag_names": ""
            }
        }
    )
    
    if update_result.modified_count > 0:
        print("✅ Store restored to original structure!")
    else:
        print("❌ Failed to restore store")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(restore_store())