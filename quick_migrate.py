#!/usr/bin/env python3
"""
Quick migration script to update your specific store document
"""

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId

# MongoDB connection
MONGODB_URI = "mongodb+srv://dthan:dxks62vCCt5vrSqU@rfp.avj0rfi.mongodb.net/cmo?retryWrites=true&w=majority&appName=rfp"

async def quick_migrate():
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client.cmo
    stores = db.stores
    
    # Your store IDs
    store_id = ObjectId("688196b8457f56e9e43250fe")
    public_id = "Y05y7N6"
    
    # Find the store
    store = await stores.find_one({"_id": store_id})
    if not store:
        print(f"Store not found with ID: {store_id}")
        return
    
    print(f"Found store: {store.get('name')}")
    
    # Extract data from old structure
    old_klaviyo = store.get('klaviyo_integration', {})
    
    # Update to new structure
    update_result = await stores.update_one(
        {"_id": store_id},
        {
            "$set": {
                # Core fields - keep existing
                "name": store.get('name'),
                "public_id": public_id,
                "domain": store.get('url', store.get('shopify_domain')),
                
                # Update klaviyo_integration to simple structure
                "klaviyo_integration": {
                    "api_key": old_klaviyo.get('apiKey', 'pk_25c6ff002ab9df7b78f57fbdba26f4c63c'),
                    "oauth_token": None,
                    "refresh_token": None,
                    "conversion_metric_id": old_klaviyo.get('conversion_metric_id', 'PhqdJM'),
                    "flow_date_times": []
                },
                
                # Simple settings
                "settings": {
                    "timezone": "UTC",
                    "currency": "USD"
                },
                
                # Keep these fields
                "last_dashboard_sync": store.get('last_dashboard_sync'),
                "is_updating_dashboard": store.get('is_updating_dashboard', False),
                "tag_names": store.get('tagNames', []),
                "created_at": store.get('created_at'),
                "updated_at": store.get('updated_at')
            }
        }
    )
    
    if update_result.modified_count > 0:
        print("✅ Store migrated successfully!")
        
        # Verify the update
        updated_store = await stores.find_one({"_id": store_id})
        print("\nUpdated structure:")
        print(f"  - Domain: {updated_store.get('domain')}")
        print(f"  - Klaviyo API Key: {updated_store.get('klaviyo_integration', {}).get('api_key')}")
        print(f"  - Settings: {updated_store.get('settings')}")
    else:
        print("❌ No changes made to the store")
    
    client.close()

if __name__ == "__main__":
    asyncio.run(quick_migrate())