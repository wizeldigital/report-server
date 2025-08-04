#!/usr/bin/env python3
"""
Migration script to transform existing store documents to match the FastAPI Store model
"""

import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB connection string - update this to match your .env
MONGODB_URI = "mongodb+srv://dthan:dxks62vCCt5vrSqU@rfp.avj0rfi.mongodb.net/cmo?retryWrites=true&w=majority&appName=rfp"
DATABASE_NAME = "cmo"


async def migrate_stores():
    """Migrate store documents to match the FastAPI model structure"""
    
    # Connect to MongoDB
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client[DATABASE_NAME]
    stores_collection = db.stores
    
    try:
        # Get all stores
        cursor = stores_collection.find({})
        stores = await cursor.to_list(length=None)
        
        logger.info(f"Found {len(stores)} stores to migrate")
        
        migrated_count = 0
        error_count = 0
        
        for store in stores:
            try:
                store_id = store['_id']
                logger.info(f"Migrating store {store_id} ({store.get('name', 'Unknown')})")
                
                # Extract klaviyo integration data
                old_klaviyo = store.get('klaviyo_integration', {})
                
                # Create new klaviyo_integration structure
                new_klaviyo_integration = {
                    'api_key': old_klaviyo.get('apiKey'),
                    'oauth_token': None,  # Add if you have OAuth tokens
                    'refresh_token': None,  # Add if you have refresh tokens
                    'conversion_metric_id': old_klaviyo.get('conversion_metric_id'),
                    'flow_date_times': []
                }
                
                # Create settings structure
                new_settings = {
                    'timezone': store.get('settings', {}).get('timezone', 'UTC') or 'UTC',
                    'currency': store.get('settings', {}).get('currency', 'USD') or 'USD'
                }
                
                # Create the updated document
                update_doc = {
                    '$set': {
                        # Core fields
                        'name': store.get('name'),
                        'public_id': store.get('public_id'),
                        'domain': store.get('url') or store.get('shopify_domain'),
                        
                        # Nested structures
                        'klaviyo_integration': new_klaviyo_integration,
                        'settings': new_settings,
                        
                        # Status fields
                        'last_dashboard_sync': store.get('last_dashboard_sync'),
                        'is_updating_dashboard': store.get('is_updating_dashboard', False),
                        
                        # Arrays
                        'tag_names': store.get('tagNames', []),
                        
                        # Timestamps
                        'created_at': store.get('created_at') or datetime.utcnow(),
                        'updated_at': store.get('updated_at') or datetime.utcnow()
                    },
                    '$unset': {
                        # Remove fields that don't exist in the new model
                        'shopify_domain': '',
                        'url': '',
                        'owner_id': '',
                        'shopify_integration': '',
                        'subscription': '',
                        'isActive': '',
                        'deletedAt': '',
                        'deletedBy': '',
                        'users': '',
                        'shared_with': '',
                        'brands': '',
                        'tagNames': ''  # Remove since we're using tag_names
                    }
                }
                
                # Apply the migration
                result = await stores_collection.update_one(
                    {'_id': store_id},
                    update_doc
                )
                
                if result.modified_count > 0:
                    migrated_count += 1
                    logger.info(f"Successfully migrated store {store_id}")
                else:
                    logger.warning(f"No changes made to store {store_id}")
                    
            except Exception as e:
                error_count += 1
                logger.error(f"Error migrating store {store.get('_id')}: {str(e)}")
                continue
        
        logger.info(f"Migration completed: {migrated_count} stores migrated, {error_count} errors")
        
    finally:
        client.close()


async def verify_migration():
    """Verify the migration by checking a few stores"""
    
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client[DATABASE_NAME]
    stores_collection = db.stores
    
    try:
        # Get a sample of migrated stores
        cursor = stores_collection.find({}).limit(3)
        stores = await cursor.to_list(length=None)
        
        logger.info("\n=== Verification: Sample migrated stores ===")
        
        for store in stores:
            logger.info(f"\nStore: {store.get('name')} (ID: {store.get('_id')})")
            logger.info(f"  Public ID: {store.get('public_id')}")
            logger.info(f"  Domain: {store.get('domain')}")
            logger.info(f"  Klaviyo API Key: {'✓' if store.get('klaviyo_integration', {}).get('api_key') else '✗'}")
            logger.info(f"  Settings: {store.get('settings')}")
            logger.info(f"  Tag Names: {store.get('tag_names', [])}")
            
    finally:
        client.close()


async def backup_stores():
    """Create a backup of stores before migration"""
    
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client[DATABASE_NAME]
    stores_collection = db.stores
    backup_collection = db.stores_backup_migration
    
    try:
        # Check if backup already exists
        backup_count = await backup_collection.count_documents({})
        if backup_count > 0:
            logger.warning(f"Backup collection already exists with {backup_count} documents")
            response = input("Do you want to overwrite the backup? (y/n): ")
            if response.lower() != 'y':
                logger.info("Skipping backup")
                return
            await backup_collection.drop()
        
        # Create backup
        cursor = stores_collection.find({})
        stores = await cursor.to_list(length=None)
        
        if stores:
            result = await backup_collection.insert_many(stores)
            logger.info(f"Backed up {len(result.inserted_ids)} stores to stores_backup_migration collection")
        else:
            logger.warning("No stores found to backup")
            
    finally:
        client.close()


async def rollback_migration():
    """Rollback the migration using the backup"""
    
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client[DATABASE_NAME]
    stores_collection = db.stores
    backup_collection = db.stores_backup_migration
    
    try:
        # Check if backup exists
        backup_count = await backup_collection.count_documents({})
        if backup_count == 0:
            logger.error("No backup found. Cannot rollback.")
            return
        
        response = input(f"This will restore {backup_count} stores from backup. Continue? (y/n): ")
        if response.lower() != 'y':
            logger.info("Rollback cancelled")
            return
        
        # Drop current stores and restore from backup
        await stores_collection.drop()
        
        cursor = backup_collection.find({})
        backup_stores = await cursor.to_list(length=None)
        
        if backup_stores:
            result = await stores_collection.insert_many(backup_stores)
            logger.info(f"Restored {len(result.inserted_ids)} stores from backup")
        
    finally:
        client.close()


async def main():
    """Main migration function"""
    
    logger.info("Store Migration Script")
    logger.info("=====================")
    logger.info("This script will migrate your store documents to match the FastAPI model structure")
    logger.info(f"Database: {DATABASE_NAME}")
    logger.info("")
    
    print("Options:")
    print("1. Backup stores (recommended to run first)")
    print("2. Run migration")
    print("3. Verify migration")
    print("4. Rollback migration")
    print("5. Exit")
    
    choice = input("\nSelect option (1-5): ")
    
    if choice == '1':
        await backup_stores()
    elif choice == '2':
        # Suggest backup first
        has_backup = await check_backup_exists()
        if not has_backup:
            logger.warning("No backup found. It's recommended to backup first.")
            response = input("Continue without backup? (y/n): ")
            if response.lower() != 'y':
                logger.info("Migration cancelled")
                return
        
        await migrate_stores()
        logger.info("\nRunning verification...")
        await verify_migration()
    elif choice == '3':
        await verify_migration()
    elif choice == '4':
        await rollback_migration()
    elif choice == '5':
        logger.info("Exiting")
        return
    else:
        logger.error("Invalid option")


async def check_backup_exists():
    """Check if backup collection exists"""
    client = AsyncIOMotorClient(MONGODB_URI)
    db = client[DATABASE_NAME]
    backup_collection = db.stores_backup_migration
    
    try:
        count = await backup_collection.count_documents({})
        return count > 0
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())