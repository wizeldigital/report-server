from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie
from ..models import Store, User, CampaignStats, FlowStats
from ..core.config import settings

client: AsyncIOMotorClient = None
database = None


async def connect_to_mongo():
    global client, database
    client = AsyncIOMotorClient(settings.MONGODB_URI)
    database = client[settings.DATABASE_NAME]
    
    await init_beanie(
        database=database,
        document_models=[
            Store,
            User,
            CampaignStats,
            FlowStats
        ]
    )


async def close_mongo_connection():
    global client
    if client:
        client.close()


async def get_database():
    return database