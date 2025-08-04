from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    PROJECT_NAME: str = "FastAPI Email Server"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"
    
    MONGODB_URI: str
    DATABASE_NAME: str = "email_server"
    
    PRIVATE_KEY: str
    
    KLAVIYO_API_KEY: Optional[str] = None
    KLAVIYO_CLIENT_ID: Optional[str] = None
    KLAVIYO_CLIENT_SECRET: Optional[str] = None
    
    PORT: int = 8001
    
    SYNC_FRESHNESS_THRESHOLD_MINUTES: int = 15
    KLAVIYO_RATE_LIMIT_DELAY: float = 0.1
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()