from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging
import time
import uuid
import asyncio

from .core.config import settings
from .db.database import connect_to_mongo, close_mongo_connection
from .api.v1.api import api_router
from .utils.memory_monitor import MemoryMonitor, estimate_user_capacity

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up FastAPI Email Server...")
    
    # Log initial memory usage
    MemoryMonitor.log_memory_usage("Startup - Before MongoDB")
    
    await connect_to_mongo()
    logger.info("Connected to MongoDB")
    
    # Log memory after MongoDB connection
    MemoryMonitor.log_memory_usage("Startup - After MongoDB")
    
    # Show capacity estimates
    capacity = estimate_user_capacity(16384)  # 16GB for Render
    logger.info(
        f"📊 Server Capacity Estimates (16GB RAM): "
        f"Max {capacity['estimated_capacity']['active_users']:,} active users, "
        f"Max {capacity['recommendations']['max_concurrent_syncs']} concurrent syncs"
    )
    
    # Start periodic memory monitoring
    monitor_task = asyncio.create_task(
        MemoryMonitor.start_periodic_monitoring(interval_seconds=300)  # Every 5 minutes
    )
    
    yield
    
    # Shutdown
    logger.info("Shutting down...")
    monitor_task.cancel()
    await close_mongo_connection()
    logger.info("Disconnected from MongoDB")
    
    # Final memory usage
    MemoryMonitor.log_memory_usage("Shutdown")


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Add request ID middleware
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time"] = str(process_time)
    
    logger.info(
        f"Request ID: {request_id} - "
        f"{request.method} {request.url.path} - "
        f"Status: {response.status_code} - "
        f"Time: {process_time:.3f}s"
    )
    
    return response


# Health check endpoint
@app.get("/", tags=["health"])
async def root():
    return {
        "status": "ok",
        "service": settings.PROJECT_NAME,
        "version": settings.VERSION
    }


# Status endpoint
@app.get("/status", tags=["health"])
async def status():
    memory_info = MemoryMonitor.get_memory_info()
    return {
        "status": "healthy",
        "service": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "timestamp": time.time(),
        "memory": {
            "process_mb": round(memory_info["process_rss_mb"], 1),
            "process_percent": round(memory_info["process_percent"], 1),
            "system_available_mb": round(memory_info["system_available_mb"], 1),
            "system_percent": round(memory_info["system_percent"], 1),
            "cpu_percent": round(memory_info["cpu_percent"], 1),
            "threads": memory_info["num_threads"],
            "connections": memory_info["num_connections"]
        }
    }


# Include API router
app.include_router(api_router, prefix=settings.API_V1_STR)


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "request_id": getattr(request.state, "request_id", "unknown")
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=True
    )