import psutil
import os
import logging
from typing import Dict, Any
import asyncio

logger = logging.getLogger(__name__)


class MemoryMonitor:
    """Monitor memory usage of the application"""
    
    @staticmethod
    def get_memory_info() -> Dict[str, Any]:
        """Get current memory usage information"""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        # System-wide memory
        virtual_memory = psutil.virtual_memory()
        
        return {
            # Process memory
            "process_rss_mb": memory_info.rss / 1024 / 1024,  # Resident Set Size
            "process_vms_mb": memory_info.vms / 1024 / 1024,  # Virtual Memory Size
            "process_percent": process.memory_percent(),
            
            # System memory
            "system_total_mb": virtual_memory.total / 1024 / 1024,
            "system_available_mb": virtual_memory.available / 1024 / 1024,
            "system_used_mb": virtual_memory.used / 1024 / 1024,
            "system_percent": virtual_memory.percent,
            
            # Additional info
            "cpu_percent": process.cpu_percent(),
            "num_threads": process.num_threads(),
            "num_connections": len(process.connections()),
        }
    
    @staticmethod
    def log_memory_usage(context: str = ""):
        """Log current memory usage"""
        try:
            info = MemoryMonitor.get_memory_info()
            
            logger.info(
                f"💾 Memory Usage{' - ' + context if context else ''}: "
                f"Process: {info['process_rss_mb']:.1f}MB ({info['process_percent']:.1f}%), "
                f"System: {info['system_used_mb']:.0f}/{info['system_total_mb']:.0f}MB ({info['system_percent']:.1f}%), "
                f"CPU: {info['cpu_percent']:.1f}%, "
                f"Threads: {info['num_threads']}, "
                f"Connections: {info['num_connections']}"
            )
            
            # Warn if memory usage is high
            if info['process_rss_mb'] > 1024:  # Over 1GB
                logger.warning(f"⚠️ High memory usage detected: {info['process_rss_mb']:.1f}MB")
            
            if info['system_percent'] > 85:
                logger.warning(f"⚠️ System memory usage critical: {info['system_percent']:.1f}%")
                
        except Exception as e:
            logger.error(f"Error monitoring memory: {str(e)}")
    
    @staticmethod
    async def start_periodic_monitoring(interval_seconds: int = 300):
        """Start periodic memory monitoring (default every 5 minutes)"""
        logger.info(f"Starting periodic memory monitoring every {interval_seconds} seconds")
        
        while True:
            MemoryMonitor.log_memory_usage("Periodic Check")
            await asyncio.sleep(interval_seconds)


def estimate_user_capacity(available_memory_mb: float = 16384) -> Dict[str, Any]:
    """Estimate how many users the server can handle based on available memory"""
    
    # Baseline memory usage (without any users)
    baseline_mb = 200  # Typical FastAPI + MongoDB connection baseline
    
    # Estimated memory per user (based on typical usage)
    memory_per_user_mb = {
        "idle": 0.5,  # User exists but not active
        "active": 2.0,  # User actively making requests
        "sync_running": 10.0,  # User running sync operations
    }
    
    # Reserve 20% for system overhead
    usable_memory_mb = available_memory_mb * 0.8
    available_for_users_mb = usable_memory_mb - baseline_mb
    
    return {
        "total_memory_mb": available_memory_mb,
        "baseline_usage_mb": baseline_mb,
        "available_for_users_mb": available_for_users_mb,
        "estimated_capacity": {
            "idle_users": int(available_for_users_mb / memory_per_user_mb["idle"]),
            "active_users": int(available_for_users_mb / memory_per_user_mb["active"]),
            "concurrent_syncs": int(available_for_users_mb / memory_per_user_mb["sync_running"]),
        },
        "recommendations": {
            "max_concurrent_syncs": min(50, int(available_for_users_mb / memory_per_user_mb["sync_running"] / 2)),
            "max_active_users": int(available_for_users_mb / memory_per_user_mb["active"] / 1.5),
        }
    }