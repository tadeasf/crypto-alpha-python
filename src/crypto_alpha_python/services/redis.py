"""
Redis service for caching market data and analysis results.
"""
import json
from datetime import datetime, timedelta
from typing import Any, Optional
import redis.asyncio as redis
import logging

from crypto_alpha_python.core.config import settings

logger = logging.getLogger(__name__)

class RedisService:
    """Service for Redis caching operations."""
    
    def __init__(self):
        """Initialize Redis connection."""
        self.redis = None
        self._connect()
    
    def _connect(self) -> None:
        """Connect to Redis server."""
        try:
            self.redis = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                decode_responses=True
            )
            logger.info("Connected to Redis server")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis = None
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from Redis cache."""
        if not self.redis:
            return None
        
        try:
            value = await self.redis.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Error getting value from Redis: {e}")
            return None
    
    async def set(
        self,
        key: str,
        value: Any,
        expire_seconds: int = 300  # Default 5 minutes
    ) -> bool:
        """Set value in Redis cache with expiration."""
        if not self.redis:
            return False
        
        try:
            await self.redis.set(
                key,
                json.dumps(value, default=str),
                ex=expire_seconds
            )
            return True
        except Exception as e:
            logger.error(f"Error setting value in Redis: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Delete value from Redis cache."""
        if not self.redis:
            return False
        
        try:
            await self.redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Error deleting value from Redis: {e}")
            return False
    
    def generate_key(
        self,
        prefix: str,
        symbol: str,
        window: Optional[str] = None,
        exchange: Optional[str] = None,
        threshold: Optional[float] = None
    ) -> str:
        """Generate a consistent cache key."""
        key_parts = [prefix, symbol]
        
        if window:
            key_parts.append(window)
        if exchange:
            key_parts.append(exchange)
        if threshold is not None:
            key_parts.append(str(threshold))
        
        return ":".join(key_parts)

# Create global Redis service instance
redis_service = RedisService() 