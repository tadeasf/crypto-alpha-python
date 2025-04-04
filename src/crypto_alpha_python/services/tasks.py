"""
Task manager for handling background tasks.
"""
import asyncio
from typing import List
import logging
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.services.collector import collector

logger = logging.getLogger(__name__)

class TaskManager:
    """Manager for background tasks."""
    
    def __init__(self):
        """Initialize task manager."""
        self.tasks: List[asyncio.Task] = []
    
    async def start_collection(
        self,
        session: AsyncSession,
        symbols: List[str],
    ) -> None:
        """Start market data collection."""
        try:
            # Initialize collector with session
            global collector
            collector = MarketDataCollector(session)
            
            # Start collection
            await collector.start_collection(symbols)
            logger.info("Market data collection started successfully")
        except Exception as e:
            logger.error(f"Error starting market data collection: {e}")
            raise
    
    async def stop_collection(self) -> None:
        """Stop market data collection."""
        try:
            if collector:
                await collector.stop_collection()
                logger.info("Market data collection stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping market data collection: {e}")
            raise

# Create global task manager instance
task_manager = TaskManager() 