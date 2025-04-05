"""
Task manager for handling background tasks.
"""
import asyncio
from typing import List, Set
import logging
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.services.collector import MarketDataCollector, collector

logger = logging.getLogger(__name__)

class TaskManager:
    """Manager for background tasks."""
    
    def __init__(self):
        """Initialize task manager."""
        self.tasks: List[asyncio.Task] = []
        self.exchanges: Set[str] = {"binance", "coinbase"}  # Default to both exchanges
        self.default_symbols: List[str] = ["BTCUSDT", "ETHUSDT"]  # Default symbols
    
    async def start_collection(
        self,
        session: AsyncSession,
        symbols: List[str],
        is_default: bool = False,
    ) -> None:
        """
        Start market data collection for specified symbols.
        
        Args:
            session: Database session
            symbols: List of symbols to collect data for
            is_default: Whether these are default symbols that shouldn't be removed
        """
        try:
            # Initialize collector with session
            global collector
            if not collector:
                collector = MarketDataCollector(session)
            
            # Start collection
            await collector.start_collection(symbols, is_default)
            logger.info("Market data collection started successfully")
        except Exception as e:
            logger.error(f"Error starting market data collection: {e}")
            raise
    
    async def add_symbol(
        self,
        session: AsyncSession,
        symbol: str,
    ) -> None:
        """
        Add a symbol to collect market data for.
        
        Args:
            session: Database session
            symbol: Symbol to add
        """
        try:
            # Initialize collector with session if needed
            global collector
            if not collector:
                collector = MarketDataCollector(session)
                # Start with default symbols if not running yet
                await collector.start_collection(self.default_symbols, is_default=True)
            
            # Add the symbol
            await collector.add_symbols([symbol])
            logger.info(f"Added market data collection for {symbol}")
        except Exception as e:
            logger.error(f"Error adding symbol {symbol} to collection: {e}")
            raise
    
    async def remove_symbol(
        self,
        symbol: str,
    ) -> bool:
        """
        Remove a symbol from market data collection.
        
        Args:
            symbol: Symbol to remove
            
        Returns:
            True if symbol was removed, False if symbol couldn't be removed
        """
        try:
            if not collector:
                logger.warning("Market data collector not initialized")
                return False
            
            # Remove the symbol
            result = await collector.remove_symbol(symbol)
            if result:
                logger.info(f"Removed market data collection for {symbol}")
            return result
        except Exception as e:
            logger.error(f"Error removing symbol {symbol} from collection: {e}")
            raise
    
    async def stop_collection(self) -> None:
        """Stop all market data collection."""
        try:
            if collector:
                await collector.stop_collection()
                logger.info("Market data collection stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping market data collection: {e}")
            raise

# Create global task manager instance
task_manager = TaskManager() 