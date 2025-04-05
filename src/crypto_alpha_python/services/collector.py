"""
Market data collector service for handling data collection from exchanges.
"""
import asyncio
import json
from datetime import datetime
from typing import Dict, List, Set
from sqlmodel.ext.asyncio.session import AsyncSession
import logging

from crypto_alpha_python.models.market_data import MarketData
from crypto_alpha_python.services.exchange import exchange_service
from crypto_alpha_python.services.market_data import create_market_data
from crypto_alpha_python.db.session import get_session

logger = logging.getLogger(__name__)

class MarketDataCollector:
    """Service for collecting market data from various exchanges."""
    
    def __init__(self, session: AsyncSession):
        """Initialize market data collector."""
        self.session = session
        self.is_running = False
        self.collection_tasks: Dict[str, asyncio.Task] = {}
        self.symbols: Set[str] = set()
        self.default_symbols: Set[str] = set()  # Track default symbols that shouldn't be removed
    
    async def start_collection(self, symbols: List[str], is_default: bool = False) -> None:
        """
        Start collecting market data for specified symbols.
        
        Args:
            symbols: List of symbols to collect data for
            is_default: Whether these are default symbols that shouldn't be removed
        """
        if self.is_running:
            # If already running, add the new symbols to collection
            return await self.add_symbols(symbols, is_default)
        
        self.is_running = True
        self.symbols = set(symbols)
        
        # Mark symbols as default if specified
        if is_default:
            self.default_symbols.update(symbols)
        
        logger.info(f"Starting market data collection for symbols: {symbols}")
        
        # Start collection tasks for each symbol
        for symbol in symbols:
            # Start Binance collection
            binance_task = asyncio.create_task(
                self._collect_data("binance", symbol),
                name=f"binance_{symbol}"
            )
            self.collection_tasks[f"binance_{symbol}"] = binance_task
            
            # Start Coinbase collection
            coinbase_task = asyncio.create_task(
                self._collect_data("coinbase", symbol),
                name=f"coinbase_{symbol}"
            )
            self.collection_tasks[f"coinbase_{symbol}"] = coinbase_task
            
            logger.info(f"Started collection tasks for {symbol} on Binance and Coinbase")

    async def add_symbols(self, symbols: List[str], is_default: bool = False) -> None:
        """
        Add symbols to the collection.
        
        Args:
            symbols: List of symbols to add
            is_default: Whether these are default symbols that shouldn't be removed
        """
        if not self.is_running:
            return await self.start_collection(symbols, is_default)
        
        new_symbols = set(symbols) - self.symbols
        if not new_symbols:
            logger.info(f"All symbols {symbols} are already being collected")
            return
        
        # Mark as default if specified
        if is_default:
            self.default_symbols.update(symbols)
        
        # Add new symbols to the set
        self.symbols.update(new_symbols)
        
        logger.info(f"Adding data collection for symbols: {new_symbols}")
        
        # Start collection tasks for each new symbol
        for symbol in new_symbols:
            # Start Binance collection
            binance_task = asyncio.create_task(
                self._collect_data("binance", symbol),
                name=f"binance_{symbol}"
            )
            self.collection_tasks[f"binance_{symbol}"] = binance_task
            
            # Start Coinbase collection
            coinbase_task = asyncio.create_task(
                self._collect_data("coinbase", symbol),
                name=f"coinbase_{symbol}"
            )
            self.collection_tasks[f"coinbase_{symbol}"] = coinbase_task
            
            logger.info(f"Started collection tasks for {symbol} on Binance and Coinbase")

    async def remove_symbol(self, symbol: str) -> bool:
        """
        Remove a symbol from collection.
        
        Args:
            symbol: Symbol to remove
            
        Returns:
            True if symbol was removed, False if symbol couldn't be removed
            (e.g., default symbol or not being collected)
        """
        if not self.is_running:
            logger.warning("Market data collection is not running")
            return False
        
        # Check if symbol is a default symbol that shouldn't be removed
        if symbol in self.default_symbols:
            logger.warning(f"Cannot remove default symbol: {symbol}")
            return False
        
        # Check if symbol is being collected
        if symbol not in self.symbols:
            logger.warning(f"Symbol {symbol} is not being collected")
            return False
        
        logger.info(f"Removing symbol {symbol} from collection")
        
        # Cancel tasks for this symbol
        for exchange in ["binance", "coinbase"]:
            task_key = f"{exchange}_{symbol}"
            if task_key in self.collection_tasks:
                try:
                    task = self.collection_tasks[task_key]
                    task.cancel()
                    await task
                    del self.collection_tasks[task_key]
                    logger.info(f"Cancelled {exchange} collection task for {symbol}")
                except asyncio.CancelledError:
                    logger.info(f"Cancelled {task_key} task")
                except Exception as e:
                    logger.error(f"Error cancelling {task_key} task: {e}")
        
        # Remove symbol from set
        self.symbols.remove(symbol)
        
        return True
    
    async def stop_collection(self) -> None:
        """Stop collecting market data for all symbols."""
        if not self.is_running:
            logger.warning("Market data collection is not running")
            return
        
        self.is_running = False
        logger.info("Stopping all market data collection")
        
        # Cancel all collection tasks
        for task_name, task in self.collection_tasks.items():
            try:
                task.cancel()
                await task
                logger.info(f"Collection task cancelled for {task_name}")
            except asyncio.CancelledError:
                logger.info(f"Cancelled collection task: {task_name}")
            except Exception as e:
                logger.error(f"Error cancelling collection task {task_name}: {e}")
        
        # Clear tasks and symbols (but keep default symbols in memory for future reference)
        self.collection_tasks.clear()
        self.symbols.clear()
        
        # Close all WebSocket connections
        await exchange_service.close_connections()
        logger.info("Market data collection stopped")
    
    async def _collect_data(self, exchange: str, symbol: str) -> None:
        """Collect market data for a symbol from an exchange."""
        max_retries = 5
        retry_delay = 2  # Initial delay in seconds
        
        # Import here to avoid circular imports
        from crypto_alpha_python.api.v1.endpoints.market_data import normalize_symbol
        from crypto_alpha_python.services.exchange import exchange_service
        
        # Keep the original symbol for logging
        original_symbol = symbol
        
        logger.info(f"Starting data collection for {original_symbol} on {exchange}")
        
        while self.is_running:
            try:
                # Connect to WebSocket - the exchange service will format the symbol correctly for each exchange
                if exchange == "binance":
                    await exchange_service.connect_binance_websocket(original_symbol)
                else:  # coinbase
                    await exchange_service.connect_coinbase_websocket(original_symbol)
                
                # Keep the connection alive and monitor for data
                while self.is_running:
                    # Get initial data using REST API
                    if exchange == "binance":
                        market_data = await exchange_service.get_binance_ticker(original_symbol)
                    else:  # coinbase
                        market_data = await exchange_service.get_coinbase_ticker(original_symbol)
                    
                    if market_data:
                        logger.info(f"Retrieved {exchange} data for {original_symbol}: last_price={market_data.last_price}, volume={market_data.volume}")
                        # Create a new session for each database operation
                        async for session in get_session():
                            try:
                                await create_market_data(session, market_data)
                                logger.debug(f"Stored {exchange} data for {original_symbol} in database")
                            except Exception as db_error:
                                logger.error(f"Failed to store {exchange} data in database: {db_error}")
                    else:
                        logger.warning(f"No data received from {exchange} for {original_symbol}")
                    
                    # Wait before next update
                    await asyncio.sleep(5)  # Update every 5 seconds as fallback
                    
            except Exception as e:
                logger.warning(f"Error collecting {exchange} data for {original_symbol}: {e}")
                
                if max_retries > 0:
                    logger.info(f"Retrying in {retry_delay} seconds (attempt {6-max_retries}/5)")
                    await asyncio.sleep(retry_delay)
                    max_retries -= 1
                    retry_delay = min(retry_delay * 2, 30)  # Exponential backoff, max 30 seconds
                else:
                    logger.error(f"Max retries reached for {exchange} {original_symbol}")
                    break
    
    async def _handle_websocket_message(self, message: str) -> None:
        """Handle WebSocket message."""
        try:
            data = json.loads(message)
            
            # Process market data based on exchange
            if "exchange" in data:
                market_data = MarketData(
                    exchange=data["exchange"],
                    symbol=data["symbol"],
                    price=float(data["price"]),
                    volume=float(data["volume"]),
                    timestamp=float(data["timestamp"])
                )
                logger.debug(f"Received market data: {market_data}")
                # Store market data in database
                await create_market_data(self.session, market_data)
                
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {e}")

# Create global collector instance (will be initialized with session)
collector = None 