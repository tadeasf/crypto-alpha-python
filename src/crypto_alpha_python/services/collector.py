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
    
    async def start_collection(self, symbols: List[str]) -> None:
        """Start collecting market data for specified symbols."""
        if self.is_running:
            logger.warning("Market data collection is already running")
            return
        
        self.is_running = True
        self.symbols = set(symbols)
        
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
    
    async def stop_collection(self) -> None:
        """Stop collecting market data."""
        if not self.is_running:
            logger.warning("Market data collection is not running")
            return
        
        self.is_running = False
        logger.info("Stopping market data collection")
        
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
        
        # Clear tasks and symbols
        self.collection_tasks.clear()
        self.symbols.clear()
        
        # Close all WebSocket connections
        await exchange_service.close_connections()
        logger.info("Market data collection stopped")
    
    async def _collect_data(self, exchange: str, symbol: str) -> None:
        """Collect market data for a symbol from an exchange."""
        max_retries = 5
        retry_delay = 2  # Initial delay in seconds
        
        while self.is_running:
            try:
                # Connect to WebSocket
                if exchange == "binance":
                    await exchange_service.connect_binance_websocket(symbol)
                else:  # coinbase
                    await exchange_service.connect_coinbase_websocket(symbol)
                
                # Keep the connection alive and monitor for data
                while self.is_running:
                    # Get initial data using REST API
                    if exchange == "binance":
                        market_data = await exchange_service.get_binance_ticker(symbol)
                    else:  # coinbase
                        market_data = await exchange_service.get_coinbase_ticker(symbol)
                    
                    if market_data:
                        logger.info(f"Retrieved initial {exchange} data for {symbol}: last_price={market_data.last_price}, volume={market_data.volume}")
                        # Create a new session for each database operation
                        async for session in get_session():
                            await create_market_data(session, market_data)
                    
                    # Wait before next update
                    await asyncio.sleep(5)  # Update every 5 seconds as fallback
                    
            except Exception as e:
                logger.warning(f"Error collecting {exchange} data for {symbol}: {e}")
                
                if max_retries > 0:
                    logger.info(f"Retrying in {retry_delay} seconds (attempt {6-max_retries}/5)")
                    await asyncio.sleep(retry_delay)
                    max_retries -= 1
                    retry_delay = min(retry_delay * 2, 30)  # Exponential backoff, max 30 seconds
                else:
                    logger.error(f"Max retries reached for {exchange} {symbol}")
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