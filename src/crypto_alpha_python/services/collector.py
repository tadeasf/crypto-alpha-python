"""
Market data collector service for handling data collection from exchanges.
"""
import asyncio
import json
from datetime import datetime
from typing import Dict, List, Set
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.models.market_data import MarketData
from crypto_alpha_python.services.exchange import exchange_service
from crypto_alpha_python.services.market_data import create_market_data

class MarketDataCollector:
    """Service for collecting market data from exchanges."""
    
    def __init__(self, session: AsyncSession):
        """Initialize the collector."""
        self.session = session
        self.symbols: Set[str] = set()
        self.collection_tasks: Dict[str, asyncio.Task] = {}
    
    async def start_collection(
        self,
        symbols: List[str],
        exchanges: List[str] = ["binance", "coinbase"],
    ) -> None:
        """
        Start collecting market data for the specified symbols.
        """
        for symbol in symbols:
            self.symbols.add(symbol)
            
            for exchange in exchanges:
                task_name = f"{exchange}_{symbol}"
                if task_name not in self.collection_tasks:
                    self.collection_tasks[task_name] = asyncio.create_task(
                        self._collect_data(symbol, exchange),
                    )
    
    async def stop_collection(self) -> None:
        """Stop collecting market data."""
        for task in self.collection_tasks.values():
            task.cancel()
        self.collection_tasks.clear()
        self.symbols.clear()
    
    async def _collect_data(
        self,
        symbol: str,
        exchange: str,
    ) -> None:
        """Collect market data from an exchange."""
        while True:
            try:
                if exchange == "binance":
                    data = await exchange_service.get_binance_ticker(symbol)
                    if data:
                        await create_market_data(self.session, data)
                
                elif exchange == "coinbase":
                    data = await exchange_service.get_coinbase_ticker(symbol)
                    if data:
                        await create_market_data(self.session, data)
                
                await asyncio.sleep(1)  # Collect data every second
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error collecting {exchange} data for {symbol}: {e}")
                await asyncio.sleep(5)  # Wait before retrying
    
    async def _handle_websocket_message(
        self,
        message: str,
        exchange: str,
        symbol: str,
    ) -> None:
        """Handle WebSocket messages from exchanges."""
        try:
            data = json.loads(message)
            
            if exchange == "binance":
                market_data = MarketData(
                    timestamp=datetime.fromtimestamp(data["E"] / 1000),
                    symbol=symbol,
                    exchange="binance",
                    bid=float(data["b"]),
                    ask=float(data["a"]),
                    last_price=float(data["c"]),
                    volume=float(data["v"]),
                    bid_size=float(data["B"]),
                    ask_size=float(data["A"]),
                    trades_count=int(data["n"]),
                    vwap=float(data["w"]),
                    high=float(data["h"]),
                    low=float(data["l"]),
                    open=float(data["o"]),
                    close=float(data["c"]),
                )
            
            elif exchange == "coinbase":
                market_data = MarketData(
                    timestamp=datetime.fromisoformat(data["time"].replace("Z", "+00:00")),
                    symbol=symbol,
                    exchange="coinbase",
                    bid=float(data["best_bid"]),
                    ask=float(data["best_ask"]),
                    last_price=float(data["price"]),
                    volume=float(data["volume_24h"]),
                    bid_size=float(data["best_bid_size"]),
                    ask_size=float(data["best_ask_size"]),
                    trades_count=0,  # Coinbase doesn't provide this in ticker
                    vwap=None,  # Coinbase doesn't provide this in ticker
                    high=None,  # Coinbase doesn't provide this in ticker
                    low=None,  # Coinbase doesn't provide this in ticker
                    open=None,  # Coinbase doesn't provide this in ticker
                    close=float(data["price"]),
                )
            
            await create_market_data(self.session, market_data)
            
        except Exception as e:
            print(f"Error handling {exchange} WebSocket message: {e}") 