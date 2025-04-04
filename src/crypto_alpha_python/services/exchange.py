"""
Exchange service for handling exchange connections and data fetching.
"""
import asyncio
from datetime import datetime
from typing import Dict, List, Optional
import websockets
from binance.client import Client as BinanceClient
from coinbase.rest import RESTClient as CoinbaseClient

from crypto_alpha_python.core.config import settings
from crypto_alpha_python.models.market_data import MarketData
from crypto_alpha_python.services.market_data import create_market_data

class ExchangeService:
    """Service for handling exchange connections and data fetching."""
    
    def __init__(self):
        """Initialize exchange connections."""
        self.binance_client = None
        self.coinbase_client = None
        self.websocket_connections: Dict[str, websockets.WebSocketClientProtocol] = {}
        
        if settings.BINANCE_API_KEY and settings.BINANCE_API_SECRET:
            self.binance_client = BinanceClient(
                settings.BINANCE_API_KEY,
                settings.BINANCE_API_SECRET,
            )
        
        if settings.COINBASE_API_KEY and settings.COINBASE_API_SECRET:
            self.coinbase_client = CoinbaseClient(
                settings.COINBASE_API_KEY,
                settings.COINBASE_API_SECRET,
            )
    
    async def connect_binance_websocket(
        self,
        symbol: str,
        callback,
    ) -> None:
        """Connect to Binance WebSocket stream."""
        stream = f"{symbol.lower()}@ticker"
        url = f"wss://stream.binance.com:9443/ws/{stream}"
        
        while True:
            try:
                async with websockets.connect(url) as websocket:
                    self.websocket_connections[f"binance_{symbol}"] = websocket
                    while True:
                        message = await websocket.recv()
                        await callback(message)
            except Exception as e:
                print(f"Binance WebSocket error: {e}")
                await asyncio.sleep(5)  # Wait before reconnecting
    
    async def connect_coinbase_websocket(
        self,
        symbol: str,
        callback,
    ) -> None:
        """Connect to Coinbase WebSocket stream."""
        url = "wss://ws-feed.pro.coinbase.com"
        
        while True:
            try:
                async with websockets.connect(url) as websocket:
                    self.websocket_connections[f"coinbase_{symbol}"] = websocket
                    
                    # Subscribe to ticker channel
                    subscribe_message = {
                        "type": "subscribe",
                        "product_ids": [symbol],
                        "channels": ["ticker"],
                    }
                    await websocket.send(str(subscribe_message))
                    
                    while True:
                        message = await websocket.recv()
                        await callback(message)
            except Exception as e:
                print(f"Coinbase WebSocket error: {e}")
                await asyncio.sleep(5)  # Wait before reconnecting
    
    async def get_binance_ticker(
        self,
        symbol: str,
    ) -> Optional[MarketData]:
        """Get Binance ticker data."""
        if not self.binance_client:
            return None
        
        try:
            ticker = self.binance_client.get_ticker(symbol=symbol)
            return MarketData(
                timestamp=datetime.fromtimestamp(ticker["time"] / 1000),
                symbol=symbol,
                exchange="binance",
                bid=float(ticker["bidPrice"]),
                ask=float(ticker["askPrice"]),
                last_price=float(ticker["lastPrice"]),
                volume=float(ticker["volume"]),
                bid_size=float(ticker["bidQty"]),
                ask_size=float(ticker["askQty"]),
                trades_count=int(ticker["count"]),
                vwap=float(ticker["weightedAvgPrice"]),
                high=float(ticker["highPrice"]),
                low=float(ticker["lowPrice"]),
                open=float(ticker["openPrice"]),
                close=float(ticker["lastPrice"]),
            )
        except Exception as e:
            print(f"Binance API error: {e}")
            return None
    
    async def get_coinbase_ticker(
        self,
        symbol: str,
    ) -> Optional[MarketData]:
        """Get Coinbase ticker data."""
        if not self.coinbase_client:
            return None
        
        try:
            ticker = self.coinbase_client.get_product_ticker(symbol)
            return MarketData(
                timestamp=datetime.fromisoformat(ticker["time"].replace("Z", "+00:00")),
                symbol=symbol,
                exchange="coinbase",
                bid=float(ticker["bid"]),
                ask=float(ticker["ask"]),
                last_price=float(ticker["price"]),
                volume=float(ticker["volume"]),
                bid_size=float(ticker["bid_size"]),
                ask_size=float(ticker["ask_size"]),
                trades_count=0,  # Coinbase doesn't provide this in ticker
                vwap=None,  # Coinbase doesn't provide this in ticker
                high=None,  # Coinbase doesn't provide this in ticker
                low=None,  # Coinbase doesn't provide this in ticker
                open=None,  # Coinbase doesn't provide this in ticker
                close=float(ticker["price"]),
            )
        except Exception as e:
            print(f"Coinbase API error: {e}")
            return None
    
    async def close_connections(self) -> None:
        """Close all WebSocket connections."""
        for websocket in self.websocket_connections.values():
            await websocket.close()
        self.websocket_connections.clear()

# Create global exchange service instance
exchange_service = ExchangeService() 