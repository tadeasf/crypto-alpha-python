"""
Exchange service for handling exchange connections and data fetching.
"""
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Set
import websockets
from binance.spot import Spot
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from coinbase.rest import RESTClient as CoinbaseClient
import time
import uuid
import jwt
import json
import logging

from crypto_alpha_python.core.config import settings
from crypto_alpha_python.models.market_data import MarketData

logger = logging.getLogger(__name__)

class ExchangeService:
    """Service for handling exchange connections and data fetching."""
    
    def __init__(self):
        """Initialize exchange connections."""
        self.binance_client = None
        self.binance_ws_client = None
        self.coinbase_client = None
        self.websocket_connections: Dict[str, websockets.WebSocketClientProtocol] = {}
        self.ws_clients: Dict[str, websockets.WebSocketClientProtocol] = {}
        self.subscribed_symbols: Set[str] = set()
        
        # Initialize Binance client if API keys are available
        if settings.BINANCE_API_KEY and settings.BINANCE_API_SECRET:
            try:
                self.binance_client = Spot(
                    api_key=settings.BINANCE_API_KEY,
                    api_secret=settings.BINANCE_API_SECRET,
                )
                logger.info("Binance client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Binance client: {e}")
        
        # Initialize Coinbase client if API keys are available
        if settings.COINBASE_API_KEY and settings.COINBASE_API_SECRET:
            try:
                # Ensure API keys are properly formatted
                api_key = settings.COINBASE_API_KEY.strip()
                api_secret = settings.COINBASE_API_SECRET.strip()
                
                if not api_key or not api_secret:
                    logger.warning("Coinbase API keys are empty after formatting")
                    return
                
                self.coinbase_client = CoinbaseClient(
                    api_key=api_key,
                    api_secret=api_secret,
                )
                logger.info("Coinbase client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Coinbase client: {e}")
                logger.error("Please ensure your Coinbase API keys are properly formatted and valid")
        else:
            logger.warning("Coinbase API credentials not configured, skipping Coinbase client initialization")
    
    def _generate_jwt(self) -> str:
        """Generate JWT token for Coinbase WebSocket authentication."""
        try:
            current_time = int(time.time())
            payload = {
                "iss": "cdp",
                "nbf": current_time,
                "exp": current_time + 120,  # JWT valid for 120 seconds
                "sub": settings.COINBASE_API_KEY,
            }
            headers = {
                "kid": settings.COINBASE_API_KEY,
                "nonce": uuid.uuid4().hex
            }
            
            # Ensure the API secret is properly formatted
            api_secret = settings.COINBASE_API_SECRET
            if not api_secret:
                raise ValueError("Coinbase API secret is empty")
            
            # Convert the API secret to bytes if it's not already
            if isinstance(api_secret, str):
                # Remove any whitespace and newlines
                api_secret = api_secret.strip()
                # Convert to bytes
                api_secret = api_secret.encode('utf-8')
            
            # Use ES256 algorithm as required by Coinbase
            return jwt.encode(
                payload,
                api_secret,
                algorithm="ES256",
                headers=headers
            )
        except Exception as e:
            logger.error(f"Failed to generate JWT token: {e}")
            raise
    
    def _format_symbol(self, symbol: str, exchange: str) -> str:
        """Format symbol according to exchange requirements."""
        if exchange == "coinbase":
            # Convert BTCUSDT -> BTC-USD
            base = symbol[:-4]  # Remove USDT
            return f"{base}-USD"
        else:  # binance
            return symbol.lower()  # Binance uses lowercase

    async def connect_binance_websocket(
        self,
        symbol: str,
    ) -> None:
        """Connect to Binance WebSocket stream."""
        try:
            if not self.binance_ws_client:
                logger.info("Initializing Binance WebSocket client...")
                # Create a synchronous wrapper for the async message handler
                def message_handler(client, message):
                    logger.debug(f"Received Binance WebSocket message: {message}")
                    # Get the current event loop
                    try:
                        loop = asyncio.get_event_loop()
                    except RuntimeError:
                        # If no event loop is running, create a new one
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                    loop.create_task(self._handle_binance_websocket_message(client, message))

                def on_close(_):
                    logger.info("Binance WebSocket connection closed")

                self.binance_ws_client = SpotWebsocketStreamClient(
                    on_message=message_handler,
                    on_close=on_close,
                    time_unit='millisecond'
                )
                logger.info("Binance WebSocket client initialized")

            if symbol not in self.subscribed_symbols:
                formatted_symbol = self._format_symbol(symbol, "binance")
                logger.info(f"Subscribing to Binance WebSocket for {formatted_symbol}...")
                # Subscribe to mini ticker stream for real-time updates
                self.binance_ws_client.mini_ticker(symbol=formatted_symbol)
                self.subscribed_symbols.add(symbol)
                logger.info(f"Successfully subscribed to Binance WebSocket for {formatted_symbol}")
        except Exception as e:
            logger.error(f"Failed to connect to Binance WebSocket for {symbol}: {e}")
            raise

    async def _handle_binance_websocket_message(self, _, message: Dict) -> None:
        """Handle incoming Binance WebSocket messages."""
        try:
            logger.debug(f"Processing Binance WebSocket message: {message}")
            if 'data' in message:
                data = message['data']
                logger.debug(f"Processing Binance WebSocket data: {data}")
                market_data = MarketData(
                    exchange="binance",
                    symbol=data['s'],
                    last_price=float(data['c']),  # Current price
                    volume=float(data['v']),  # Volume
                    bid=float(data['b']),  # Best bid price
                    ask=float(data['a']),  # Best ask price
                    bid_size=float(data['B']),  # Best bid size
                    ask_size=float(data['A']),  # Best ask size
                    timestamp=datetime.fromtimestamp(data['E'] / 1000),
                    trades_count=0,  # Not provided in mini ticker
                    vwap=None,  # Not provided in mini ticker
                    high=float(data.get('h', 0)),  # High price
                    low=float(data.get('l', 0)),  # Low price
                    open=float(data.get('o', 0)),  # Open price
                    close=float(data['c'])  # Close price (same as last price)
                )
                # Store market data in database using a new session
                from crypto_alpha_python.services.market_data import create_market_data
                from crypto_alpha_python.db.session import get_session
                async for session in get_session():
                    await create_market_data(session, market_data)
                logger.info(f"Successfully stored Binance market data for {market_data.symbol}: last_price={market_data.last_price}, volume={market_data.volume}")
            else:
                logger.debug(f"Received non-data message from Binance: {message}")
        except Exception as e:
            logger.error(f"Error handling Binance WebSocket message: {e}")
            logger.error(f"Message that caused error: {message}")
    
    async def connect_coinbase_websocket(
        self,
        symbol: str,
    ) -> None:
        """Connect to Coinbase WebSocket stream."""
        try:
            if not settings.COINBASE_API_KEY or not settings.COINBASE_API_SECRET:
                logger.warning("Coinbase API credentials not configured, skipping WebSocket connection")
                return

            if symbol not in self.ws_clients:
                # Convert symbol format (e.g., BTCUSDT -> BTC-USD)
                formatted_symbol = self._format_symbol(symbol, "coinbase")
                
                # Create WebSocket connection
                ws_url = "wss://advanced-trade-ws.coinbase.com"
                ws_client = await websockets.connect(ws_url)
                
                try:
                    # Generate JWT token
                    jwt_token = self._generate_jwt()
                    
                    # Subscribe to ticker_batch channel with proper message format
                    subscribe_message = {
                        "type": "subscribe",
                        "product_ids": [formatted_symbol],
                        "channel": "ticker_batch",  # Using ticker_batch for better performance
                        "jwt": jwt_token
                    }
                    
                    # Send subscription message
                    await ws_client.send(json.dumps(subscribe_message))
                    
                    # Store the connection
                    self.ws_clients[symbol] = ws_client
                    
                    # Start message handler task
                    asyncio.create_task(self._handle_coinbase_messages(symbol, ws_client))
                    
                    logger.info(f"Connected to Coinbase WebSocket for {formatted_symbol}")
                except Exception as e:
                    await ws_client.close()
                    raise
        except Exception as e:
            logger.error(f"Failed to connect to Coinbase WebSocket for {symbol}: {e}")
            raise
    
    async def _handle_coinbase_messages(self, symbol: str, ws_client: websockets.WebSocketClientProtocol) -> None:
        """Handle Coinbase WebSocket messages."""
        try:
            while True:
                message = await ws_client.recv()
                await self._handle_coinbase_websocket_message(json.loads(message))
        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"Coinbase WebSocket connection closed for {symbol}")
        except Exception as e:
            logger.error(f"Error handling Coinbase messages for {symbol}: {e}")
        finally:
            if symbol in self.ws_clients:
                del self.ws_clients[symbol]
    
    async def _handle_coinbase_websocket_message(self, msg: Dict) -> None:
        """Handle Coinbase WebSocket message."""
        try:
            logger.debug(f"Received Coinbase WebSocket message: {msg}")
            if msg.get("channel") == "ticker_batch" and "events" in msg:
                for event in msg["events"]:
                    if event.get("type") == "snapshot" and "tickers" in event:
                        for ticker in event["tickers"]:
                            try:
                                # Convert symbol format (e.g., BTC-USD -> BTCUSDT)
                                product_id = ticker['product_id']
                                base, quote = product_id.split('-')
                                symbol = f"{base}{quote}"  # Convert back to our format
                                
                                # Parse timestamp and ensure it's timezone-aware
                                timestamp_str = msg.get("timestamp")
                                if not timestamp_str:
                                    logger.warning("No timestamp in Coinbase message, using current time")
                                    timestamp = datetime.utcnow()
                                else:
                                    # Convert ISO format timestamp to datetime
                                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                                
                                # Create market data with proper type conversion
                                market_data = MarketData(
                                    exchange="coinbase",
                                    symbol=symbol,
                                    last_price=float(ticker["price"]),
                                    volume=float(ticker["volume_24_h"]),
                                    bid=float(ticker.get("best_bid", 0)),  # ticker_batch might not have these
                                    ask=float(ticker.get("best_ask", 0)),  # ticker_batch might not have these
                                    bid_size=float(ticker.get("best_bid_quantity", 0)),
                                    ask_size=float(ticker.get("best_ask_quantity", 0)),
                                    timestamp=timestamp,
                                    trades_count=0,  # Coinbase doesn't provide this
                                    vwap=None,  # Coinbase doesn't provide this
                                    high=float(ticker.get("high_24_h", 0)),
                                    low=float(ticker.get("low_24_h", 0)),
                                    open=None,  # Coinbase doesn't provide this
                                    close=float(ticker["price"])  # Use current price as close
                                )
                                
                                # Store market data in database using a new session
                                from crypto_alpha_python.services.market_data import create_market_data
                                from crypto_alpha_python.db.session import get_session
                                async for session in get_session():
                                    await create_market_data(session, market_data)
                                logger.info(f"Successfully stored Coinbase market data for {market_data.symbol}: last_price={market_data.last_price}, volume={market_data.volume}")
                            except KeyError as e:
                                logger.error(f"Missing required field in Coinbase ticker data: {e}")
                                continue
                            except ValueError as e:
                                logger.error(f"Error converting Coinbase data: {e}")
                                continue
                            except Exception as e:
                                logger.error(f"Unexpected error processing Coinbase ticker: {e}")
                                continue
            else:
                logger.debug(f"Received non-ticker message from Coinbase: {msg}")
        except Exception as e:
            logger.error(f"Error handling Coinbase WebSocket message: {e}")
            logger.error(f"Message that caused error: {msg}")
    
    async def get_binance_ticker(
        self,
        symbol: str,
    ) -> Optional[MarketData]:
        """Get Binance ticker data."""
        if not self.binance_client:
            logger.warning("Binance client not initialized")
            return None
        
        try:
            # Get ticker data using the correct method
            ticker = self.binance_client.ticker_24hr(symbol=symbol)
            
            # Convert timestamp to datetime
            timestamp = datetime.fromtimestamp(ticker["closeTime"] / 1000)
            
            # Create market data object
            market_data = MarketData(
                timestamp=timestamp,
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
            
            logger.debug(f"Retrieved Binance ticker data for {symbol}")
            return market_data
            
        except Exception as e:
            logger.error(f"Binance API error for {symbol}: {e}")
            return None
    
    async def get_coinbase_ticker(
        self,
        symbol: str,
    ) -> Optional[MarketData]:
        """Get Coinbase ticker data."""
        if not self.coinbase_client:
            logger.warning("Coinbase client not initialized")
            return None
        
        try:
            # Convert symbol format (e.g., BTCUSDT -> BTC-USD)
            base, quote = symbol[:-4], symbol[-4:]
            product_id = f"{base}-{quote}"
            
            # Get product ticker using Advanced Trade API
            ticker = self.coinbase_client.get_product(product_id)
            
            return MarketData(
                timestamp=datetime.fromisoformat(ticker["time"].replace("Z", "+00:00")),
                symbol=symbol,
                exchange="coinbase",
                bid=float(ticker["best_bid"]),
                ask=float(ticker["best_ask"]),
                last_price=float(ticker["price"]),
                volume=float(ticker["volume_24h"]),
                bid_size=float(ticker["best_bid_size"]),
                ask_size=float(ticker["best_ask_size"]),
                trades_count=0,  # Coinbase doesn't provide this in ticker
                vwap=None,  # Coinbase doesn't provide this in ticker
                high=None,  # Coinbase doesn't provide this in ticker
                low=None,  # Coinbase doesn't provide this in ticker
                open=None,  # Coinbase doesn't provide this in ticker
                close=float(ticker["price"]),
            )
        except Exception as e:
            logger.error(f"Coinbase API error: {e}")
            return None
    
    async def close_connections(self) -> None:
        """Close all WebSocket connections."""
        try:
            # Close Binance WebSocket client
            if self.binance_ws_client:
                self.binance_ws_client.stop()  # Use stop() instead of close()
                self.binance_ws_client = None
                logger.info("Binance WebSocket client stopped")

            # Close Coinbase WebSocket connections
            # Create a copy of the items to avoid dictionary size change during iteration
            ws_clients_copy = list(self.ws_clients.items())
            for symbol, ws in ws_clients_copy:
                try:
                    # Generate JWT token for unsubscribe
                    jwt_token = self._generate_jwt()
                    
                    # Unsubscribe message
                    unsubscribe_message = {
                        "type": "unsubscribe",
                        "product_ids": [f"{symbol[:-4]}-{symbol[-4:]}"],
                        "channel": "ticker_batch",
                        "jwt": jwt_token
                    }
                    
                    # Send unsubscribe message
                    await ws.send(json.dumps(unsubscribe_message))
                    
                    # Close the connection
                    await ws.close()
                    logger.info(f"Closed Coinbase WebSocket connection for {symbol}")
                except Exception as e:
                    logger.error(f"Error closing Coinbase WebSocket for {symbol}: {e}")
            
            self.ws_clients.clear()
            self.subscribed_symbols.clear()
            logger.info("All WebSocket connections closed")
        except Exception as e:
            logger.error(f"Error closing connections: {e}")
            raise

# Create global exchange service instance
exchange_service = ExchangeService() 