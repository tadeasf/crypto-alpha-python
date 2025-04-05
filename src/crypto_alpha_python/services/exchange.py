"""
Exchange service for handling exchange connections and data fetching.
"""
import asyncio
from datetime import datetime, UTC
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
import aiohttp

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
        else:
            # If API keys are not available, initialize with just the base URL for public endpoints
            try:
                self.binance_client = Spot()
                logger.info("Binance client initialized for public endpoints only")
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
            logger.warning("Coinbase API credentials not configured, some functionality may be limited")
    
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
    
    def _normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol to standard format."""
        # Ensure symbol ends with USDT if it doesn't have a quote currency
        if not (symbol.endswith('USDT') or symbol.endswith('USD')):
            return f"{symbol}USDT"
        return symbol
    
    def _format_symbol(self, symbol: str, exchange: str) -> str:
        """Format symbol according to exchange requirements."""
        if exchange == "coinbase":
            # Convert BTCUSDT -> BTC-USD
            if symbol.endswith('USDT'):
                # Remove USDT suffix and add -USD
                base = symbol[:-4]
                return f"{base}-USD"
            elif symbol.endswith('USD'):
                # Convert BTCUSD -> BTC-USD if not already in correct format
                if '-' not in symbol:
                    base = symbol[:-3]
                    return f"{base}-USD"
            return symbol  # Return as is if already properly formatted
        else:  # binance
            # Binance expects symbols like BTCUSDT (no dash)
            if '-' in symbol:
                # Convert BTC-USD -> BTCUSDT
                base, quote = symbol.split('-')
                if quote == 'USD':
                    return f"{base}USDT"
                return f"{base}{quote}"
            elif symbol.endswith('USD') and '-' not in symbol:
                # Convert BTCUSD -> BTCUSDT
                base = symbol[:-3]
                return f"{base}USDT"
            return symbol  # Return as is if already in correct format (BTCUSDT)

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
                # Format symbol for Binance (e.g., BTCUSDT)
                formatted_symbol = self._format_symbol(symbol, "binance").lower()
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
                
                # Convert Binance symbol format to our normalized format for display/storage
                # e.g., 'btcusdt' -> 'BTCUSDT'
                raw_symbol = data['s'].upper()
                
                # We store the market data with the normalized symbol 
                # to ensure consistent database queries
                market_data = MarketData(
                    exchange="binance",
                    symbol=raw_symbol,  # Use the original Binance format
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
                logger.info(f"Successfully stored Binance market data for {raw_symbol}: last_price={market_data.last_price}, volume={market_data.volume}")
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
                                    timestamp = datetime.now(UTC)
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
            # Format symbol for Binance API (e.g., BTCUSDT)
            formatted_symbol = self._format_symbol(symbol, "binance")
            logger.debug(f"Getting Binance ticker for {formatted_symbol}")
            
            # Get ticker data using the correct method
            ticker = self.binance_client.ticker_24hr(symbol=formatted_symbol)
            
            # Convert timestamp to datetime
            timestamp = datetime.fromtimestamp(ticker["closeTime"] / 1000)
            
            # Store the symbol in the format expected in the database
            stored_symbol = formatted_symbol
            
            # Create market data object
            market_data = MarketData(
                timestamp=timestamp,
                symbol=stored_symbol,
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
            # Format symbol for Coinbase API
            formatted_symbol = self._format_symbol(symbol, "coinbase")
            logger.debug(f"Getting Coinbase ticker for {formatted_symbol}")
            
            # Get product ticker using Advanced Trade API
            ticker = self.coinbase_client.get_product(formatted_symbol)
            
            # Get best bid/ask separately since it's not in the product response
            best_bid_ask = self.coinbase_client.get_best_bid_ask(product_ids=[formatted_symbol])
            
            # Handle timestamp
            timestamp_str = ticker.time if hasattr(ticker, 'time') else None
            if not timestamp_str:
                logger.warning(f"No timestamp in Coinbase ticker data for {symbol}, using current time")
                timestamp = datetime.now(UTC)
            else:
                # Convert ISO format timestamp to datetime
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            
            # Store the original symbol format in our database for consistency
            stored_symbol = symbol
            
            # Get best bid/ask from the response
            # Find the pricebook for our product
            pricebook = next((pb for pb in best_bid_ask.pricebooks if pb.product_id == formatted_symbol), None)
            
            if not pricebook or not hasattr(pricebook, 'bids') or not hasattr(pricebook, 'asks'):
                logger.warning(f"No pricebook data for {formatted_symbol}")
                # Default values if price book is missing
                best_bid = 0
                best_ask = 0
                best_bid_size = 0
                best_ask_size = 0
            else:
                # Get best bid/ask from the pricebook
                best_bid = pricebook.bids[0].price if pricebook.bids else 0
                best_ask = pricebook.asks[0].price if pricebook.asks else 0
                best_bid_size = pricebook.bids[0].size if pricebook.bids else 0
                best_ask_size = pricebook.asks[0].size if pricebook.asks else 0
            
            return MarketData(
                timestamp=timestamp,
                symbol=stored_symbol,
                exchange="coinbase",
                bid=float(best_bid),
                ask=float(best_ask),
                last_price=float(ticker.price),
                volume=float(ticker.volume_24h),
                bid_size=float(best_bid_size),
                ask_size=float(best_ask_size),
                trades_count=0,  # Coinbase doesn't provide this in ticker
                vwap=None,  # Coinbase doesn't provide this in ticker
                high=None,  # Coinbase doesn't provide this in ticker
                low=None,  # Coinbase doesn't provide this in ticker
                open=None,  # Coinbase doesn't provide this in ticker
                close=float(ticker.price),
            )
        except Exception as e:
            logger.error(f"Coinbase API error for {symbol}: {e}")
            return None
    
    async def validate_symbol(self, symbol: str) -> Dict[str, bool]:
        """
        Validate if a symbol exists on supported exchanges.
        
        Args:
            symbol: The symbol to validate (e.g., 'BTCUSDT', 'BTC-USD')
            
        Returns:
            A dictionary with exchange names as keys and boolean values indicating
            if the symbol is valid on that exchange.
        """
        result = {
            "binance": False,
            "coinbase": False
        }
        
        try:
            # Validate on Binance
            if self.binance_client:
                try:
                    # Format symbol for Binance
                    binance_symbol = self._format_symbol(symbol, "binance")
                    # Try to get exchange info for this symbol
                    exchange_info = self.binance_client.exchange_info(symbol=binance_symbol)
                    # If we get here without exception, symbol exists
                    if exchange_info and "symbols" in exchange_info:
                        for sym_info in exchange_info["symbols"]:
                            if sym_info["symbol"] == binance_symbol:
                                result["binance"] = True
                                break
                except Exception as e:
                    logger.warning(f"Symbol {symbol} validation failed on Binance: {e}")
            
            # Validate on Coinbase
            if self.coinbase_client:
                try:
                    # Format symbol for Coinbase
                    coinbase_symbol = self._format_symbol(symbol, "coinbase")
                    # Try to get product info
                    try:
                        product = self.coinbase_client.get_product(coinbase_symbol)
                        if product and hasattr(product, 'product_id'):
                            result["coinbase"] = True
                    except Exception as e:
                        if "product not found" not in str(e).lower():
                            logger.warning(f"Error checking Coinbase product: {e}")
                except Exception as e:
                    logger.warning(f"Symbol {symbol} validation failed on Coinbase: {e}")
            
        except Exception as e:
            logger.error(f"Error validating symbol {symbol}: {e}")
        
        return result

    async def get_available_symbols(self, limit: int = 20) -> Dict[str, List[str]]:
        """
        Get a list of available symbols from supported exchanges.
        
        Args:
            limit: Maximum number of symbols to return per exchange
            
        Returns:
            Dictionary with exchange names as keys and lists of symbols as values
        """
        result = {
            "binance": [],
            "coinbase": []
        }
        
        try:
            # Get Binance symbols
            if self.binance_client:
                try:
                    # Get exchange info
                    exchange_info = self.binance_client.exchange_info()
                    if exchange_info and "symbols" in exchange_info:
                        # Filter symbols to only include USDT pairs and status==TRADING
                        usdt_symbols = [
                            sym["symbol"] for sym in exchange_info["symbols"]
                            if sym["quoteAsset"] == "USDT" and sym["status"] == "TRADING"
                        ]
                        # Sort by symbol name and take the top ones
                        result["binance"] = sorted(usdt_symbols)[:limit]
                except Exception as e:
                    logger.error(f"Error getting Binance symbols: {e}")
            
            # Get Coinbase symbols using public API
            try:
                # Use the public API endpoint that doesn't require authentication
                url = "https://api.coinbase.com/api/v3/brokerage/market/products"
                params = {
                    "limit": limit,
                    "product_type": "SPOT"  # Focus on spot trading pairs
                }
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "products" in data:
                                # Extract product IDs from the response
                                coinbase_products = []
                                for product in data.get("products", []):
                                    if isinstance(product, dict) and "product_id" in product:
                                        # Filter to include only USD pairs
                                        product_id = product["product_id"]
                                        if product_id.endswith("-USD"):
                                            coinbase_products.append(product_id)
                                            
                                # Sort and take the top ones
                                result["coinbase"] = sorted(coinbase_products)[:limit]
                            else:
                                logger.warning("No 'products' field in Coinbase API response")
                        else:
                            logger.warning(f"Failed to get Coinbase products: {response.status}")
            except Exception as e:
                logger.error(f"Error getting Coinbase symbols via public API: {e}")
        
        except Exception as e:
            logger.error(f"Error getting available symbols: {e}")
            
        return result

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