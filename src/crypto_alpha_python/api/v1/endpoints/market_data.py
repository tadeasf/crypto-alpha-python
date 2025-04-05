"""
Market data endpoints for handling market data operations.
"""
from datetime import datetime, timedelta
from typing import List
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlmodel.ext.asyncio.session import AsyncSession
import re
import logging

from crypto_alpha_python.db.session import get_session
from crypto_alpha_python.models.market_data import MarketData, MarketDataRead
from crypto_alpha_python.services.market_data import get_market_data
from crypto_alpha_python.core.tasks import task_manager
from crypto_alpha_python.services.exchange import exchange_service
from crypto_alpha_python.services.collector import collector

logger = logging.getLogger(__name__)

router = APIRouter()

def normalize_symbol(symbol: str) -> str:
    """
    Normalize symbol to a consistent format for database queries.
    
    This normalizes to the appropriate format based on where the query is being sent.
    For Binance formats like BTCUSDT, we keep it as is.
    For Coinbase formats like BTC-USD, we keep it as is.
    But we convert between formats if needed.
    """
    # If symbol is in Binance format like BTCUSDT
    if symbol.upper().endswith('USDT') and '-' not in symbol:
        return symbol.upper()
    
    # If symbol is in Coinbase format like BTC-USD
    if '-' in symbol and symbol.upper().endswith('-USD'):
        return symbol.upper()
    
    # If it's just a base currency like BTC or ETH
    if not any(x in symbol for x in ['USD', 'USDT', '-', 'BTC']):
        # Default to Binance format
        return f"{symbol.upper()}USDT"
    
    # If it's like BTCUSD, convert to BTCUSDT for Binance
    if symbol.upper().endswith('USD') and '-' not in symbol:
        base = symbol[:-3].upper()
        return f"{base}USDT"
    
    return symbol.upper()  # Default case

@router.get("/available-symbols")
async def get_available_symbols(
    limit: int = Query(20, description="Maximum number of symbols to return per exchange"),
) -> dict:
    """
    Get a list of available symbols from supported exchanges.
    
    This endpoint can be used to discover valid symbols that can be used 
    for data collection.
    """
    try:
        symbols = await exchange_service.get_available_symbols(limit=limit)
        return {
            "available_symbols": symbols
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get available symbols: {str(e)}",
        )

@router.get("/collect/symbols")
async def get_collected_symbols(
    session: AsyncSession = Depends(get_session),
) -> dict:
    """Get a list of symbols currently being collected."""
    try:
        # Get collector instance
        if not collector:
            return {
                "symbols": [],
                "default_symbols": task_manager.default_symbols,
                "is_collecting": False
            }
        
        return {
            "symbols": list(collector.symbols),
            "default_symbols": list(collector.default_symbols),
            "is_collecting": collector.is_running
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get collected symbols: {str(e)}",
        )

@router.get("/{symbol}", response_model=List[MarketDataRead])
async def read_market_data(
    symbol: str,
    start_time: str = Query(..., description="Start time in ddmmyyyy format"),
    end_time: str = Query(..., description="End time in ddmmyyyy format"),
    exchange: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> List[MarketData]:
    """
    Retrieve market data for a specific symbol within a time range.
    """
    try:
        # Parse date strings in ddmmyyyy format
        start_datetime = datetime.strptime(start_time, "%d%m%Y")
        end_datetime = datetime.strptime(end_time, "%d%m%Y")
        # Set end time to end of day
        end_datetime = end_datetime.replace(hour=23, minute=59, second=59)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Please use ddmmyyyy format."
        )
    
    # Normalize symbol for consistency
    normalized_symbol = normalize_symbol(symbol)
    logger.info(f"Normalized symbol {symbol} to {normalized_symbol}")
    
    # Get the data
    data = await get_market_data(
        session=session,
        symbol=normalized_symbol,
        start_time=start_datetime,
        end_time=end_datetime,
        exchange=exchange,
    )
    
    # Log the data sources for debugging
    if data:
        exchanges = set(item.exchange for item in data)
        logger.info(f"Retrieved {len(data)} market data records for {normalized_symbol} from exchanges: {exchanges}")
    else:
        logger.warning(f"No market data found for {normalized_symbol} between {start_datetime} and {end_datetime}")
    
    return data

@router.get("/spreads/{symbol}")
async def get_spreads(
    symbol: str,
    window: str = Query("5min", description="Time window (e.g., '1min', '5min', '1h', '1d')"),
    exchange: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """
    Calculate the spread between the lowest and highest asking prices across all exchanges.
    """
    # Parse window string using regex to handle formats like "1min", "5min", "1h", "1d"
    match = re.match(r"^(\d+)(min|h|d|s)$", window)
    if not match:
        raise HTTPException(
            status_code=400,
            detail="Invalid window format. Use patterns like '1min', '5min', '1h', '1d'."
        )
    
    value = int(match.group(1))
    unit = match.group(2)
    
    # Convert to standard unit character
    unit_map = {"min": "m", "h": "h", "d": "d", "s": "s"}
    std_unit = unit_map.get(unit, "m")
    
    # Calculate timedelta
    delta = {
        "s": timedelta(seconds=value),
        "m": timedelta(minutes=value),
        "h": timedelta(hours=value),
        "d": timedelta(days=value),
    }[std_unit]
    
    end_time = datetime.utcnow()
    start_time = end_time - delta
    
    logger.info(f"Fetching spread data for {symbol} from {start_time} to {end_time}")
    
    # Normalize symbol to consistent format
    normalized_symbol = normalize_symbol(symbol)
    logger.info(f"Normalized symbol {symbol} to {normalized_symbol}")
    
    data = await get_market_data(
        session=session,
        symbol=normalized_symbol,
        start_time=start_time,
        end_time=end_time,
        exchange=exchange,
    )
    
    logger.info(f"Retrieved {len(data)} records for {normalized_symbol}")
    
    # Find valid data points with ask prices
    valid_points = [p for p in data if p.ask and p.ask > 0]
    
    if not valid_points:
        return {
            "symbol": normalized_symbol,
            "window": window,
            "message": "No valid ask prices found in the specified time window"
        }
    
    # Find lowest asking price
    lowest_ask_point = min(valid_points, key=lambda p: p.ask)
    lowest_ask = lowest_ask_point.ask
    lowest_ask_exchange = lowest_ask_point.exchange
    lowest_ask_timestamp = lowest_ask_point.timestamp
    
    # Find highest asking price
    highest_ask_point = max(valid_points, key=lambda p: p.ask)
    highest_ask = highest_ask_point.ask
    highest_ask_exchange = highest_ask_point.exchange
    highest_ask_timestamp = highest_ask_point.timestamp
    
    # Calculate the spread between lowest and highest ask
    ask_spread = highest_ask - lowest_ask
    ask_spread_percentage = (ask_spread / lowest_ask) * 100
    
    # Create the response
    return {
        "symbol": normalized_symbol,
        "window": window,
        "lowest_ask": {
            "price": lowest_ask,
            "exchange": lowest_ask_exchange,
            "timestamp": lowest_ask_timestamp
        },
        "highest_ask": {
            "price": highest_ask,
            "exchange": highest_ask_exchange,
            "timestamp": highest_ask_timestamp
        },
        "ask_spread": ask_spread,
        "ask_spread_percentage": ask_spread_percentage
    }

@router.post("/collect/{symbol}")
async def start_collection(
    symbol: str,
    exchange: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """
    Start collecting market data for a symbol.
    
    This endpoint validates that the symbol exists on either Binance or Coinbase 
    before adding it to the collection.
    """
    try:
        # Normalize the symbol
        normalized_symbol = normalize_symbol(symbol)
        
        # Validate that the symbol exists on at least one exchange
        validation_result = await exchange_service.validate_symbol(normalized_symbol)
        
        if not (validation_result.get("binance") or validation_result.get("coinbase")):
            valid_symbols = await exchange_service.get_available_symbols(limit=10)
            raise HTTPException(
                status_code=400,
                detail={
                    "error": f"Symbol {normalized_symbol} is not valid on any supported exchange",
                    "valid_examples": {
                        "binance": valid_symbols.get("binance", [])[:5],
                        "coinbase": valid_symbols.get("coinbase", [])[:5],
                    }
                }
            )
        
        # Add the symbol to collection
        await task_manager.add_symbol(
            session=session,
            symbol=normalized_symbol,
        )
        
        return {
            "message": f"Started collecting market data for {normalized_symbol}",
            "symbol": normalized_symbol,
            "validity": validation_result
        }
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start collection: {str(e)}",
        )

@router.delete("/collect/{symbol}")
async def stop_collection(
    symbol: str,
) -> dict:
    """
    Stop collecting market data for a specific symbol.
    
    This endpoint will not allow removing default symbols
    (BTCUSDT, ETHUSDT) which are protected.
    """
    try:
        # Normalize the symbol
        normalized_symbol = normalize_symbol(symbol)
        
        # Try to remove the symbol
        result = await task_manager.remove_symbol(normalized_symbol)
        
        if not result:
            # If symbol couldn't be removed (e.g., it's a default symbol)
            return {
                "message": f"Could not remove market data collection for {normalized_symbol}. It may be a protected default symbol or not currently being collected.",
                "symbol": normalized_symbol,
                "removed": False
            }
        
        return {
            "message": f"Stopped collecting market data for {normalized_symbol}",
            "symbol": normalized_symbol,
            "removed": True
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop collection: {str(e)}",
        ) 