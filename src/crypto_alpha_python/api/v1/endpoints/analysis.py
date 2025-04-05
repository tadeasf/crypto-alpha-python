"""
Analysis endpoints for market analysis.
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlmodel.ext.asyncio.session import AsyncSession
import re

from crypto_alpha_python.db.session import get_session
from crypto_alpha_python.services.analysis import (
    calculate_volatility,
    calculate_order_book_imbalance,
    calculate_twap,
)

router = APIRouter()

def validate_window(window: str) -> bool:
    """Validate time window format."""
    # Support extended formats including weeks and months
    return bool(re.match(r"^(\d+)(min|h|d|w|m|s)$", window))

@router.get("/volatility/{symbol}")
async def get_volatility(
    symbol: str,
    window: str = Query("1h", description="Time window (e.g., '1min', '5min', '1h', '1d', '1w', '1m')"),
    exchange: str | None = Query(None, description="Exchange name (e.g., 'binance', 'coinbase')"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """
    Calculate rolling volatility for a symbol.
    
    Returns annualized volatility as a percentage for the given symbol and time window.
    """
    # Validate window format
    if not validate_window(window):
        raise HTTPException(
            status_code=400,
            detail="Invalid window format. Use patterns like '1min', '5min', '1h', '1d', '1w', '1m'."
        )
    
    # Validate symbol
    if not symbol:
        raise HTTPException(
            status_code=400,
            detail="Symbol is required."
        )
    
    # Uppercase the symbol for consistency
    symbol = symbol.upper()
    
    volatility = await calculate_volatility(
        session=session,
        symbol=symbol,
        window=window,
        exchange=exchange,
    )
    
    # Convert to percentage
    volatility_percent = volatility * 100
    
    return {
        "symbol": symbol,
        "exchange": exchange or "all exchanges",
        "window": window,
        "volatility": round(volatility_percent, 2),
        "volatility_description": f"{round(volatility_percent, 2)}% (annualized)",
        "calculation_time": datetime.utcnow().isoformat(),
    }

@router.get("/orderbook-imbalance/{symbol}")
async def get_order_book_imbalance(
    symbol: str,
    depth: float = Query(0.02, gt=0, le=0.1, description="Depth of the order book (0.01 = 1%)"),
    exchange: str | None = Query(None, description="Exchange name (e.g., 'binance', 'coinbase')"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """
    Calculate order book imbalance at specified depth.
    
    Returns a value between -1 and 1, where:
    - Positive values indicate buying pressure (more bids than asks)
    - Negative values indicate selling pressure (more asks than bids)
    - Values closer to 0 indicate a balanced order book
    """
    # Validate symbol
    if not symbol:
        raise HTTPException(
            status_code=400,
            detail="Symbol is required."
        )
    
    # Uppercase the symbol for consistency
    symbol = symbol.upper()
    
    imbalance = await calculate_order_book_imbalance(
        session=session,
        symbol=symbol,
        depth=depth,
        exchange=exchange,
    )
    
    # Create a descriptive message based on the imbalance value
    if imbalance > 0.5:
        description = "Strong buying pressure"
    elif imbalance > 0.2:
        description = "Moderate buying pressure"
    elif imbalance > 0:
        description = "Slight buying pressure"
    elif imbalance < -0.5:
        description = "Strong selling pressure"
    elif imbalance < -0.2:
        description = "Moderate selling pressure" 
    elif imbalance < 0:
        description = "Slight selling pressure"
    else:
        description = "Balanced order book"
    
    return {
        "symbol": symbol,
        "exchange": exchange or "all exchanges",
        "depth": f"{depth * 100}%",
        "imbalance": round(imbalance, 4),
        "imbalance_description": description,
        "calculation_time": datetime.utcnow().isoformat(),
    }

@router.get("/twap/{symbol}")
async def get_twap(
    symbol: str,
    window: str = Query("1h", description="Time window (e.g., '1min', '5min', '1h', '1d', '1w', '1m')"),
    exchange: str | None = Query(None, description="Exchange name (e.g., 'binance', 'coinbase')"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    """
    Calculate Time-Weighted Average Price (TWAP).
    
    Returns the time-weighted average price for the specified symbol and time window.
    """
    # Validate window format
    if not validate_window(window):
        raise HTTPException(
            status_code=400,
            detail="Invalid window format. Use patterns like '1min', '5min', '1h', '1d', '1w', '1m'."
        )
    
    # Validate symbol
    if not symbol:
        raise HTTPException(
            status_code=400,
            detail="Symbol is required."
        )
    
    # Uppercase the symbol for consistency
    symbol = symbol.upper()
    
    twap = await calculate_twap(
        session=session,
        symbol=symbol,
        window=window,
        exchange=exchange,
    )
    
    return {
        "symbol": symbol,
        "exchange": exchange or "all exchanges",
        "window": window,
        "twap": round(twap, 8),
        "calculation_time": datetime.utcnow().isoformat(),
    } 