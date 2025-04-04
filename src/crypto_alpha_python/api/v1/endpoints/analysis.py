"""
Analysis endpoints for market analysis.
"""
from datetime import datetime, timedelta
from typing import List
from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.db.session import get_session
from crypto_alpha_python.services.analysis import (
    calculate_volatility,
    calculate_order_book_imbalance,
    calculate_twap,
)

router = APIRouter()

@router.get("/volatility/{symbol}")
async def get_volatility(
    symbol: str,
    window: str = Query("1h", regex="^\d+[smhd]$"),
    exchange: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """
    Calculate rolling volatility for a symbol.
    """
    # Convert window string to timedelta
    value = int(window[:-1])
    unit = window[-1]
    delta = {
        "s": timedelta(seconds=value),
        "m": timedelta(minutes=value),
        "h": timedelta(hours=value),
        "d": timedelta(days=value),
    }[unit]
    
    end_time = datetime.utcnow()
    start_time = end_time - delta
    
    volatility = await calculate_volatility(
        session=session,
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        exchange=exchange,
    )
    
    return {
        "symbol": symbol,
        "window": window,
        "volatility": volatility,
    }

@router.get("/orderbook-imbalance/{symbol}")
async def get_order_book_imbalance(
    symbol: str,
    depth: float = Query(0.02, gt=0, le=0.1),
    exchange: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """
    Calculate order book imbalance at specified depth.
    """
    imbalance = await calculate_order_book_imbalance(
        session=session,
        symbol=symbol,
        depth=depth,
        exchange=exchange,
    )
    
    return {
        "symbol": symbol,
        "depth": depth,
        "imbalance": imbalance,
    }

@router.get("/twap/{symbol}")
async def get_twap(
    symbol: str,
    window: str = Query("1h", regex="^\d+[smhd]$"),
    exchange: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """
    Calculate Time-Weighted Average Price (TWAP).
    """
    # Convert window string to timedelta
    value = int(window[:-1])
    unit = window[-1]
    delta = {
        "s": timedelta(seconds=value),
        "m": timedelta(minutes=value),
        "h": timedelta(hours=value),
        "d": timedelta(days=value),
    }[unit]
    
    end_time = datetime.utcnow()
    start_time = end_time - delta
    
    twap = await calculate_twap(
        session=session,
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        exchange=exchange,
    )
    
    return {
        "symbol": symbol,
        "window": window,
        "twap": twap,
    } 