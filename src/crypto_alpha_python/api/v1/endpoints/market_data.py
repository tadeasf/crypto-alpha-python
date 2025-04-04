"""
Market data endpoints for handling market data operations.
"""
from datetime import datetime, timedelta
from typing import List
from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.db.session import get_session
from crypto_alpha_python.models.market_data import MarketData, MarketDataRead
from crypto_alpha_python.services.market_data import get_market_data

router = APIRouter()

@router.get("/{symbol}", response_model=List[MarketDataRead])
async def read_market_data(
    symbol: str,
    start_time: datetime = Query(...),
    end_time: datetime = Query(...),
    exchange: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> List[MarketData]:
    """
    Retrieve market data for a specific symbol within a time range.
    """
    return await get_market_data(
        session=session,
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        exchange=exchange,
    )

@router.get("/spreads/{symbol}")
async def get_spreads(
    symbol: str,
    window: str = Query("5min", regex=r"^\d+[smhd]$"),
    exchange: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """
    Calculate bid-ask spreads over time windows.
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
    
    data = await get_market_data(
        session=session,
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        exchange=exchange,
    )
    
    # Calculate spreads
    spreads = [
        {
            "timestamp": d.timestamp,
            "spread": d.ask - d.bid,
            "spread_percentage": (d.ask - d.bid) / d.bid * 100,
        }
        for d in data
    ]
    
    return {
        "symbol": symbol,
        "window": window,
        "spreads": spreads,
    } 