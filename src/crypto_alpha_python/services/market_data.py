"""
Market data service for handling market data operations.
"""
from datetime import datetime
from typing import List
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.models.market_data import MarketData

async def get_market_data(
    session: AsyncSession,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    exchange: str | None = None,
) -> List[MarketData]:
    """
    Retrieve market data for a specific symbol within a time range.
    """
    query = select(MarketData).where(
        MarketData.symbol == symbol,
        MarketData.timestamp >= start_time,
        MarketData.timestamp <= end_time,
    )
    
    if exchange:
        query = query.where(MarketData.exchange == exchange)
    
    result = await session.execute(query)
    return result.scalars().all()

async def create_market_data(
    session: AsyncSession,
    market_data: MarketData,
) -> MarketData:
    """
    Create new market data entry.
    """
    session.add(market_data)
    await session.commit()
    await session.refresh(market_data)
    return market_data

async def get_latest_market_data(
    session: AsyncSession,
    symbol: str,
    exchange: str | None = None,
) -> MarketData | None:
    """
    Get the latest market data for a symbol.
    """
    query = select(MarketData).where(MarketData.symbol == symbol)
    
    if exchange:
        query = query.where(MarketData.exchange == exchange)
    
    query = query.order_by(MarketData.timestamp.desc())
    result = await session.execute(query)
    return result.scalar_one_or_none() 