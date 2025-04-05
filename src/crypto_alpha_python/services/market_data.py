"""
Market data service for handling market data operations.
"""
from datetime import datetime
from typing import List
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.models.market_data import MarketData
from crypto_alpha_python.services.redis import redis_service

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
    # Generate cache key
    cache_key = redis_service.generate_key(
        prefix="market_data",
        symbol=symbol,
        window=f"{start_time.isoformat()}_{end_time.isoformat()}",
        exchange=exchange
    )
    
    # Try to get from cache first
    cached_data = await redis_service.get(cache_key)
    if cached_data:
        return [MarketData(**item) for item in cached_data]
    
    # If not in cache, query database
    query = select(MarketData).where(
        MarketData.symbol == symbol,
        MarketData.timestamp >= start_time,
        MarketData.timestamp <= end_time,
    )
    
    if exchange:
        query = query.where(MarketData.exchange == exchange)
    
    result = await session.execute(query)
    data = result.scalars().all()
    
    # Cache the results for 5 minutes
    if data:
        await redis_service.set(
            cache_key,
            [item.dict() for item in data],
            expire_seconds=300
        )
    
    return data

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
    
    # Invalidate related caches
    await redis_service.delete(
        redis_service.generate_key(
            prefix="market_data",
            symbol=market_data.symbol,
            exchange=market_data.exchange
        )
    )
    
    return market_data

async def get_latest_market_data(
    session: AsyncSession,
    symbol: str,
    exchange: str | None = None,
) -> MarketData | None:
    """
    Get the latest market data for a symbol.
    """
    # Generate cache key
    cache_key = redis_service.generate_key(
        prefix="latest_market_data",
        symbol=symbol,
        exchange=exchange
    )
    
    # Try to get from cache first
    cached_data = await redis_service.get(cache_key)
    if cached_data:
        return MarketData(**cached_data)
    
    # If not in cache, query database
    query = select(MarketData).where(MarketData.symbol == symbol)
    
    if exchange:
        query = query.where(MarketData.exchange == exchange)
    
    query = query.order_by(MarketData.timestamp.desc())
    result = await session.execute(query)
    data = result.scalar_one_or_none()
    
    # Cache the result for 1 minute
    if data:
        await redis_service.set(
            cache_key,
            data.dict(),
            expire_seconds=60
        )
    
    return data 