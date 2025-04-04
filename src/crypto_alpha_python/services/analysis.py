"""
Market analysis service with various analysis functions.
"""
from datetime import datetime
from typing import List
import numpy as np
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.models.market_data import MarketData
from crypto_alpha_python.services.market_data import get_market_data

async def calculate_volatility(
    session: AsyncSession,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    exchange: str | None = None,
) -> float:
    """
    Calculate rolling volatility for a symbol.
    """
    data = await get_market_data(
        session=session,
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        exchange=exchange,
    )
    
    if not data:
        return 0.0
    
    # Calculate returns
    prices = [d.last_price for d in data]
    returns = np.diff(np.log(prices))
    
    # Calculate volatility (annualized)
    volatility = np.std(returns) * np.sqrt(252)  # Assuming daily data
    
    return float(volatility)

async def calculate_order_book_imbalance(
    session: AsyncSession,
    symbol: str,
    depth: float,
    exchange: str | None = None,
) -> float:
    """
    Calculate order book imbalance at specified depth.
    """
    # Get latest market data
    query = select(MarketData).where(MarketData.symbol == symbol)
    if exchange:
        query = query.where(MarketData.exchange == exchange)
    query = query.order_by(MarketData.timestamp.desc())
    result = await session.execute(query)
    latest_data = result.scalar_one_or_none()
    
    if not latest_data:
        return 0.0
    
    # Calculate imbalance
    mid_price = (latest_data.bid + latest_data.ask) / 2
    bid_depth = latest_data.bid_size
    ask_depth = latest_data.ask_size
    
    # Calculate imbalance ratio (-1 to 1)
    total_depth = bid_depth + ask_depth
    if total_depth == 0:
        return 0.0
    
    imbalance = (bid_depth - ask_depth) / total_depth
    return float(imbalance)

async def calculate_twap(
    session: AsyncSession,
    symbol: str,
    start_time: datetime,
    end_time: datetime,
    exchange: str | None = None,
) -> float:
    """
    Calculate Time-Weighted Average Price (TWAP).
    """
    data = await get_market_data(
        session=session,
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        exchange=exchange,
    )
    
    if not data:
        return 0.0
    
    # Calculate TWAP
    total_volume = sum(d.volume for d in data)
    if total_volume == 0:
        return 0.0
    
    twap = sum(d.last_price * d.volume for d in data) / total_volume
    return float(twap) 