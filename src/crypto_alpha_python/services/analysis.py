"""
Market analysis service with various analysis functions.
"""
from datetime import datetime, timedelta
from typing import List
import numpy as np
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
import re
import logging

from crypto_alpha_python.models.market_data import MarketData
from crypto_alpha_python.services.market_data import get_market_data

logger = logging.getLogger(__name__)

def parse_window(window: str) -> timedelta:
    """
    Parse window string and convert to timedelta.
    
    Supports formats:
    - Nx seconds: e.g. "30s"
    - Nx minutes: e.g. "5min"
    - Nx hours: e.g. "2h"
    - Nx days: e.g. "3d"
    - Nx weeks: e.g. "1w"
    - Nx months: e.g. "2m" (approximate, using 30 days per month)
    """
    match = re.match(r"^(\d+)(min|h|d|w|m|s)$", window)
    if not match:
        raise ValueError(f"Invalid window format: {window}. Use patterns like '1min', '5min', '1h', '1d', '1w', '1m'.")
    
    value = int(match.group(1))
    unit = match.group(2)
    
    # Convert to timedelta based on unit
    if unit == "s":
        return timedelta(seconds=value)
    elif unit == "min":
        return timedelta(minutes=value)
    elif unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)
    elif unit == "w":
        return timedelta(weeks=value)
    elif unit == "m":
        # Approximate months as 30 days
        return timedelta(days=30 * value)
    else:
        # This should never happen due to regex validation
        raise ValueError(f"Unknown time unit: {unit}")

async def calculate_volatility(
    session: AsyncSession,
    symbol: str,
    start_time: datetime = None,
    end_time: datetime = None,
    exchange: str | None = None,
    window: str | None = None,
) -> float:
    """
    Calculate rolling volatility for a symbol.
    
    Either provide start_time and end_time, or provide window.
    Returns the annualized volatility as a decimal (e.g., 0.15 for 15%).
    """
    if start_time is None or end_time is None:
        if window:
            try:
                delta = parse_window(window)
                end_time = datetime.utcnow()
                start_time = end_time - delta
                logger.debug(f"Using time window {window} for volatility calculation: {start_time} to {end_time}")
            except ValueError as e:
                logger.error(f"Error parsing window: {e}")
                return 0.0  # Return zero on invalid window format
        else:
            # Default to 1 day if neither time range nor window provided
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=1)
            logger.debug("Using default 1-day window for volatility calculation")
    
    data = await get_market_data(
        session=session,
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        exchange=exchange,
    )
    
    if not data:
        logger.warning(f"No market data found for {symbol} in the specified time range")
        return 0.0
    
    # Calculate returns
    prices = [d.last_price for d in data]
    
    if len(prices) < 2:
        logger.warning(f"Insufficient data points for {symbol} to calculate volatility")
        return 0.0
        
    # Calculate log returns
    returns = np.diff(np.log(prices))
    
    # Calculate volatility (annualized)
    # Scale factor depends on data frequency
    time_diff = (data[-1].timestamp - data[0].timestamp).total_seconds()
    samples_per_day = (86400 / (time_diff / len(returns))) if time_diff > 0 else 252
    annualization_factor = np.sqrt(samples_per_day)
    
    volatility = np.std(returns) * annualization_factor
    
    logger.debug(f"Calculated volatility for {symbol}: {volatility:.6f}")
    return float(volatility)

async def calculate_order_book_imbalance(
    session: AsyncSession,
    symbol: str,
    depth: float,
    exchange: str | None = None,
) -> float:
    """
    Calculate order book imbalance at specified depth.
    
    Returns a value between -1 and 1, where:
    - Positive values indicate buying pressure (more bids than asks)
    - Negative values indicate selling pressure (more asks than bids)
    - Values closer to 0 indicate a balanced order book
    """
    # Get latest market data
    query = select(MarketData).where(MarketData.symbol == symbol)
    if exchange:
        query = query.where(MarketData.exchange == exchange)
    query = query.order_by(MarketData.timestamp.desc())
    result = await session.execute(query)
    latest_data = result.scalar_one_or_none()
    
    if not latest_data:
        logger.warning(f"No market data found for {symbol}")
        return 0.0
    
    # Calculate imbalance
    mid_price = (latest_data.bid + latest_data.ask) / 2
    bid_depth = latest_data.bid_size
    ask_depth = latest_data.ask_size
    
    # Calculate imbalance ratio (-1 to 1)
    total_depth = bid_depth + ask_depth
    if total_depth == 0:
        logger.warning(f"Zero depth for {symbol}, returning 0.0 imbalance")
        return 0.0
    
    imbalance = (bid_depth - ask_depth) / total_depth
    logger.debug(f"Calculated order book imbalance for {symbol}: {imbalance:.4f}")
    return float(imbalance)

async def calculate_twap(
    session: AsyncSession,
    symbol: str,
    start_time: datetime = None,
    end_time: datetime = None,
    exchange: str | None = None,
    window: str | None = None,
) -> float:
    """
    Calculate Time-Weighted Average Price (TWAP).
    
    Either provide start_time and end_time, or provide window.
    Returns the time-weighted average price for the specified time period.
    """
    if start_time is None or end_time is None:
        if window:
            try:
                delta = parse_window(window)
                end_time = datetime.utcnow()
                start_time = end_time - delta
                logger.debug(f"Using time window {window} for TWAP calculation: {start_time} to {end_time}")
            except ValueError as e:
                logger.error(f"Error parsing window: {e}")
                return 0.0  # Return zero on invalid window format
        else:
            # Default to 1 day if neither time range nor window provided
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=1)
            logger.debug("Using default 1-day window for TWAP calculation")
    
    data = await get_market_data(
        session=session,
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        exchange=exchange,
    )
    
    if not data:
        logger.warning(f"No market data found for {symbol} in the specified time range")
        return 0.0
    
    # Calculate standard TWAP (simple average of prices over time)
    if all(d.volume == 0 for d in data):
        # If no volume data, use simple time-weighted average
        twap = sum(d.last_price for d in data) / len(data)
        logger.debug(f"Calculated TWAP (no volume data) for {symbol}: {twap:.8f}")
    else:
        # Calculate volume-weighted TWAP if volume data is available
        total_volume = sum(d.volume for d in data)
        if total_volume == 0:
            twap = sum(d.last_price for d in data) / len(data)
            logger.debug(f"Calculated TWAP (zero total volume) for {symbol}: {twap:.8f}")
        else:
            twap = sum(d.last_price * d.volume for d in data) / total_volume
            logger.debug(f"Calculated volume-weighted TWAP for {symbol}: {twap:.8f}")
    
    return float(twap) 