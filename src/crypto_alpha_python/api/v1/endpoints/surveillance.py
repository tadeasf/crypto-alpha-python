"""
Market surveillance endpoints for handling market surveillance operations.
"""
from typing import Dict
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlmodel.ext.asyncio.session import AsyncSession
import re

from crypto_alpha_python.db.session import get_session
from crypto_alpha_python.services.surveillance import MarketSurveillance
from crypto_alpha_python.models.surveillance import (
    VolumeSpikeResponse,
    PriceJumpResponse,
    WashTradingResponse,
)

router = APIRouter()

def validate_window(window: str) -> bool:
    """Validate time window format."""
    # Support extended formats including weeks and months
    return bool(re.match(r"^(\d+)(min|h|d|w|m|s)$", window))

@router.get("/volume-spike/{symbol}", response_model=VolumeSpikeResponse)
async def detect_volume_spike(
    symbol: str,
    threshold: float = Query(3.0, gt=0, description="Z-score threshold for volume spike detection (e.g., 3.0)"),
    window: str = Query("1h", description="Time window (e.g., '1min', '5min', '1h', '1d', '1w', '1m')"),
    exchange: str | None = Query(None, description="Exchange name (e.g., 'binance', 'coinbase')"),
    session: AsyncSession = Depends(get_session),
) -> VolumeSpikeResponse:
    """
    Detect volume spikes in market data.
    
    Returns information about volume spikes where the volume exceeds the average
    by more than the specified threshold (measured in standard deviations).
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
    
    surveillance = MarketSurveillance(session)
    return await surveillance.detect_volume_spike(
        symbol=symbol,
        threshold=threshold,
        window=window,
        exchange=exchange,
    )

@router.get("/price-jump/{symbol}", response_model=PriceJumpResponse)
async def detect_price_jump(
    symbol: str,
    threshold: float = Query(0.02, gt=0, description="Price change threshold as a decimal (e.g., 0.02 = 2%)"),
    window: str = Query("1h", description="Time window (e.g., '1min', '5min', '1h', '1d', '1w', '1m')"),
    exchange: str | None = Query(None, description="Exchange name (e.g., 'binance', 'coinbase')"),
    session: AsyncSession = Depends(get_session),
) -> PriceJumpResponse:
    """
    Detect significant price jumps.
    
    Returns information about price jumps where the percentage change
    exceeds the specified threshold.
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
    
    surveillance = MarketSurveillance(session)
    return await surveillance.detect_price_jump(
        symbol=symbol,
        threshold=threshold,
        window=window,
        exchange=exchange,
    )

@router.get("/wash-trading/{symbol}", response_model=WashTradingResponse)
async def detect_wash_trading(
    symbol: str,
    window: str = Query("1h", description="Time window (e.g., '1min', '5min', '1h', '1d', '1w', '1m')"),
    exchange: str | None = Query(None, description="Exchange name (e.g., 'binance', 'coinbase')"),
    session: AsyncSession = Depends(get_session),
) -> WashTradingResponse:
    """
    Detect potential wash trading patterns.
    
    Analyzes trading patterns to identify suspicious activity that might
    indicate wash trading, such as rapid price reversals with unusual volume.
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
    
    surveillance = MarketSurveillance(session)
    return await surveillance.detect_wash_trading(
        symbol=symbol,
        window=window,
        exchange=exchange,
    ) 