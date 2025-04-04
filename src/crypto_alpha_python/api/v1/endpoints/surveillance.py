"""
Market surveillance endpoints for handling market surveillance operations.
"""
from typing import Dict
from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.db.session import get_session
from crypto_alpha_python.services.surveillance import MarketSurveillance

router = APIRouter()

@router.get("/volume-spike/{symbol}")
async def detect_volume_spike(
    symbol: str,
    threshold: float = Query(3.0, gt=0),
    window: str = Query("1h", regex="^\d+[smhd]$"),
    exchange: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, any]:
    """
    Detect volume spikes in market data.
    """
    surveillance = MarketSurveillance(session)
    return await surveillance.detect_volume_spike(
        symbol=symbol,
        threshold=threshold,
        window=window,
        exchange=exchange,
    )

@router.get("/price-jump/{symbol}")
async def detect_price_jump(
    symbol: str,
    threshold: float = Query(0.02, gt=0),
    window: str = Query("1h", regex="^\d+[smhd]$"),
    exchange: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, any]:
    """
    Detect significant price jumps.
    """
    surveillance = MarketSurveillance(session)
    return await surveillance.detect_price_jump(
        symbol=symbol,
        threshold=threshold,
        window=window,
        exchange=exchange,
    )

@router.get("/wash-trading/{symbol}")
async def detect_wash_trading(
    symbol: str,
    window: str = Query("1h", regex="^\d+[smhd]$"),
    exchange: str | None = None,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, any]:
    """
    Detect potential wash trading patterns.
    """
    surveillance = MarketSurveillance(session)
    return await surveillance.detect_wash_trading(
        symbol=symbol,
        window=window,
        exchange=exchange,
    ) 