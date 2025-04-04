"""
Portfolio endpoints for handling portfolio operations.
"""
from typing import Dict
from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession
from uuid import UUID

from crypto_alpha_python.db.session import get_session
from crypto_alpha_python.services.portfolio import PortfolioService

router = APIRouter()

@router.get("/value/{user_id}")
async def get_portfolio_value(
    user_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, float]:
    """
    Get current portfolio value for a user.
    """
    portfolio_service = PortfolioService(session)
    value = await portfolio_service.get_portfolio_value(user_id)
    return {"value": value}

@router.get("/var/{user_id}")
async def get_portfolio_var(
    user_id: UUID,
    confidence_level: float = Query(0.95, gt=0, le=1),
    window: str = Query("1d", regex="^\d+[smhd]$"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, float]:
    """
    Calculate Value at Risk (VaR) for a user's portfolio.
    """
    portfolio_service = PortfolioService(session)
    var = await portfolio_service.calculate_var(
        user_id=user_id,
        confidence_level=confidence_level,
        window=window,
    )
    return {"var": var}

@router.get("/sharpe/{user_id}")
async def get_portfolio_sharpe(
    user_id: UUID,
    risk_free_rate: float = Query(0.02, gt=0),
    window: str = Query("1d", regex="^\d+[smhd]$"),
    session: AsyncSession = Depends(get_session),
) -> Dict[str, float]:
    """
    Calculate Sharpe ratio for a user's portfolio.
    """
    portfolio_service = PortfolioService(session)
    sharpe = await portfolio_service.calculate_sharpe_ratio(
        user_id=user_id,
        risk_free_rate=risk_free_rate,
        window=window,
    )
    return {"sharpe_ratio": sharpe} 