"""
Main API router that combines all endpoint routers.
"""
from fastapi import APIRouter

from crypto_alpha_python.api.v1.endpoints import (
    market_data,
    analysis,
    # portfolio,
    surveillance,
    websocket,
)

api_router = APIRouter()

api_router.include_router(market_data.router, prefix="/market-data", tags=["market-data"])
api_router.include_router(analysis.router, prefix="/analysis", tags=["analysis"])
# api_router.include_router(portfolio.router, prefix="/portfolio", tags=["portfolio"])
api_router.include_router(surveillance.router, prefix="/surveillance", tags=["surveillance"])
api_router.include_router(websocket.router, tags=["websocket"]) 