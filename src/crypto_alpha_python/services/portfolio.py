"""
Portfolio service for handling portfolio operations and calculations.
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import numpy as np
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from crypto_alpha_python.models.orders import Order, OrderStatus
from crypto_alpha_python.models.market_data import MarketData
from crypto_alpha_python.services.market_data import get_latest_market_data

class PortfolioService:
    """Service for handling portfolio operations and calculations."""
    
    def __init__(self, session: AsyncSession):
        """Initialize the portfolio service."""
        self.session = session
    
    async def get_portfolio_value(
        self,
        user_id: str,
        timestamp: Optional[datetime] = None,
    ) -> float:
        """
        Calculate portfolio value at a specific timestamp.
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Get all filled orders for the user
        query = select(Order).where(
            Order.user_id == user_id,
            Order.status == OrderStatus.FILLED,
            Order.filled_at <= timestamp,
        )
        result = await self.session.execute(query)
        orders = result.scalars().all()
        
        portfolio_value = 0.0
        
        # Group orders by symbol
        symbol_orders: Dict[str, List[Order]] = {}
        for order in orders:
            if order.symbol not in symbol_orders:
                symbol_orders[order.symbol] = []
            symbol_orders[order.symbol].append(order)
        
        # Calculate value for each symbol
        for symbol, symbol_orders in symbol_orders.items():
            # Get latest market data for the symbol
            market_data = await get_latest_market_data(
                self.session,
                symbol,
                timestamp=timestamp,
            )
            
            if market_data:
                # Calculate position size
                position_size = sum(
                    order.filled_quantity * (1 if order.side == "buy" else -1)
                    for order in symbol_orders
                )
                
                # Add to portfolio value
                portfolio_value += position_size * market_data.last_price
        
        return portfolio_value
    
    async def calculate_var(
        self,
        user_id: str,
        confidence_level: float = 0.95,
        window: str = "1d",
    ) -> float:
        """
        Calculate Value at Risk (VaR) for the portfolio.
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
        
        # Get portfolio values over time
        portfolio_values = []
        current_time = start_time
        while current_time <= end_time:
            value = await self.get_portfolio_value(user_id, current_time)
            portfolio_values.append(value)
            current_time += timedelta(hours=1)  # Sample every hour
        
        if not portfolio_values:
            return 0.0
        
        # Calculate returns
        returns = np.diff(np.log(portfolio_values))
        
        # Calculate VaR
        var = np.percentile(
            returns,
            (1 - confidence_level) * 100,
        )
        
        # Convert to absolute value
        current_value = portfolio_values[-1]
        var_absolute = current_value * (1 - np.exp(var))
        
        return float(abs(var_absolute))
    
    async def calculate_sharpe_ratio(
        self,
        user_id: str,
        risk_free_rate: float = 0.02,  # 2% annual risk-free rate
        window: str = "1d",
    ) -> float:
        """
        Calculate Sharpe ratio for the portfolio.
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
        
        # Get portfolio values over time
        portfolio_values = []
        current_time = start_time
        while current_time <= end_time:
            value = await self.get_portfolio_value(user_id, current_time)
            portfolio_values.append(value)
            current_time += timedelta(hours=1)  # Sample every hour
        
        if not portfolio_values:
            return 0.0
        
        # Calculate returns
        returns = np.diff(np.log(portfolio_values))
        
        # Calculate annualized metrics
        annualized_return = np.mean(returns) * 252  # Assuming daily data
        annualized_volatility = np.std(returns) * np.sqrt(252)
        
        if annualized_volatility == 0:
            return 0.0
        
        # Calculate Sharpe ratio
        sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility
        
        return float(sharpe_ratio) 