"""
Market data models for storing exchange data.
"""
from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel

class MarketDataBase(SQLModel):
    """Base model for market data."""
    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: datetime = Field(index=True)
    symbol: str = Field(index=True)
    exchange: str = Field(index=True)
    bid: float
    ask: float
    last_price: float
    volume: float
    bid_size: float
    ask_size: float
    trades_count: int = 0
    vwap: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    open: Optional[float] = None
    close: Optional[float] = None

class MarketData(MarketDataBase, table=True):
    """Market data model for database storage."""
    __tablename__ = "market_data"

class MarketDataCreate(MarketDataBase):
    """Market data model for creation."""
    pass

class MarketDataRead(MarketDataBase):
    """Market data model for reading."""
    pass 