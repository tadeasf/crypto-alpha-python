"""
Order models for tracking trades and positions.
"""
from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4
from sqlmodel import Field, SQLModel

class OrderSide(str, Enum):
    """Order side enumeration."""
    BUY = "buy"
    SELL = "sell"

class OrderStatus(str, Enum):
    """Order status enumeration."""
    OPEN = "open"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    PARTIALLY_FILLED = "partially_filled"

class OrderBase(SQLModel):
    """Base model for orders."""
    order_id: UUID = Field(default_factory=uuid4, primary_key=True)
    user_id: UUID = Field(index=True)
    symbol: str = Field(index=True)
    side: OrderSide
    price: float
    quantity: float
    status: OrderStatus = OrderStatus.OPEN
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    filled_at: Optional[datetime] = None
    filled_price: Optional[float] = None
    filled_quantity: Optional[float] = None
    commission: Optional[float] = None
    commission_asset: Optional[str] = None
    notes: Optional[str] = None

class Order(OrderBase, table=True):
    """Order model for database storage."""
    __tablename__ = "orders"

class OrderCreate(OrderBase):
    """Order model for creation."""
    pass

class OrderRead(OrderBase):
    """Order model for reading."""
    pass 